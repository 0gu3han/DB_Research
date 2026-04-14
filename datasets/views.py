from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth import login, logout, authenticate
from django.contrib import messages
from django.db.models import Count, Q, Sum, Avg, Min, Max
from django.http import JsonResponse, HttpResponse
from django.core.paginator import Paginator
from django.views.decorators.http import require_POST
from django.db import transaction
from django.core.cache import cache
from django.utils import timezone
from datetime import timedelta
import pandas as pd
import json
import csv

from .models import (
    Dataset, WardRecord, LabRecord,
    ADIRecord, ComorbidityRecord, DemographicsRecord, NursingHomeVisitRecord,
)
from .forms import DatasetUploadForm, RegisterForm


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _safe_int(v, d=0):
    try: return int(v)
    except: return d

def _safe_date(v):
    if pd.isna(v): return None
    try: return pd.to_datetime(str(v)).date()
    except: return None

def _safe_float(v):
    try: return float(v)
    except: return None


# ─── Dashboard ───────────────────────────────────────────────────────────────

def home(request):
    stats = cache.get('dashboard_stats')
    if stats is None:
        # Single aggregate on the small Dataset table — uses stored row_count,
        # so speed is O(datasets) not O(records), regardless of how large the
        # imported tables are.
        agg = Dataset.objects.filter(status='ready').aggregate(
            total=Count('id'),
            ward=Count('id', filter=Q(schema_type='ward')),
            lab=Count('id', filter=Q(schema_type='lab')),
            adi=Count('id', filter=Q(schema_type='ADI')),
            comorbidity=Count('id', filter=Q(schema_type='comorbidity')),
            demographics=Count('id', filter=Q(schema_type='demographics')),
            nursing=Count('id', filter=Q(schema_type='nursing_home_visits')),
            ward_rows=Sum('row_count', filter=Q(schema_type='ward')),
            lab_rows=Sum('row_count', filter=Q(schema_type='lab')),
            adi_rows=Sum('row_count', filter=Q(schema_type='ADI')),
            demo_rows=Sum('row_count', filter=Q(schema_type='demographics')),
            comorbidity_rows=Sum('row_count', filter=Q(schema_type='comorbidity')),
            nursing_rows=Sum('row_count', filter=Q(schema_type='nursing_home_visits')),
        )

        # Ward breakdown — one aggregation query on WardRecord
        w = WardRecord.objects.aggregate(
            IP=Count('id', filter=Q(hosp_ward_IP=1)),
            OP=Count('id', filter=Q(hosp_ward_OP=1)),
            ER=Count('id', filter=Q(hosp_ward_ER=1)),
            UC=Count('id', filter=Q(hosp_ward_UC=1)),
            DS=Count('id', filter=Q(hosp_ward_day_surg=1)),
        )

        lab_test_stats = list(
            LabRecord.objects
            .values('name')
            .annotate(count=Count('id'), avg=Avg('result'))
            .order_by('-count')[:8]
        )

        stats = {
            'total_datasets':        agg['total'] or 0,
            'ward_datasets':         agg['ward'] or 0,
            'lab_datasets':          agg['lab'] or 0,
            'adi_datasets':          agg['adi'] or 0,
            'comorbidity_datasets':  agg['comorbidity'] or 0,
            'demographics_datasets': agg['demographics'] or 0,
            'nursing_datasets':      agg['nursing'] or 0,
            'total_ward':            agg['ward_rows'] or 0,
            'total_lab':             agg['lab_rows'] or 0,
            'total_adi':             agg['adi_rows'] or 0,
            'total_demographics':    agg['demo_rows'] or 0,
            'total_comorbidity':     agg['comorbidity_rows'] or 0,
            'total_nursing':         agg['nursing_rows'] or 0,
            'ward_stats':            {'IP': w['IP'], 'OP': w['OP'], 'ER': w['ER'],
                                      'UC': w['UC'], 'DS': w['DS']},
            'lab_test_stats':        lab_test_stats,
        }
        cache.set('dashboard_stats', stats, 300)

    recent_datasets = Dataset.objects.filter(status='ready').order_by('-created_at')[:6]

    context = {
        **stats,
        'recent_datasets':     recent_datasets,
        'ward_stats_json':     json.dumps(stats['ward_stats']),
        'lab_test_stats_json': json.dumps([
            {'name': t['name'], 'count': t['count'], 'avg': round(t['avg'] or 0, 3)}
            for t in stats['lab_test_stats']
        ]),
    }
    return render(request, 'home.html', context)


def api_dashboard_stats(request):
    ward_stats = {
        'IP': WardRecord.objects.filter(hosp_ward_IP=1).count(),
        'OP': WardRecord.objects.filter(hosp_ward_OP=1).count(),
        'ER': WardRecord.objects.filter(hosp_ward_ER=1).count(),
        'UC': WardRecord.objects.filter(hosp_ward_UC=1).count(),
        'DS': WardRecord.objects.filter(hosp_ward_day_surg=1).count(),
    }
    return JsonResponse(ward_stats)


# ─── Dataset List ─────────────────────────────────────────────────────────────

VALID_SCHEMAS = {'ward', 'lab', 'ADI', 'comorbidity', 'demographics', 'nursing_home_visits'}

def dataset_list(request):
    # Auto-expire datasets stuck in 'processing' for more than 10 minutes
    # (caused by server restarts or crashes during ingestion)
    Dataset.objects.filter(
        status='processing',
        updated_at__lt=timezone.now() - timedelta(minutes=10)
    ).update(status='error')

    datasets = Dataset.objects.all()
    search = request.GET.get('search', '').strip()
    schema = request.GET.get('schema', '').strip()
    if search:
        datasets = datasets.filter(
            Q(name__icontains=search) | Q(description__icontains=search)
        )
    if schema in VALID_SCHEMAS:
        datasets = datasets.filter(schema_type=schema)

    paginator = Paginator(datasets, 12)
    page = request.GET.get('page', 1)
    datasets_page = paginator.get_page(page)

    return render(request, 'datasets/list.html', {
        'datasets': datasets_page,
        'search': search,
        'schema': schema,
        'total_count': Dataset.objects.count(),
    })


# ─── Upload ───────────────────────────────────────────────────────────────────

def dataset_upload(request):
    if request.method == 'POST':
        form = DatasetUploadForm(request.POST, request.FILES)
        if form.is_valid():
            dataset = form.save(commit=False)
            if request.user.is_authenticated:
                dataset.created_by = request.user
            dataset.status = 'processing'
            dataset.save()

            try:
                uploaded_file = request.FILES['file']
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file)
                df.columns = [c.strip() for c in df.columns]

                dispatch = {
                    'lab':                 _ingest_lab,
                    'ADI':                 _ingest_adi,
                    'comorbidity':         _ingest_comorbidity,
                    'demographics':        _ingest_demographics,
                    'nursing_home_visits': _ingest_nursing_home_visits,
                }
                ingest_fn = dispatch.get(dataset.schema_type, _ingest_ward)

                # Wrap in a single atomic block so that:
                # 1. Partial inserts are rolled back on error
                # 2. The connection is cleanly exited before the except runs,
                #    ensuring dataset.save(status='error') always succeeds.
                with transaction.atomic():
                    ingest_fn(df, dataset)

                cache.delete('dashboard_stats')
                messages.success(
                    request,
                    f'✅ Dataset <strong>{dataset.name}</strong> uploaded with '
                    f'<strong>{dataset.row_count:,}</strong> records!'
                )
                return redirect('dataset_detail', pk=dataset.pk)

            except Exception as e:
                # The atomic block above has already been rolled back cleanly.
                # This save is now guaranteed to execute outside any transaction.
                dataset.status = 'error'
                dataset.save(update_fields=['status'])
                messages.error(request, f'❌ Error processing file: {e}')
    else:
        form = DatasetUploadForm()

    return render(request, 'datasets/upload.html', {'form': form})


INGEST_CHUNK = 2000


def _ingest_ward(df, dataset):
    required = {
        'anon_id', 'pat_enc_csn_id_coded', 'order_proc_id_coded',
        'order_time_jittered_utc_shifted',
        'hosp_ward_IP', 'hosp_ward_OP', 'hosp_ward_ER',
        'hosp_ward_UC', 'hosp_ward_day_surg',
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")

    total = 0
    for i in range(0, len(df), INGEST_CHUNK):
        batch = df.iloc[i:i + INGEST_CHUNK]
        WardRecord.objects.bulk_create([
            WardRecord(
                dataset=dataset,
                anon_id=_safe_int(row['anon_id']),
                pat_enc_csn_id_coded=_safe_int(row['pat_enc_csn_id_coded']),
                order_proc_id_coded=_safe_int(row['order_proc_id_coded']),
                order_time_jittered_utc_shifted=_safe_date(row['order_time_jittered_utc_shifted']),
                hosp_ward_IP=_safe_int(row['hosp_ward_IP']),
                hosp_ward_OP=_safe_int(row['hosp_ward_OP']),
                hosp_ward_ER=_safe_int(row['hosp_ward_ER']),
                hosp_ward_UC=_safe_int(row['hosp_ward_UC']),
                hosp_ward_day_surg=_safe_int(row['hosp_ward_day_surg']),
            )
            for _, row in batch.iterrows()
        ])
        total += len(batch)
    dataset.row_count = total
    dataset.status = 'ready'
    dataset.save()


def _ingest_lab(df, dataset):
    required = {'caseid', 'dt', 'name', 'result'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")

    total = 0
    for i in range(0, len(df), INGEST_CHUNK):
        batch = df[['caseid', 'dt', 'name', 'result']].iloc[i:i + INGEST_CHUNK]
        LabRecord.objects.bulk_create([
            LabRecord(
                dataset=dataset,
                caseid=int(row.caseid),
                dt=int(row.dt),
                name=str(row.name).strip(),
                result=_safe_float(row.result),
            )
            for row in batch.itertuples(index=False)
        ])
        total += len(batch)

    dataset.row_count = total
    dataset.status = 'ready'
    dataset.save()


def _ingest_adi(df, dataset):
    required = {'anon_id', 'pat_enc_csn_id_coded', 'order_proc_id_coded', 'adi_score'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")

    total = 0
    for i in range(0, len(df), INGEST_CHUNK):
        batch = df.iloc[i:i + INGEST_CHUNK]
        ADIRecord.objects.bulk_create([
            ADIRecord(
                dataset=dataset,
                anon_id=_safe_int(row['anon_id']),
                pat_enc_csn_id_coded=_safe_int(row['pat_enc_csn_id_coded']),
                order_proc_id_coded=_safe_int(row['order_proc_id_coded']),
                adi_score=_safe_float(row['adi_score']),
                adi_state_rank=_safe_int(row.get('adi_state_rank', 0), None),
                order_time_jittered_utc_shifted=_safe_date(row.get('order_time_jittered_utc_shifted', float('nan'))),
            )
            for _, row in batch.iterrows()
        ])
        total += len(batch)
    dataset.row_count = total
    dataset.status = 'ready'
    dataset.save()


def _ingest_comorbidity(df, dataset):
    required = {'anon_id', 'pat_enc_csn_id_coded', 'order_proc_id_coded', 'ICD10'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")

    total = 0
    for i in range(0, len(df), INGEST_CHUNK):
        batch = df.iloc[i:i + INGEST_CHUNK]
        ComorbidityRecord.objects.bulk_create([
            ComorbidityRecord(
                dataset=dataset,
                anon_id=_safe_int(row['anon_id']),
                pat_enc_csn_id_coded=_safe_int(row['pat_enc_csn_id_coded']),
                order_proc_id_coded=_safe_int(row['order_proc_id_coded']),
                ICD10=str(row['ICD10']).strip(),
                category=str(row.get('category', '') or '').strip(),
                order_time_jittered_utc_shifted=_safe_date(row.get('order_time_jittered_utc_shifted', float('nan'))),
            )
            for _, row in batch.iterrows()
        ])
        total += len(batch)
    dataset.row_count = total
    dataset.status = 'ready'
    dataset.save()


def _ingest_demographics(df, dataset):
    required = {'anon_id', 'pat_enc_csn_id_coded', 'order_proc_id_coded'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")

    total = 0
    for i in range(0, len(df), INGEST_CHUNK):
        batch = df.iloc[i:i + INGEST_CHUNK]
        DemographicsRecord.objects.bulk_create([
            DemographicsRecord(
                dataset=dataset,
                anon_id=_safe_int(row['anon_id']),
                pat_enc_csn_id_coded=_safe_int(row['pat_enc_csn_id_coded']),
                order_proc_id_coded=_safe_int(row['order_proc_id_coded']),
                age=_safe_int(row.get('age', 0), None),
                gender=str(row.get('gender', '') or '').strip(),
            )
            for _, row in batch.iterrows()
        ])
        total += len(batch)
    dataset.row_count = total
    dataset.status = 'ready'
    dataset.save()


def _ingest_nursing_home_visits(df, dataset):
    required = {'anon_id', 'pat_enc_csn_id_coded', 'order_proc_id_coded'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")

    total = 0
    for i in range(0, len(df), INGEST_CHUNK):
        batch = df.iloc[i:i + INGEST_CHUNK]
        NursingHomeVisitRecord.objects.bulk_create([
            NursingHomeVisitRecord(
                dataset=dataset,
                anon_id=_safe_int(row['anon_id']),
                pat_enc_csn_id_coded=_safe_int(row['pat_enc_csn_id_coded']),
                order_proc_id_coded=_safe_int(row['order_proc_id_coded']),
                nursing_home_visit_culture=_safe_int(row.get('nursing_home_visit_culture', 0), None),
                order_time_jittered_utc_shifted=_safe_date(row.get('order_time_jittered_utc_shifted', float('nan'))),
                visit_date_shifted=_safe_date(row.get('visit_date_shifted', float('nan'))),
            )
            for _, row in batch.iterrows()
        ])
        total += len(batch)
    dataset.row_count = total
    dataset.status = 'ready'
    dataset.save()


# ─── Detail ───────────────────────────────────────────────────────────────────

def dataset_detail(request, pk):
    dataset = get_object_or_404(Dataset, pk=pk)
    dispatch = {
        'lab':                 _lab_detail,
        'ADI':                 _adi_detail,
        'comorbidity':         _comorbidity_detail,
        'demographics':        _demographics_detail,
        'nursing_home_visits': _nursing_home_visits_detail,
    }
    fn = dispatch.get(dataset.schema_type, _ward_detail)
    return fn(request, dataset)


def _ward_detail(request, dataset):
    anon_id_filter = request.GET.get('anon_id', '').strip()
    ward_type      = request.GET.get('ward_type', '').strip()
    date_from      = request.GET.get('date_from', '').strip()
    date_to        = request.GET.get('date_to', '').strip()

    records = WardRecord.objects.filter(dataset=dataset)
    if anon_id_filter:
        records = records.filter(anon_id=anon_id_filter)
    ward_filter_map = {
        'IP': {'hosp_ward_IP': 1}, 'OP': {'hosp_ward_OP': 1},
        'ER': {'hosp_ward_ER': 1}, 'UC': {'hosp_ward_UC': 1},
        'DS': {'hosp_ward_day_surg': 1},
    }
    if ward_type in ward_filter_map:
        records = records.filter(**ward_filter_map[ward_type])
    if date_from:
        records = records.filter(order_time_jittered_utc_shifted__gte=date_from)
    if date_to:
        records = records.filter(order_time_jittered_utc_shifted__lte=date_to)

    cache_key = f'ward_stats_{dataset.pk}'
    ward_stats = cache.get(cache_key)
    if ward_stats is None:
        w = dataset.ward_records.aggregate(
            IP=Count('id', filter=Q(hosp_ward_IP=1)),
            OP=Count('id', filter=Q(hosp_ward_OP=1)),
            ER=Count('id', filter=Q(hosp_ward_ER=1)),
            UC=Count('id', filter=Q(hosp_ward_UC=1)),
            DS=Count('id', filter=Q(hosp_ward_day_surg=1)),
        )
        ward_stats = {'IP': w['IP'], 'OP': w['OP'], 'ER': w['ER'],
                      'UC': w['UC'], 'DS': w['DS']}
        cache.set(cache_key, ward_stats, 60)

    return render(request, 'datasets/detail.html', {
        'dataset':         dataset,
        'ward_stats':      ward_stats,
        'ward_stats_json': json.dumps(ward_stats),
        'filters': {
            'anon_id': anon_id_filter, 'ward_type': ward_type,
            'date_from': date_from, 'date_to': date_to,
        },
        'filtered_count': records.count(),
    })


def _lab_detail(request, dataset):
    caseid_filter  = request.GET.get('caseid', '').strip()
    test_filter    = request.GET.get('test', '').strip()
    dt_from        = request.GET.get('dt_from', '').strip()
    dt_to          = request.GET.get('dt_to', '').strip()

    cache_key = f'lab_detail_stats_{dataset.pk}'
    lab_cache = cache.get(cache_key)
    if lab_cache is None:
        available_tests = list(
            dataset.lab_records.values_list('name', flat=True).distinct().order_by('name')
        )
        test_stats = list(
            dataset.lab_records
            .values('name')
            .annotate(count=Count('id'), avg=Avg('result'), lo=Min('result'), hi=Max('result'))
            .order_by('name')
        )
        unique_cases = dataset.lab_records.values('caseid').distinct().count()
        lab_cache = {
            'available_tests': available_tests,
            'test_stats':      test_stats,
            'unique_cases':    unique_cases,
        }
        cache.set(cache_key, lab_cache, 120)

    return render(request, 'datasets/detail_lab.html', {
        'dataset':          dataset,
        'available_tests':  lab_cache['available_tests'],
        'test_stats':       lab_cache['test_stats'],
        'test_stats_json':  json.dumps([
            {'name': t['name'], 'count': t['count'], 'avg': round(t['avg'] or 0, 3)}
            for t in lab_cache['test_stats']
        ]),
        'unique_cases':     lab_cache['unique_cases'],
        'filters': {
            'caseid':  caseid_filter,
            'test':    test_filter,
            'dt_from': dt_from,
            'dt_to':   dt_to,
        },
    })


def _adi_detail(request, dataset):
    cache_key = f'adi_detail_stats_{dataset.pk}'
    cached = cache.get(cache_key)
    if cached is None:
        unique_patients = dataset.adi_records.values('anon_id').distinct().count()
        avg_adi = dataset.adi_records.aggregate(avg=Avg('adi_score'))['avg'] or 0

        # Distribution in 10-point buckets
        from collections import Counter
        scores = list(dataset.adi_records.values_list('adi_score', flat=True))
        buckets = Counter()
        for s in scores:
            if s is not None:
                b = int(s // 10) * 10
                buckets[b] += 1
        adi_dist = [{'bucket': k, 'count': v} for k, v in sorted(buckets.items())]

        cached = {
            'unique_patients': unique_patients,
            'avg_adi': round(avg_adi, 1),
            'adi_dist': adi_dist,
        }
        cache.set(cache_key, cached, 120)

    return render(request, 'datasets/detail_adi.html', {
        'dataset':         dataset,
        'total_records':   dataset.row_count,
        'unique_patients': cached['unique_patients'],
        'avg_adi':         cached['avg_adi'],
        'adi_dist_json':   json.dumps(cached['adi_dist']),
        'filters': {
            'anon_id':    request.GET.get('anon_id', ''),
            'score_min':  request.GET.get('score_min', ''),
            'score_max':  request.GET.get('score_max', ''),
            'date_from':  request.GET.get('date_from', ''),
            'date_to':    request.GET.get('date_to', ''),
        },
    })


def _comorbidity_detail(request, dataset):
    cache_key = f'comorbidity_detail_stats_{dataset.pk}'
    cached = cache.get(cache_key)
    if cached is None:
        unique_patients = dataset.comorbidity_records.values('anon_id').distinct().count()
        unique_icd10 = dataset.comorbidity_records.values('ICD10').distinct().count()
        top_cats = list(
            dataset.comorbidity_records.values('category')
            .annotate(count=Count('id')).order_by('-count')[:10]
        )
        available_categories = list(
            dataset.comorbidity_records.values_list('category', flat=True)
            .distinct().order_by('category')
        )
        cached = {
            'unique_patients': unique_patients,
            'unique_icd10': unique_icd10,
            'top_categories': top_cats,
            'available_categories': available_categories,
        }
        cache.set(cache_key, cached, 120)

    return render(request, 'datasets/detail_comorbidity.html', {
        'dataset':               dataset,
        'total_records':         dataset.row_count,
        'unique_patients':       cached['unique_patients'],
        'unique_icd10':          cached['unique_icd10'],
        'top_categories_json':   json.dumps(cached['top_categories']),
        'available_categories':  cached['available_categories'],
        'filters': {
            'anon_id':   request.GET.get('anon_id', ''),
            'icd10':     request.GET.get('icd10', ''),
            'category':  request.GET.get('category', ''),
            'date_from': request.GET.get('date_from', ''),
            'date_to':   request.GET.get('date_to', ''),
        },
    })


def _demographics_detail(request, dataset):
    cache_key = f'demographics_detail_stats_{dataset.pk}'
    cached = cache.get(cache_key)
    if cached is None:
        unique_patients = dataset.demographics_records.values('anon_id').distinct().count()
        gender_counts = list(
            dataset.demographics_records
            .exclude(gender__in=['', 'Unknown', 'unknown', 'X', 'x'])
            .values('gender')
            .annotate(count=Count('id')).order_by('gender')
        )
        available_genders = list(
            dataset.demographics_records
            .exclude(gender__in=['', 'Unknown', 'unknown', 'X', 'x'])
            .values_list('gender', flat=True)
            .distinct().order_by('gender')
        )

        from collections import Counter
        ages = list(dataset.demographics_records.values_list('age', flat=True))
        age_buckets = Counter()
        for a in ages:
            if a is not None:
                b = int(a // 10) * 10
                age_buckets[b] += 1
        age_dist = [{'bucket': f'{k}–{k+9}', 'count': v} for k, v in sorted(age_buckets.items())]

        cached = {
            'unique_patients': unique_patients,
            'gender_counts': gender_counts,
            'available_genders': available_genders,
            'age_dist': age_dist,
        }
        cache.set(cache_key, cached, 120)

    return render(request, 'datasets/detail_demographics.html', {
        'dataset':           dataset,
        'total_records':     dataset.row_count,
        'unique_patients':   cached['unique_patients'],
        'gender_counts_json': json.dumps(cached['gender_counts']),
        'age_dist_json':     json.dumps(cached['age_dist']),
        'available_genders': cached['available_genders'],
        'filters': {
            'anon_id': request.GET.get('anon_id', ''),
            'gender':  request.GET.get('gender', ''),
            'age':     request.GET.get('age', ''),
        },
    })


def _nursing_home_visits_detail(request, dataset):
    cache_key = f'nhv_detail_stats_{dataset.pk}'
    cached = cache.get(cache_key)
    if cached is None:
        unique_patients = dataset.nursing_home_visit_records.values('anon_id').distinct().count()
        valid_cultures = dataset.nursing_home_visit_records.filter(
            nursing_home_visit_culture__gte=0
        ).count()

        from collections import Counter
        cultures = list(dataset.nursing_home_visit_records.values_list('nursing_home_visit_culture', flat=True))
        culture_dist = [
            {'value': k, 'count': v}
            for k, v in sorted(Counter(c for c in cultures if c is not None).items())
        ]

        cached = {
            'unique_patients': unique_patients,
            'valid_cultures': valid_cultures,
            'culture_dist': culture_dist,
        }
        cache.set(cache_key, cached, 120)

    return render(request, 'datasets/detail_nursing_home_visits.html', {
        'dataset':          dataset,
        'total_records':    dataset.row_count,
        'unique_patients':  cached['unique_patients'],
        'valid_cultures':   cached['valid_cultures'],
        'culture_dist_json': json.dumps(cached['culture_dist']),
        'filters': {
            'anon_id':      request.GET.get('anon_id', ''),
            'culture_min':  request.GET.get('culture_min', ''),
            'culture_max':  request.GET.get('culture_max', ''),
            'date_from':    request.GET.get('date_from', ''),
            'date_to':      request.GET.get('date_to', ''),
        },
    })


# ─── API for DataTables ────────────────────────────────────────────────────────

def api_records(request, pk):
    dataset = get_object_or_404(Dataset, pk=pk)
    dispatch = {
        'lab':                 _api_lab_records,
        'ADI':                 _api_adi_records,
        'comorbidity':         _api_comorbidity_records,
        'demographics':        _api_demographics_records,
        'nursing_home_visits': _api_nursing_home_visits_records,
    }
    fn = dispatch.get(dataset.schema_type, _api_ward_records)
    return fn(request, dataset)


def _api_ward_records(request, dataset):
    draw   = int(request.GET.get('draw', 1))
    start  = int(request.GET.get('start', 0))
    length = int(request.GET.get('length', 25))
    search_value = request.GET.get('search[value]', '').strip()

    col_map = {
        '0': 'anon_id', '1': 'pat_enc_csn_id_coded', '2': 'order_proc_id_coded',
        '3': 'order_time_jittered_utc_shifted',
        '4': 'hosp_ward_IP', '5': 'hosp_ward_OP', '6': 'hosp_ward_ER',
        '7': 'hosp_ward_UC', '8': 'hosp_ward_day_surg',
    }
    order_col   = request.GET.get('order[0][column]', '0')
    order_dir   = request.GET.get('order[0][dir]', 'asc')
    order_field = col_map.get(order_col, 'anon_id')
    if order_dir == 'desc':
        order_field = f'-{order_field}'

    anon_id_filter = request.GET.get('anon_id', '').strip()
    ward_type      = request.GET.get('ward_type', '').strip()
    date_from      = request.GET.get('date_from', '').strip()
    date_to        = request.GET.get('date_to', '').strip()

    qs = WardRecord.objects.filter(dataset=dataset)
    if anon_id_filter:
        qs = qs.filter(anon_id=anon_id_filter)
    ward_filter_map = {
        'IP': {'hosp_ward_IP': 1}, 'OP': {'hosp_ward_OP': 1},
        'ER': {'hosp_ward_ER': 1}, 'UC': {'hosp_ward_UC': 1},
        'DS': {'hosp_ward_day_surg': 1},
    }
    if ward_type in ward_filter_map:
        qs = qs.filter(**ward_filter_map[ward_type])
    if date_from:
        qs = qs.filter(order_time_jittered_utc_shifted__gte=date_from)
    if date_to:
        qs = qs.filter(order_time_jittered_utc_shifted__lte=date_to)

    total = qs.count()
    if search_value:
        try:
            sv = int(search_value)
            qs = qs.filter(
                Q(anon_id=sv) | Q(pat_enc_csn_id_coded=sv) | Q(order_proc_id_coded=sv)
            )
        except ValueError:
            qs = qs.filter(order_time_jittered_utc_shifted__icontains=search_value)

    filtered_total = qs.count()
    qs = qs.order_by(order_field)[start:start + length]

    data = [{
        'anon_id': r.anon_id,
        'pat_enc_csn_id_coded': r.pat_enc_csn_id_coded,
        'order_proc_id_coded': r.order_proc_id_coded,
        'order_time': str(r.order_time_jittered_utc_shifted) if r.order_time_jittered_utc_shifted else '',
        'hosp_ward_IP': r.hosp_ward_IP, 'hosp_ward_OP': r.hosp_ward_OP,
        'hosp_ward_ER': r.hosp_ward_ER, 'hosp_ward_UC': r.hosp_ward_UC,
        'hosp_ward_day_surg': r.hosp_ward_day_surg,
    } for r in qs]

    return JsonResponse({'draw': draw, 'recordsTotal': total,
                         'recordsFiltered': filtered_total, 'data': data})


def _api_lab_records(request, dataset):
    draw   = int(request.GET.get('draw', 1))
    start  = int(request.GET.get('start', 0))
    length = int(request.GET.get('length', 25))
    search_value = request.GET.get('search[value]', '').strip()

    col_map = {'0': 'caseid', '1': 'dt', '2': 'name', '3': 'result'}
    order_col   = request.GET.get('order[0][column]', '0')
    order_dir   = request.GET.get('order[0][dir]', 'asc')
    order_field = col_map.get(order_col, 'caseid')
    if order_dir == 'desc':
        order_field = f'-{order_field}'

    caseid_filter = request.GET.get('caseid', '').strip()
    test_filter   = request.GET.get('test', '').strip()
    dt_from       = request.GET.get('dt_from', '').strip()
    dt_to         = request.GET.get('dt_to', '').strip()

    qs = LabRecord.objects.filter(dataset=dataset)
    if caseid_filter:
        try: qs = qs.filter(caseid=int(caseid_filter))
        except ValueError: pass
    if test_filter:
        qs = qs.filter(name=test_filter)
    if dt_from:
        try: qs = qs.filter(dt__gte=int(dt_from))
        except ValueError: pass
    if dt_to:
        try: qs = qs.filter(dt__lte=int(dt_to))
        except ValueError: pass

    total = qs.count()
    if search_value:
        try:
            sv = int(search_value)
            qs = qs.filter(Q(caseid=sv) | Q(dt=sv))
        except ValueError:
            qs = qs.filter(name__icontains=search_value)

    filtered_total = qs.count()
    qs = qs.order_by(order_field)[start:start + length]

    data = [{'caseid': r.caseid, 'dt': r.dt, 'name': r.name,
              'result': r.result} for r in qs]

    return JsonResponse({'draw': draw, 'recordsTotal': total,
                         'recordsFiltered': filtered_total, 'data': data})


def _api_adi_records(request, dataset):
    draw   = int(request.GET.get('draw', 1))
    start  = int(request.GET.get('start', 0))
    length = int(request.GET.get('length', 25))

    col_map = {'0': 'anon_id', '1': 'pat_enc_csn_id_coded', '2': 'order_proc_id_coded',
               '3': 'adi_score', '4': 'adi_state_rank', '5': 'order_time_jittered_utc_shifted'}
    order_col   = request.GET.get('order[0][column]', '0')
    order_dir   = request.GET.get('order[0][dir]', 'asc')
    order_field = col_map.get(order_col, 'anon_id')
    if order_dir == 'desc':
        order_field = f'-{order_field}'

    qs = ADIRecord.objects.filter(dataset=dataset)
    anon_id = request.GET.get('anon_id', '').strip()
    score_min = request.GET.get('score_min', '').strip()
    score_max = request.GET.get('score_max', '').strip()
    date_from = request.GET.get('date_from', '').strip()
    date_to   = request.GET.get('date_to', '').strip()
    if anon_id:
        try: qs = qs.filter(anon_id=int(anon_id))
        except ValueError: pass
    if score_min:
        try: qs = qs.filter(adi_score__gte=float(score_min))
        except ValueError: pass
    if score_max:
        try: qs = qs.filter(adi_score__lte=float(score_max))
        except ValueError: pass
    if date_from:
        qs = qs.filter(order_time_jittered_utc_shifted__gte=date_from)
    if date_to:
        qs = qs.filter(order_time_jittered_utc_shifted__lte=date_to)

    total = filtered_total = qs.count()
    qs = qs.order_by(order_field)[start:start + length]

    data = [{
        'anon_id': r.anon_id,
        'pat_enc_csn_id_coded': r.pat_enc_csn_id_coded,
        'order_proc_id_coded': r.order_proc_id_coded,
        'adi_score': r.adi_score,
        'adi_state_rank': r.adi_state_rank,
        'order_time': str(r.order_time_jittered_utc_shifted) if r.order_time_jittered_utc_shifted else '',
    } for r in qs]

    return JsonResponse({'draw': draw, 'recordsTotal': total,
                         'recordsFiltered': filtered_total, 'data': data})


def _api_comorbidity_records(request, dataset):
    draw   = int(request.GET.get('draw', 1))
    start  = int(request.GET.get('start', 0))
    length = int(request.GET.get('length', 25))

    col_map = {'0': 'anon_id', '1': 'pat_enc_csn_id_coded', '2': 'order_proc_id_coded',
               '3': 'ICD10', '4': 'category', '5': 'order_time_jittered_utc_shifted'}
    order_col   = request.GET.get('order[0][column]', '0')
    order_dir   = request.GET.get('order[0][dir]', 'asc')
    order_field = col_map.get(order_col, 'anon_id')
    if order_dir == 'desc':
        order_field = f'-{order_field}'

    qs = ComorbidityRecord.objects.filter(dataset=dataset)
    anon_id  = request.GET.get('anon_id', '').strip()
    icd10    = request.GET.get('icd10', '').strip()
    category = request.GET.get('category', '').strip()
    date_from = request.GET.get('date_from', '').strip()
    date_to   = request.GET.get('date_to', '').strip()
    if anon_id:
        try: qs = qs.filter(anon_id=int(anon_id))
        except ValueError: pass
    if icd10:
        qs = qs.filter(ICD10__icontains=icd10)
    if category:
        qs = qs.filter(category=category)
    if date_from:
        qs = qs.filter(order_time_jittered_utc_shifted__gte=date_from)
    if date_to:
        qs = qs.filter(order_time_jittered_utc_shifted__lte=date_to)

    total = filtered_total = qs.count()
    qs = qs.order_by(order_field)[start:start + length]

    data = [{
        'anon_id': r.anon_id,
        'pat_enc_csn_id_coded': r.pat_enc_csn_id_coded,
        'order_proc_id_coded': r.order_proc_id_coded,
        'ICD10': r.ICD10,
        'category': r.category,
        'order_time': str(r.order_time_jittered_utc_shifted) if r.order_time_jittered_utc_shifted else '',
    } for r in qs]

    return JsonResponse({'draw': draw, 'recordsTotal': total,
                         'recordsFiltered': filtered_total, 'data': data})


def _api_demographics_records(request, dataset):
    draw   = int(request.GET.get('draw', 1))
    start  = int(request.GET.get('start', 0))
    length = int(request.GET.get('length', 25))

    col_map = {'0': 'anon_id', '1': 'pat_enc_csn_id_coded', '2': 'order_proc_id_coded',
               '3': 'age', '4': 'gender'}
    order_col   = request.GET.get('order[0][column]', '0')
    order_dir   = request.GET.get('order[0][dir]', 'asc')
    order_field = col_map.get(order_col, 'anon_id')
    if order_dir == 'desc':
        order_field = f'-{order_field}'

    qs = DemographicsRecord.objects.filter(dataset=dataset)\
        .exclude(gender__in=['', 'Unknown', 'unknown', 'X', 'x'])
    anon_id = request.GET.get('anon_id', '').strip()
    gender  = request.GET.get('gender', '').strip()
    age     = request.GET.get('age', '').strip()
    if anon_id:
        try: qs = qs.filter(anon_id=int(anon_id))
        except ValueError: pass
    if gender:
        qs = qs.filter(gender=gender)
    if age:
        try: qs = qs.filter(age=int(age))
        except ValueError: pass

    total = filtered_total = qs.count()
    qs = qs.order_by(order_field)[start:start + length]

    data = [{
        'anon_id': r.anon_id,
        'pat_enc_csn_id_coded': r.pat_enc_csn_id_coded,
        'order_proc_id_coded': r.order_proc_id_coded,
        'age': r.age,
        'gender': r.gender,
    } for r in qs]

    return JsonResponse({'draw': draw, 'recordsTotal': total,
                         'recordsFiltered': filtered_total, 'data': data})


def _api_nursing_home_visits_records(request, dataset):
    draw   = int(request.GET.get('draw', 1))
    start  = int(request.GET.get('start', 0))
    length = int(request.GET.get('length', 25))

    col_map = {'0': 'anon_id', '1': 'pat_enc_csn_id_coded', '2': 'order_proc_id_coded',
               '3': 'nursing_home_visit_culture', '4': 'order_time_jittered_utc_shifted',
               '5': 'visit_date_shifted'}
    order_col   = request.GET.get('order[0][column]', '0')
    order_dir   = request.GET.get('order[0][dir]', 'asc')
    order_field = col_map.get(order_col, 'anon_id')
    if order_dir == 'desc':
        order_field = f'-{order_field}'

    qs = NursingHomeVisitRecord.objects.filter(dataset=dataset)
    anon_id     = request.GET.get('anon_id', '').strip()
    cult_min    = request.GET.get('culture_min', '').strip()
    cult_max    = request.GET.get('culture_max', '').strip()
    date_from   = request.GET.get('date_from', '').strip()
    date_to     = request.GET.get('date_to', '').strip()
    if anon_id:
        try: qs = qs.filter(anon_id=int(anon_id))
        except ValueError: pass
    if cult_min:
        try: qs = qs.filter(nursing_home_visit_culture__gte=int(cult_min))
        except ValueError: pass
    if cult_max:
        try: qs = qs.filter(nursing_home_visit_culture__lte=int(cult_max))
        except ValueError: pass
    if date_from:
        qs = qs.filter(order_time_jittered_utc_shifted__gte=date_from)
    if date_to:
        qs = qs.filter(order_time_jittered_utc_shifted__lte=date_to)

    total = filtered_total = qs.count()
    qs = qs.order_by(order_field)[start:start + length]

    data = [{
        'anon_id': r.anon_id,
        'pat_enc_csn_id_coded': r.pat_enc_csn_id_coded,
        'order_proc_id_coded': r.order_proc_id_coded,
        'nursing_home_visit_culture': r.nursing_home_visit_culture,
        'order_time': str(r.order_time_jittered_utc_shifted) if r.order_time_jittered_utc_shifted else '',
        'visit_date': str(r.visit_date_shifted) if r.visit_date_shifted else '',
    } for r in qs]

    return JsonResponse({'draw': draw, 'recordsTotal': total,
                         'recordsFiltered': filtered_total, 'data': data})


# ─── Export ───────────────────────────────────────────────────────────────────

def dataset_export(request, pk):
    dataset = get_object_or_404(Dataset, pk=pk)
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="{dataset.name}.csv"'
    writer = csv.writer(response)

    if dataset.schema_type == 'lab':
        writer.writerow(['caseid', 'dt', 'name', 'result'])
        for r in dataset.lab_records.all():
            writer.writerow([r.caseid, r.dt, r.name, r.result])
    elif dataset.schema_type == 'ADI':
        writer.writerow(['anon_id', 'pat_enc_csn_id_coded', 'order_proc_id_coded',
                         'adi_score', 'adi_state_rank', 'order_time_jittered_utc_shifted'])
        for r in dataset.adi_records.all():
            writer.writerow([r.anon_id, r.pat_enc_csn_id_coded, r.order_proc_id_coded,
                             r.adi_score, r.adi_state_rank, r.order_time_jittered_utc_shifted])
    elif dataset.schema_type == 'comorbidity':
        writer.writerow(['anon_id', 'pat_enc_csn_id_coded', 'order_proc_id_coded',
                         'ICD10', 'category', 'order_time_jittered_utc_shifted'])
        for r in dataset.comorbidity_records.all():
            writer.writerow([r.anon_id, r.pat_enc_csn_id_coded, r.order_proc_id_coded,
                             r.ICD10, r.category, r.order_time_jittered_utc_shifted])
    elif dataset.schema_type == 'demographics':
        writer.writerow(['anon_id', 'pat_enc_csn_id_coded', 'order_proc_id_coded', 'age', 'gender'])
        for r in dataset.demographics_records.all():
            writer.writerow([r.anon_id, r.pat_enc_csn_id_coded, r.order_proc_id_coded,
                             r.age, r.gender])
    elif dataset.schema_type == 'nursing_home_visits':
        writer.writerow(['anon_id', 'pat_enc_csn_id_coded', 'order_proc_id_coded',
                         'nursing_home_visit_culture', 'order_time_jittered_utc_shifted',
                         'visit_date_shifted'])
        for r in dataset.nursing_home_visit_records.all():
            writer.writerow([r.anon_id, r.pat_enc_csn_id_coded, r.order_proc_id_coded,
                             r.nursing_home_visit_culture, r.order_time_jittered_utc_shifted,
                             r.visit_date_shifted])
    else:
        writer.writerow([
            'anon_id', 'pat_enc_csn_id_coded', 'order_proc_id_coded',
            'order_time_jittered_utc_shifted',
            'hosp_ward_IP', 'hosp_ward_OP', 'hosp_ward_ER',
            'hosp_ward_UC', 'hosp_ward_day_surg',
        ])
        for r in dataset.ward_records.all():
            writer.writerow([
                r.anon_id, r.pat_enc_csn_id_coded, r.order_proc_id_coded,
                r.order_time_jittered_utc_shifted,
                r.hosp_ward_IP, r.hosp_ward_OP, r.hosp_ward_ER,
                r.hosp_ward_UC, r.hosp_ward_day_surg,
            ])
    return response


# ─── Delete ───────────────────────────────────────────────────────────────────

@require_POST
def dataset_delete(request, pk):
    dataset = get_object_or_404(Dataset, pk=pk)
    name = dataset.name
    cache.delete('dashboard_stats')
    cache.delete(f'ward_stats_{pk}')
    cache.delete(f'lab_detail_stats_{pk}')
    cache.delete(f'adi_detail_stats_{pk}')
    cache.delete(f'comorbidity_detail_stats_{pk}')
    cache.delete(f'demographics_detail_stats_{pk}')
    cache.delete(f'nhv_detail_stats_{pk}')
    dataset.delete()
    messages.success(request, f'Dataset "{name}" has been deleted.')
    return redirect('dataset_list')


# ─── Auth ─────────────────────────────────────────────────────────────────────

def login_view(request):
    if request.user.is_authenticated:
        return redirect('home')
    if request.method == 'POST':
        username = request.POST.get('username', '').strip()
        password = request.POST.get('password', '')
        user = authenticate(request, username=username, password=password)
        if user:
            login(request, user)
            return redirect(request.GET.get('next', 'home'))
        messages.error(request, 'Invalid username or password.')
    return render(request, 'registration/login.html')


def logout_view(request):
    logout(request)
    return redirect('login')


def register_view(request):
    if request.user.is_authenticated:
        return redirect('home')
    if request.method == 'POST':
        form = RegisterForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            messages.success(request, 'Account created! Welcome to ResearchDB.')
            return redirect('home')
    else:
        form = RegisterForm()
    return render(request, 'registration/register.html', {'form': form})
