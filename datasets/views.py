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
import pandas as pd
import json
import csv

from .models import Dataset, WardRecord, LabRecord
from .forms import DatasetUploadForm, RegisterForm


# ─── Dashboard ───────────────────────────────────────────────────────────────

def home(request):
    # Cache dashboard stats for 60 s — avoids 12 full-table queries on every load
    stats = cache.get('dashboard_stats')
    if stats is None:
        # Dataset counts
        ds_counts = Dataset.objects.filter(status='ready').aggregate(
            total=Count('id'),
            ward=Count('id', filter=Q(schema_type='ward')),
            lab=Count('id', filter=Q(schema_type='lab')),
        )
        total_ward = WardRecord.objects.count()
        total_lab  = LabRecord.objects.count()

        # Ward breakdown — single query via conditional aggregation
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

        total_patients = WardRecord.objects.values('anon_id').distinct().count()
        total_cases    = LabRecord.objects.values('caseid').distinct().count()

        stats = {
            'total_datasets': ds_counts['total'] or 0,
            'ward_datasets':  ds_counts['ward']  or 0,
            'lab_datasets':   ds_counts['lab']   or 0,
            'total_ward':     total_ward,
            'total_lab':      total_lab,
            'total_patients': total_patients,
            'total_cases':    total_cases,
            'ward_stats':     {'IP': w['IP'], 'OP': w['OP'], 'ER': w['ER'],
                               'UC': w['UC'], 'DS': w['DS']},
            'lab_test_stats': lab_test_stats,
        }
        cache.set('dashboard_stats', stats, 60)  # 60-second TTL

    recent_datasets = Dataset.objects.filter(status='ready').order_by('-created_at')[:6]
    ward_stats      = stats['ward_stats']
    lab_test_stats  = stats['lab_test_stats']

    context = {
        'total_datasets':  stats['total_datasets'],
        'ward_datasets':   stats['ward_datasets'],
        'lab_datasets':    stats['lab_datasets'],
        'total_ward':      stats['total_ward'],
        'total_lab':       stats['total_lab'],
        'total_patients':  stats['total_patients'],
        'total_cases':     stats['total_cases'],
        'recent_datasets': recent_datasets,
        'ward_stats':      ward_stats,
        'ward_stats_json': json.dumps(ward_stats),
        'lab_test_stats':  lab_test_stats,
        'lab_test_stats_json': json.dumps([
            {'name': t['name'], 'count': t['count'], 'avg': round(t['avg'] or 0, 3)}
            for t in lab_test_stats
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

def dataset_list(request):
    datasets = Dataset.objects.all()
    search = request.GET.get('search', '').strip()
    schema = request.GET.get('schema', '').strip()
    if search:
        datasets = datasets.filter(
            Q(name__icontains=search) | Q(description__icontains=search)
        )
    if schema in ('ward', 'lab'):
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

                if dataset.schema_type == 'lab':
                    _ingest_lab(df, dataset)
                else:
                    _ingest_ward(df, dataset)

                cache.delete('dashboard_stats')
                messages.success(
                    request,
                    f'✅ Dataset <strong>{dataset.name}</strong> uploaded with '
                    f'<strong>{dataset.row_count:,}</strong> records!'
                )
                return redirect('dataset_detail', pk=dataset.pk)

            except Exception as e:
                dataset.status = 'error'
                dataset.save()
                messages.error(request, f'❌ Error processing file: {e}')
    else:
        form = DatasetUploadForm()

    return render(request, 'datasets/upload.html', {'form': form})


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

    def safe_int(v, d=0):
        try: return int(v)
        except: return d

    def safe_date(v):
        if pd.isna(v): return None
        try: return pd.to_datetime(str(v)).date()
        except: return None

    records = [
        WardRecord(
            dataset=dataset,
            anon_id=safe_int(row['anon_id']),
            pat_enc_csn_id_coded=safe_int(row['pat_enc_csn_id_coded']),
            order_proc_id_coded=safe_int(row['order_proc_id_coded']),
            order_time_jittered_utc_shifted=safe_date(row['order_time_jittered_utc_shifted']),
            hosp_ward_IP=safe_int(row['hosp_ward_IP']),
            hosp_ward_OP=safe_int(row['hosp_ward_OP']),
            hosp_ward_ER=safe_int(row['hosp_ward_ER']),
            hosp_ward_UC=safe_int(row['hosp_ward_UC']),
            hosp_ward_day_surg=safe_int(row['hosp_ward_day_surg']),
        )
        for _, row in df.iterrows()
    ]
    WardRecord.objects.bulk_create(records, batch_size=2000)
    dataset.row_count = len(records)
    dataset.status = 'ready'
    dataset.save()


def _ingest_lab(df, dataset):
    required = {'caseid', 'dt', 'name', 'result'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")

    def safe_float(v):
        try: return float(v)
        except: return None

    CHUNK = 5000
    total = 0
    rows = df[['caseid', 'dt', 'name', 'result']].itertuples(index=False)

    chunk = []
    for row in rows:
        chunk.append(LabRecord(
            dataset=dataset,
            caseid=int(row.caseid),
            dt=int(row.dt),
            name=str(row.name).strip(),
            result=safe_float(row.result),
        ))
        if len(chunk) >= CHUNK:
            with transaction.atomic():
                LabRecord.objects.bulk_create(chunk)
            total += len(chunk)
            chunk = []

    if chunk:
        with transaction.atomic():
            LabRecord.objects.bulk_create(chunk)
        total += len(chunk)

    dataset.row_count = total
    dataset.status = 'ready'
    dataset.save()


# ─── Detail ───────────────────────────────────────────────────────────────────

def dataset_detail(request, pk):
    dataset = get_object_or_404(Dataset, pk=pk)
    if dataset.schema_type == 'lab':
        return _lab_detail(request, dataset)
    return _ward_detail(request, dataset)


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

    # Single aggregate query instead of 5 separate COUNTs
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

    # Cache the expensive per-dataset aggregates
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


# ─── API for DataTables ────────────────────────────────────────────────────────

def api_records(request, pk):
    dataset = get_object_or_404(Dataset, pk=pk)
    if dataset.schema_type == 'lab':
        return _api_lab_records(request, dataset)
    return _api_ward_records(request, dataset)


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
