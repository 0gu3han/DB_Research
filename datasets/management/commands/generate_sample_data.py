"""
Management command: python manage.py generate_sample_data

Creates a demo dataset with realistic-looking ward records.
"""
import random
from datetime import date, timedelta
from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from datasets.models import Dataset, WardRecord


WARD_PROFILES = [
    # (IP, OP, ER, UC, DS) — probability weights
    (1, 0, 0, 0, 0),
    (0, 1, 0, 0, 0),
    (0, 0, 1, 0, 0),
    (0, 0, 0, 1, 0),
    (0, 0, 0, 0, 1),
    (1, 0, 0, 0, 0),
    (0, 1, 0, 0, 0),
    (1, 0, 0, 0, 0),
    (0, 1, 0, 0, 0),
    (0, 0, 1, 0, 0),
]


class Command(BaseCommand):
    help = 'Generate a sample ward_type_deid_tj dataset for demo purposes'

    def add_arguments(self, parser):
        parser.add_argument('--rows', type=int, default=500,
                            help='Number of records to generate (default: 500)')
        parser.add_argument('--name', type=str, default='ward_type_deid_tj_demo',
                            help='Dataset name')

    def handle(self, *args, **options):
        rows = options['rows']
        name = options['name']

        self.stdout.write(f'Creating dataset "{name}" with {rows} records…')

        # Get or create superuser for ownership
        admin_user = User.objects.filter(is_superuser=True).first()

        dataset = Dataset.objects.create(
            name=name,
            description='Auto-generated demo dataset matching the ward_type_deid_tj format. '
                        'Contains synthetic anonymised patient encounter records.',
            created_by=admin_user,
            status='processing',
        )

        # Generate realistic patient pools
        patient_ids = random.sample(range(10000, 999999), min(rows // 3, 1000))
        enc_pool = random.sample(range(50000, 999999), rows)
        proc_pool = random.sample(range(50000, 999999), rows)

        start_date = date(1940, 1, 1)
        end_date = date(2090, 12, 31)
        date_range = (end_date - start_date).days

        records = []
        for i in range(rows):
            anon = random.choice(patient_ids)
            ward = random.choice(WARD_PROFILES)
            rec_date = start_date + timedelta(days=random.randint(0, date_range))
            records.append(WardRecord(
                dataset=dataset,
                anon_id=anon,
                pat_enc_csn_id_coded=enc_pool[i],
                order_proc_id_coded=proc_pool[i],
                order_time_jittered_utc_shifted=rec_date,
                hosp_ward_IP=ward[0],
                hosp_ward_OP=ward[1],
                hosp_ward_ER=ward[2],
                hosp_ward_UC=ward[3],
                hosp_ward_day_surg=ward[4],
            ))

        WardRecord.objects.bulk_create(records, batch_size=2000)

        dataset.row_count = rows
        dataset.status = 'ready'
        dataset.save()

        self.stdout.write(self.style.SUCCESS(
            f'✅  Dataset "{name}" created with {rows:,} records (pk={dataset.pk})'
        ))
