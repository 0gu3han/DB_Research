from django.db import models
from django.contrib.auth.models import User
import os


class Dataset(models.Model):
    STATUS_CHOICES = [
        ('processing', 'Processing'),
        ('ready', 'Ready'),
        ('error', 'Error'),
    ]
    SCHEMA_CHOICES = [
        ('ward',                'Ward Encounters (ward_type_deid_tj)'),
        ('lab',                 'Lab Results (lab_data)'),
        ('ADI',                 'Area Deprivation Index (ADI)'),
        ('comorbidity',         'Comorbidity (ICD-10)'),
        ('demographics',        'Demographics'),
        ('nursing_home_visits', 'Nursing Home Visits'),
    ]

    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    schema_type = models.CharField(max_length=20, choices=SCHEMA_CHOICES, default='ward')
    file = models.FileField(upload_to='datasets/', null=True, blank=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    row_count = models.IntegerField(default=0)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='processing')

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return self.name

    @property
    def file_name(self):
        if self.file:
            return os.path.basename(self.file.name)
        return None

    @property
    def status_badge_class(self):
        mapping = {
            'ready': 'badge-ready',
            'processing': 'badge-processing',
            'error': 'badge-error',
        }
        return mapping.get(self.status, 'badge-processing')


class WardRecord(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='ward_records')
    anon_id = models.BigIntegerField(db_index=True)
    pat_enc_csn_id_coded = models.BigIntegerField()
    order_proc_id_coded = models.BigIntegerField()
    order_time_jittered_utc_shifted = models.DateField(null=True, blank=True)
    hosp_ward_IP = models.SmallIntegerField(default=0)
    hosp_ward_OP = models.SmallIntegerField(default=0)
    hosp_ward_ER = models.SmallIntegerField(default=0)
    hosp_ward_UC = models.SmallIntegerField(default=0)
    hosp_ward_day_surg = models.SmallIntegerField(default=0)

    class Meta:
        ordering = ['anon_id', 'order_time_jittered_utc_shifted']
        indexes = [
            models.Index(fields=['anon_id']),
            models.Index(fields=['order_time_jittered_utc_shifted']),
            models.Index(fields=['dataset', 'anon_id']),
        ]

    def __str__(self):
        return f"Record {self.anon_id} — {self.dataset.name}"

    @property
    def ward_label(self):
        wards = []
        if self.hosp_ward_IP:
            wards.append(('IP', 'Inpatient'))
        if self.hosp_ward_OP:
            wards.append(('OP', 'Outpatient'))
        if self.hosp_ward_ER:
            wards.append(('ER', 'Emergency Room'))
        if self.hosp_ward_UC:
            wards.append(('UC', 'Urgent Care'))
        if self.hosp_ward_day_surg:
            wards.append(('DS', 'Day Surgery'))
        return wards


# ─── Lab Results ──────────────────────────────────────────────────────────────

class LabRecord(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='lab_records')
    caseid  = models.BigIntegerField(db_index=True)
    dt      = models.BigIntegerField()          
    name    = models.CharField(max_length=50, db_index=True)
    result  = models.FloatField(null=True, blank=True)

    class Meta:
        ordering = ['caseid', 'dt']
        indexes = [
            models.Index(fields=['caseid']),
            models.Index(fields=['name']),
            models.Index(fields=['dataset', 'caseid']),
            models.Index(fields=['dataset', 'name']),
        ]

    def __str__(self):
        return f"Case {self.caseid} | {self.name}={self.result} (dt={self.dt})"


# ─── Area Deprivation Index ────────────────────────────────────────────────────

class ADIRecord(models.Model):
    dataset                          = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='adi_records')
    anon_id                          = models.BigIntegerField(db_index=True)
    pat_enc_csn_id_coded             = models.BigIntegerField()
    order_proc_id_coded              = models.BigIntegerField()
    adi_score                        = models.FloatField(null=True, blank=True)
    adi_state_rank                   = models.IntegerField(null=True, blank=True)
    order_time_jittered_utc_shifted  = models.DateField(null=True, blank=True)

    class Meta:
        ordering = ['anon_id']
        indexes = [
            models.Index(fields=['anon_id']),
            models.Index(fields=['dataset', 'anon_id']),
        ]

    def __str__(self):
        return f"ADI {self.anon_id} score={self.adi_score}"


# ─── Comorbidity ──────────────────────────────────────────────────────────────

class ComorbidityRecord(models.Model):
    dataset                          = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='comorbidity_records')
    anon_id                          = models.BigIntegerField(db_index=True)
    pat_enc_csn_id_coded             = models.BigIntegerField()
    order_proc_id_coded              = models.BigIntegerField()
    ICD10                            = models.CharField(max_length=20, db_index=True)
    category                         = models.CharField(max_length=100, blank=True)
    order_time_jittered_utc_shifted  = models.DateField(null=True, blank=True)

    class Meta:
        ordering = ['anon_id']
        indexes = [
            models.Index(fields=['anon_id']),
            models.Index(fields=['ICD10']),
            models.Index(fields=['dataset', 'anon_id']),
        ]

    def __str__(self):
        return f"Comorbidity {self.anon_id} {self.ICD10}"


# ─── Demographics ─────────────────────────────────────────────────────────────

class DemographicsRecord(models.Model):
    dataset              = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='demographics_records')
    anon_id              = models.BigIntegerField(db_index=True)
    pat_enc_csn_id_coded = models.BigIntegerField()
    order_proc_id_coded  = models.BigIntegerField()
    age                  = models.IntegerField(null=True, blank=True)
    gender               = models.CharField(max_length=20, blank=True)

    class Meta:
        ordering = ['anon_id']
        indexes = [
            models.Index(fields=['anon_id']),
            models.Index(fields=['dataset', 'anon_id']),
        ]

    def __str__(self):
        return f"Demographics {self.anon_id} age={self.age} gender={self.gender}"


# ─── Nursing Home Visits ──────────────────────────────────────────────────────

class NursingHomeVisitRecord(models.Model):
    dataset                          = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='nursing_home_visit_records')
    anon_id                          = models.BigIntegerField(db_index=True)
    pat_enc_csn_id_coded             = models.BigIntegerField()
    order_proc_id_coded              = models.BigIntegerField()
    nursing_home_visit_culture       = models.IntegerField(null=True, blank=True)
    order_time_jittered_utc_shifted  = models.DateField(null=True, blank=True)
    visit_date_shifted               = models.DateField(null=True, blank=True)

    class Meta:
        ordering = ['anon_id']
        indexes = [
            models.Index(fields=['anon_id']),
            models.Index(fields=['dataset', 'anon_id']),
        ]

    def __str__(self):
        return f"NHVisit {self.anon_id} culture={self.nursing_home_visit_culture}"
