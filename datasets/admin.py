from django.contrib import admin
from .models import Dataset, WardRecord, LabRecord


@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ['name', 'status', 'row_count', 'created_by', 'created_at']
    list_filter = ['status', 'created_at']
    search_fields = ['name', 'description']
    readonly_fields = ['created_at', 'updated_at', 'row_count']


@admin.register(WardRecord)
class WardRecordAdmin(admin.ModelAdmin):
    list_display = [
        'anon_id', 'dataset', 'pat_enc_csn_id_coded',
        'order_proc_id_coded', 'order_time_jittered_utc_shifted',
        'hosp_ward_IP', 'hosp_ward_OP', 'hosp_ward_ER',
        'hosp_ward_UC', 'hosp_ward_day_surg',
    ]
    list_filter = [
        'dataset', 'hosp_ward_IP', 'hosp_ward_OP',
        'hosp_ward_ER', 'hosp_ward_UC', 'hosp_ward_day_surg',
    ]
    search_fields = ['anon_id', 'pat_enc_csn_id_coded']
    list_per_page = 100


@admin.register(LabRecord)
class LabRecordAdmin(admin.ModelAdmin):
    list_display  = ['caseid', 'dataset', 'dt', 'name', 'result']
    list_filter   = ['dataset', 'name']
    search_fields = ['caseid']
    list_per_page = 100