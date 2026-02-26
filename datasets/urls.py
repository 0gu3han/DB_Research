from django.urls import path
from . import views

urlpatterns = [
    # Dashboard
    path('', views.home, name='home'),

    # Datasets
    path('datasets/', views.dataset_list, name='dataset_list'),
    path('datasets/upload/', views.dataset_upload, name='dataset_upload'),
    path('datasets/<int:pk>/', views.dataset_detail, name='dataset_detail'),
    path('datasets/<int:pk>/export/', views.dataset_export, name='dataset_export'),
    path('datasets/<int:pk>/delete/', views.dataset_delete, name='dataset_delete'),

    # API for DataTables server-side processing
    path('api/datasets/<int:pk>/records/', views.api_records, name='api_records'),
    path('api/dashboard/stats/', views.api_dashboard_stats, name='api_dashboard_stats'),

    # Auth
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
    path('register/', views.register_view, name='register'),
]
