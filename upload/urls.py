from django.urls import path
from .views import  upload_files, transaction_results_view
# check_task_status

urlpatterns = [
    path('upload/', upload_files, name='upload'),
    path('transaction-results/', transaction_results_view, name='transaction_results'),

    # path('display/', display_data, name='display_data'),
    # path('task_status/', check_task_status, name='check_task_status'),
]