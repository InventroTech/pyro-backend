from django.urls import path
from .views import FileUploadView, FileDownloadView, FileDeleteView, FileListView

app_name = 'files'

urlpatterns = [
    path('upload/', FileUploadView.as_view(), name='file-upload'),
    path('download/<uuid:file_id>/', FileDownloadView.as_view(), name='file-download'),
    path('delete/<uuid:file_id>/', FileDeleteView.as_view(), name='file-delete'),
    path('list/', FileListView.as_view(), name='file-list'),
]
