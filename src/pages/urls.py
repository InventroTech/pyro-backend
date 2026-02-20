from django.urls import path
from .views import PageListCreateView, PageDetailView

urlpatterns = [
    path('', PageListCreateView.as_view(), name='page-list-create'),
    path('<uuid:pk>/', PageDetailView.as_view(), name='page-detail'),
]
