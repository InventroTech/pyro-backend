# urls.py
from django.urls import path
from .views import SupabaseAuthCheckView

urlpatterns = [
    path('test-supabase-login/', SupabaseAuthCheckView.as_view(), name = "authcheck"),
]
