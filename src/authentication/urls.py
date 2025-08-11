
from django.urls import path
from .views import (SupabaseAuthCheckView)
app_name = "authentication"

urlpatterns = [
    path(
        'test-supabase-login/',
        SupabaseAuthCheckView.as_view(),
        name="authcheck"
    ),
]