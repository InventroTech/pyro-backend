
from django.urls import path
from .views import PasswordResetConfirmView, SupabaseAuthCheckView, SupabasePasswordRecoverView

app_name = "authentication"

urlpatterns = [
    path(
        'test-supabase-login/',
        SupabaseAuthCheckView.as_view(),
        name="authcheck"
    ),
    path(
        'forgot-password/',
        SupabasePasswordRecoverView.as_view(),
        name="forgot-password"
    ),
    path(
        'reset-password/confirm/',
        PasswordResetConfirmView.as_view(),
        name="reset-password-confirm"
    ),
]