
from django.urls import path
from .views import (
    SupabaseAuthCheckView,
    SupabaseSignUpView,
    SupabaseSessionView,
    SupabaseSignOutView,
    SupabaseOAuthView,
    InviteUserView,
    UpdateUserRoleView,
)

app_name = "authentication"

urlpatterns = [
    path('test-supabase-login/', SupabaseAuthCheckView.as_view(), name="authcheck"),
    path('signup/', SupabaseSignUpView.as_view(), name="signup"),
    path('session/', SupabaseSessionView.as_view(), name="session"),
    path('signout/', SupabaseSignOutView.as_view(), name="signout"),
    path('oauth/', SupabaseOAuthView.as_view(), name="oauth"),
    path('invite-user/', InviteUserView.as_view(), name="invite-user"),
    path('users/<uuid:user_id>/', UpdateUserRoleView.as_view(), name="update-user-role"),
]