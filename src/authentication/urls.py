
from django.urls import path
from .account_views import ChangePasswordView, MeView, ResendConfirmationView, SignupView, VerifyEmailView
from .oauth_views import OAuthAuthorizeView, OAuthCallbackView, OAuthExchangeView
from .session_views import LoginView, LogoutView, SupabaseSessionExchangeView, TokenRefreshView
from .views import PasswordResetConfirmView, SupabasePasswordRecoverView

app_name = "authentication"

urlpatterns = [
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
    path('login/', LoginView.as_view(), name="login"),
    path('token/refresh/', TokenRefreshView.as_view(), name="token-refresh"),
    path('token/from-supabase/', SupabaseSessionExchangeView.as_view(), name="token-from-supabase"),
    path('logout/', LogoutView.as_view(), name="logout"),
    path('signup/', SignupView.as_view(), name="signup"),
    path('verify-email/', VerifyEmailView.as_view(), name="verify-email"),
    path('resend-confirmation/', ResendConfirmationView.as_view(), name="resend-confirmation"),
    path('me/', MeView.as_view(), name="me"),
    path('change-password/', ChangePasswordView.as_view(), name="change-password"),
    path('oauth/exchange/', OAuthExchangeView.as_view(), name="oauth-exchange"),
    path('oauth/<str:provider>/authorize/', OAuthAuthorizeView.as_view(), name="oauth-authorize"),
    path('oauth/<str:provider>/callback/', OAuthCallbackView.as_view(), name="oauth-callback"),
]
