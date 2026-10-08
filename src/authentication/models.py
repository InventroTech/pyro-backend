import uuid

from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin
from django.db import models
from object_history.tracking import HistoryTrackedModel

class UserManager(BaseUserManager):
    def create_user(self, supabase_uid, email, password=None, **extra_fields):
        if not email:
            raise ValueError('Users must have an email')
        email = self.normalize_email(email)
        user = self.model(supabase_uid=supabase_uid, email=email, **extra_fields)
        if password:
            user.set_password(password)
        else:
            user.set_unusable_password()
        user.save(using=self._db)
        return user
    def create_superuser(self, supabase_uid, email, **extra_fields):
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)
        return self.create_user(supabase_uid, email, **extra_fields)
    
class User(HistoryTrackedModel, AbstractBaseUser, PermissionsMixin):
    supabase_uid = models.CharField(max_length=255, unique=True)
    email = models.EmailField(null=True, blank=True)
    role = models.CharField(max_length=50, blank=True, null=True)
    tenant_id = models.CharField(max_length=255, blank=True, null=True)
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)
    email_verified_at = models.DateTimeField(null=True, blank=True)
    user_metadata = models.JSONField(default=dict, blank=True)

    objects = UserManager()
    USERNAME_FIELD = 'supabase_uid'
    REQUIRED_FIELDS = ['email']

    def __str__(self):
        return self.email


class PasswordResetOTP(HistoryTrackedModel, models.Model):
    """
    One-time password reset codes emailed to users. Expires after OTP_TTL_SECONDS (see views).
    Plain OTP is never stored — only HMAC digest.
    """

    email = models.EmailField(db_index=True)
    otp_hash = models.CharField(max_length=128)
    expires_at = models.DateTimeField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["email", "expires_at"], name="auth_pwreset_email_exp_idx"),
        ]

    def __str__(self):
        return f"PasswordResetOTP({self.email}, expires={self.expires_at})"


class RefreshToken(models.Model):
    """
    One login session step. Only the SHA-256 of the token is stored. Each refresh
    revokes the row and issues a new one in the same ``family_id``; presenting a
    revoked token again revokes the whole family (token theft defence).
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="refresh_tokens")
    token_hash = models.CharField(max_length=64, unique=True)
    family_id = models.UUIDField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()
    revoked_at = models.DateTimeField(null=True, blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(blank=True, default="")

    class Meta:
        indexes = [
            models.Index(fields=["user", "revoked_at"], name="auth_refresh_user_rev_idx"),
        ]

    def __str__(self):
        return f"RefreshToken(user={self.user_id}, family={self.family_id})"


class EmailVerificationToken(models.Model):
    """Single-use link token emailed at sign-up. Only the SHA-256 of the token is stored."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="email_verification_tokens")
    token_hash = models.CharField(max_length=64, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()
    used_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"EmailVerificationToken(user={self.user_id}, expires={self.expires_at})"


class OAuthIdentity(models.Model):
    """A Google / Zoho account linked to a user (``subject`` is the provider's user id)."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="oauth_identities")
    provider = models.CharField(max_length=32)
    subject = models.CharField(max_length=255)
    email = models.EmailField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    last_login_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["provider", "subject"], name="auth_oauth_provider_subject_uniq"),
        ]

    def __str__(self):
        return f"OAuthIdentity({self.provider}, user={self.user_id})"


class OAuthLoginCode(models.Model):
    """
    Short-lived single-use code handed to the frontend after a provider login; the
    frontend exchanges it for a session. Only the SHA-256 of the code is stored.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="oauth_login_codes")
    code_hash = models.CharField(max_length=64, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()
    used_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"OAuthLoginCode(user={self.user_id}, expires={self.expires_at})"
