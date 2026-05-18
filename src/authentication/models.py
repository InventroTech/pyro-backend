from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin
from django.db import models

class UserManager(BaseUserManager):
    def create_user(self, supabase_uid, email, **extra_fields):
        if not email:
            raise ValueError('Users must have an email')
        email = self.normalize_email(email)
        user = self.model(supabase_uid=supabase_uid, email=email, **extra_fields)
        user.set_unusable_password()
        user.save(using=self._db)
        return user
    def create_superuser(self, supabase_uid, email, **extra_fields):
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)
        return self.create_user(supabase_uid, email, **extra_fields)
    
class User(AbstractBaseUser, PermissionsMixin):
    supabase_uid = models.CharField(max_length=255, unique=True)
    email = models.EmailField(null=True, blank=True)
    role = models.CharField(max_length=50, blank=True, null=True)
    tenant_id = models.CharField(max_length=255, blank=True, null=True)
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)

    objects = UserManager()
    USERNAME_FIELD = 'supabase_uid'
    REQUIRED_FIELDS = ['email']

    def __str__(self):
        return self.email


class PasswordResetOTP(models.Model):
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