"""
python manage.py find_duplicate_user_emails

Read-only. Lists every email address held by more than one user (ignoring case), with
the details needed to decide which account to keep. Migration
authentication.0007_user_email_unique refuses to run until this prints nothing.
"""
from django.core.management.base import BaseCommand
from django.db.models import Count
from django.db.models.functions import Lower

from authentication.models import OAuthIdentity, User
from authz.models import TenantMembership


def duplicate_email_keys() -> list[str]:
    return list(
        User.objects.exclude(email__isnull=True)
        .exclude(email="")
        .annotate(email_key=Lower("email"))
        .values("email_key")
        .annotate(n=Count("id"))
        .filter(n__gt=1)
        .order_by("email_key")
        .values_list("email_key", flat=True)
    )


class Command(BaseCommand):
    help = "List email addresses shared by more than one user (read-only)."

    def handle(self, *args, **options):
        keys = duplicate_email_keys()
        if not keys:
            self.stdout.write(self.style.SUCCESS("No duplicate emails."))
            return

        self.stdout.write(self.style.WARNING(f"{len(keys)} email address(es) belong to more than one user:"))
        for key in keys:
            self.stdout.write(f"\n{key}")
            for user in User.objects.filter(email__iexact=key).order_by("pk"):
                memberships = TenantMembership.objects.filter(user_id=user.supabase_uid, is_active=True).count()
                identities = OAuthIdentity.objects.filter(user=user).count()
                self.stdout.write(
                    f"  pk={user.pk} uid={user.supabase_uid} active={user.is_active} "
                    f"last_login={user.last_login or '-'} verified={bool(user.email_verified_at)} "
                    f"password={user.has_usable_password()} memberships={memberships} oauth_links={identities}"
                )
