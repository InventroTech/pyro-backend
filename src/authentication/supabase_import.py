"""
Copy Supabase Auth accounts (``auth.users`` / ``auth.identities``) into Django so users
keep their id, password, confirmed email and Google / Zoho links after the switch.

Supabase stores bcrypt hashes; Django checks them as ``bcrypt$<hash>`` and re-hashes
with PBKDF2 on the user's next successful login.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timezone

from django.db import transaction

from authentication.models import OAuthIdentity, User

USERS_SQL = """
    SELECT id::text, lower(email), encrypted_password, email_confirmed_at, raw_user_meta_data
    FROM auth.users
    WHERE email IS NOT NULL AND email <> ''
      AND deleted_at IS NULL
      AND coalesce(is_anonymous, false) = false
    ORDER BY created_at
"""

IDENTITIES_SQL = """
    SELECT user_id::text, provider, provider_id, lower(coalesce(identity_data->>'email', ''))
    FROM auth.identities
    WHERE provider <> 'email'
"""

PROVIDER_NAMES = {"google": "google", "custom:zoho": "zoho", "zoho": "zoho"}


@dataclass
class ImportReport:
    created: int = 0
    updated: int = 0
    unchanged: int = 0
    passwords_set: int = 0
    identities_linked: int = 0
    skipped: list[str] = field(default_factory=list)


def _naive_utc(value):
    # Settings use USE_TZ=False, so aware timestamps from auth.users must be made naive (UTC).
    if value is not None and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def fetch_supabase_rows(cursor) -> tuple[list[dict], list[dict]]:
    cursor.execute(USERS_SQL)
    users = [
        {
            "id": r[0],
            "email": r[1],
            "encrypted_password": r[2],
            "email_confirmed_at": _naive_utc(r[3]),
            "user_metadata": r[4],
        }
        for r in cursor.fetchall()
    ]
    cursor.execute(IDENTITIES_SQL)
    identities = [
        {"user_id": r[0], "provider": r[1], "provider_id": r[2], "email": r[3]} for r in cursor.fetchall()
    ]
    return users, identities


def _django_password(encrypted_password: str | None) -> str | None:
    if encrypted_password and encrypted_password.startswith(("$2a$", "$2b$", "$2y$")):
        return f"bcrypt${encrypted_password}"
    return None


def import_rows(users: list[dict], identities: list[dict], *, overwrite_passwords: bool = False) -> ImportReport:
    """
    Create or update one Django user per Supabase user (matched on ``supabase_uid``).
    Passwords already set in Django are kept unless ``overwrite_passwords``.
    """
    report = ImportReport()
    with transaction.atomic():
        for row in users:
            user = User.objects.filter(supabase_uid=row["id"]).first()
            created = user is None
            if created:
                user = User(supabase_uid=row["id"], email=row["email"], is_active=True)
                user.set_unusable_password()
            changed = set()

            if not user.email:
                user.email = row["email"]
                changed.add("email")
            password = _django_password(row["encrypted_password"])
            if password and (overwrite_passwords or not user.has_usable_password()) and user.password != password:
                user.password = password
                changed.add("password")
                report.passwords_set += 1
            if row["email_confirmed_at"] and user.email_verified_at is None:
                user.email_verified_at = row["email_confirmed_at"]
                changed.add("email_verified_at")
            if row["user_metadata"] and not user.user_metadata:
                user.user_metadata = row["user_metadata"]
                changed.add("user_metadata")

            if created:
                user.save()
                report.created += 1
            elif changed:
                user.save(update_fields=sorted(changed))
                report.updated += 1
            else:
                report.unchanged += 1

        users_by_uid = {u.supabase_uid: u for u in User.objects.filter(supabase_uid__in=[r["id"] for r in users])}
        for row in identities:
            provider = PROVIDER_NAMES.get(row["provider"])
            user = users_by_uid.get(row["user_id"])
            if provider is None or user is None or not row["provider_id"]:
                report.skipped.append(f"identity {row['provider']} for {row['user_id']}")
                continue
            existing = OAuthIdentity.objects.filter(provider=provider, subject=row["provider_id"]).first()
            if existing is None:
                OAuthIdentity.objects.create(
                    user=user, provider=provider, subject=row["provider_id"], email=row["email"] or ""
                )
                report.identities_linked += 1
            elif existing.user_id != user.pk:
                report.skipped.append(f"identity {provider}:{row['provider_id']} already linked to another user")
    return report
