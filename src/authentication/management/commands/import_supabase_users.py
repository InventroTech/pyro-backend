"""
python manage.py import_supabase_users [--dry-run] [--overwrite-passwords]

Reads Supabase ``auth.users`` / ``auth.identities`` from the database in the
SUPABASE_SOURCE_DB_URL environment variable (a Postgres connection URL), or from
Django's own database when it is not set, and copies the accounts into Django.
Safe to re-run.
"""
import os
from urllib.parse import urlsplit

import psycopg2
from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, connection, transaction

from authentication.supabase_import import fetch_supabase_rows, import_rows

SOURCE_ENV = "SUPABASE_SOURCE_DB_URL"


class _DryRun(Exception):
    pass


class Command(BaseCommand):
    help = "Copy Supabase Auth users (ids, passwords, confirmed emails, Google/Zoho links) into Django."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Show what would change, then roll back.")
        parser.add_argument(
            "--overwrite-passwords",
            action="store_true",
            help="Replace passwords already set in Django with the Supabase ones.",
        )

    def handle(self, *args, dry_run=False, overwrite_passwords=False, **options):
        source_url = os.environ.get(SOURCE_ENV, "").strip()
        try:
            if source_url:
                parts = urlsplit(source_url)
                self.stdout.write(f"Reading Supabase accounts from {parts.hostname}:{parts.port or 5432}{parts.path}")
                with psycopg2.connect(source_url) as source, source.cursor() as cursor:
                    users, identities = fetch_supabase_rows(cursor)
            else:
                db = connection.settings_dict
                self.stdout.write(f"Reading Supabase accounts from Django database {db['HOST']}/{db['NAME']}")
                with connection.cursor() as cursor:
                    users, identities = fetch_supabase_rows(cursor)
        except (psycopg2.Error, DatabaseError) as exc:
            raise CommandError(
                f"Could not read auth.users / auth.identities ({str(exc).splitlines()[0]}). "
                f"Set {SOURCE_ENV} to the Supabase database URL."
            ) from exc

        self.stdout.write(f"Found {len(users)} users and {len(identities)} Google/Zoho identities.")
        try:
            with transaction.atomic():
                report = import_rows(users, identities, overwrite_passwords=overwrite_passwords)
                if dry_run:
                    raise _DryRun
        except _DryRun:
            self.stdout.write(self.style.WARNING("Dry run: nothing was saved."))

        self.stdout.write(
            f"created={report.created} updated={report.updated} unchanged={report.unchanged} "
            f"passwords_set={report.passwords_set} identities_linked={report.identities_linked}"
        )
        for note in report.skipped:
            self.stdout.write(self.style.WARNING(f"skipped: {note}"))
        if not dry_run:
            self.stdout.write(self.style.SUCCESS("Import finished."))
