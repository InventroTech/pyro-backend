#!/usr/bin/env python3
"""Copy the production database into preprod, then make that copy safe to boot.

The source and target are just Postgres connection settings, so the same
command works while production is still on Supabase and after it moves to
RDS. Preprod is already the AWS database. Run this on a machine in that VPC
(an EC2 instance next to RDS), not a laptop: the public data is about 158 GB.

  python3 scripts/sync_prod_to_preprod.py check
  python3 scripts/sync_prod_to_preprod.py incremental
  python3 scripts/sync_prod_to_preprod.py sync --confirm-overwrite-preprod

incremental runs when a pull request opens against preprod, before the load
test. It copies every record, sets leads to FRESH with no assignee, and copies groups, rule sets, entity type schemas,
and scoring rules. It does not copy tenants, users, history, or job queues.

Source env (first name wins): SOURCE_DB_* or PROD_SYNC_DB_*
Target env (first name wins): TARGET_DB_* or PREPROD_DB_*

Each side needs HOST, PORT, NAME, USER, PASSWORD.
Supabase dumps need the session pooler on port 5432. Port 6543 cannot dump.
When production is RDS, point SOURCE_DB_HOST at the RDS endpoint and SOURCE_DB_PORT at 5432.

sync refuses unless TARGET_DISK_BYTES is larger than the source data plus
20 percent. That is the allocated disk of the preprod database. Today the AWS
database is a few GB, so sync will not start until that disk is grown.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
import re
import secrets
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PSQL = "/opt/homebrew/bin/psql" if Path("/opt/homebrew/bin/psql").exists() else "psql"
PG_DUMP = "/opt/homebrew/bin/pg_dump" if Path("/opt/homebrew/bin/pg_dump").exists() else "pg_dump"
PG_RESTORE = "/opt/homebrew/bin/pg_restore" if Path("/opt/homebrew/bin/pg_restore").exists() else "pg_restore"
# Django 5.2 PBKDF2 iteration count. Login checks this hash.
PBKDF2_ITERATIONS = 1_000_000


def load_dotenv() -> None:
    path = REPO_ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        if not line or line.strip().startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip('"').strip("'")


def first_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


class Database:
    def __init__(self, label: str, host: str, port: str, name: str, user: str, password: str):
        self.label = label
        self.host = host
        self.port = port
        self.name = name
        self.user = user
        self.password = password

    @property
    def describe(self) -> str:
        return f"{self.label} {self.host}:{self.port}/{self.name}"

    def env(self) -> dict[str, str]:
        return {
            "PGPASSWORD": self.password,
            "PGSSLMODE": "require",
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        }

    def psql(self, sql: str, **params: str) -> str:
        command = [
            PSQL,
            "-h", self.host,
            "-p", self.port,
            "-U", self.user,
            "-d", self.name,
            "-v", "ON_ERROR_STOP=1",
            "-At",
        ]
        for name, value in params.items():
            if not name.isidentifier():
                raise SystemExit(f"Bad SQL parameter name {name}.")
            # psql splits -v on the first "=" and quotes :'name' as a literal.
            command.extend(["-v", f"{name}={value}"])
        command.extend(["-c", sql])
        result = subprocess.run(
            command,
            env=self.env(),
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "query failed").strip().splitlines()
            raise SystemExit(f"{self.describe}: {detail[-1] if detail else 'query failed'}")
        return result.stdout.strip()


def require_database(label: str, primary: str, fallback: str) -> Database:
    prefix = primary if first_env(f"{primary}_HOST") else fallback
    host = first_env(f"{prefix}_HOST")
    port = first_env(f"{prefix}_PORT")
    name = first_env(f"{prefix}_NAME")
    user = first_env(f"{prefix}_USER")
    password = first_env(f"{prefix}_PASSWORD")
    missing = [
        key
        for key, value in (
            ("HOST", host),
            ("PORT", port),
            ("NAME", name),
            ("USER", user),
            ("PASSWORD", password),
        )
        if not value
    ]
    if missing:
        raise SystemExit(
            f"Set {primary}_HOST/PORT/NAME/USER/PASSWORD "
            f"(or {fallback}_*) before running. Missing {', '.join(missing)}."
        )
    if port == "6543":
        raise SystemExit(
            f"{label} is using port 6543. Use the session port 5432. "
            "The transaction pooler cannot take a dump."
        )
    return Database(label, host, port, name, user, password)


def public_bytes(db: Database) -> int:
    raw = db.psql(
        "SELECT COALESCE(sum(pg_total_relation_size(c.oid)), 0) "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public'"
    )
    return int(raw or "0")


def pretty_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(value)} B"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def same_database(source: Database, target: Database) -> bool:
    return (
        source.host == target.host
        and source.port == target.port
        and source.name == target.name
        and source.user == target.user
    )


def cmd_check(source: Database, target: Database) -> int:
    source_bytes = public_bytes(source)
    target_bytes = public_bytes(target)
    print(source.describe)
    print(f"  public data {pretty_bytes(source_bytes)}")
    print(target.describe)
    print(f"  public data {pretty_bytes(target_bytes)}")
    rows = source.psql(
        "SELECT n.nspname || '.' || c.relname || ' ' || "
        "pg_size_pretty(pg_total_relation_size(c.oid)) "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relkind = 'r' "
        "ORDER BY pg_total_relation_size(c.oid) DESC LIMIT 8"
    )
    print("largest source tables:")
    print(rows or "  (none)")
    disk = os.environ.get("TARGET_DISK_BYTES", "").strip()
    needed = int(source_bytes * 1.2)
    if not disk:
        print(
            "sync is blocked until TARGET_DISK_BYTES is set to the preprod "
            f"disk size. The copy needs at least {pretty_bytes(needed)}."
        )
        return 0
    disk_bytes = int(disk)
    print(f"target disk {pretty_bytes(disk_bytes)}")
    if disk_bytes < needed:
        print(
            f"sync would be refused. {pretty_bytes(disk_bytes)} is smaller than "
            f"{pretty_bytes(needed)}."
        )
        return 1
    print("sync is allowed on this disk.")
    return 0


def sanitize(target: Database) -> None:
    statements = [
        """
        UPDATE email_protocol_zoho_mail_connection
        SET refresh_token = '',
            access_token = '',
            is_active = false,
            disconnected_at = COALESCE(disconnected_at, now())
        WHERE disconnected_at IS NULL OR is_active = true
        """,
        """
        UPDATE background_jobs
        SET status = 'FAILED'
        WHERE status IN ('PENDING', 'PROCESSING', 'RETRYING')
        """,
        """
        UPDATE pyro_job
        SET status = 'FAILED'
        WHERE status IN ('PENDING', 'RUNNING') AND is_deleted = false
        """,
    ]
    for statement in statements:
        table = statement.split()[1]
        exists = target.psql(
            "SELECT to_regclass('public." + table + "') IS NOT NULL"
        )
        if exists != "t":
            print(f"skip sanitize, {table} is not on the target")
            continue
        target.psql(statement)
        print(f"stopped outbound rows in {table}")


def django_password_hash(password: str) -> str:
    salt = secrets.token_hex(12)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode(),
        salt.encode(),
        PBKDF2_ITERATIONS,
    )
    encoded = base64.b64encode(digest).decode().strip()
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt}${encoded}"


def ensure_loadtest_user(target: Database) -> None:
    email = first_env("LOADTEST_USER_EMAIL").lower()
    password = first_env("LOADTEST_USER_PASSWORD")
    slug = first_env("LOADTEST_TENANT_SLUG") or "praja"
    if not email or not password:
        print(
            "Load-test login was not created. Set LOADTEST_USER_EMAIL and "
            "LOADTEST_USER_PASSWORD, then run: "
            "python3 scripts/sync_prod_to_preprod.py sanitize"
        )
        return
    tenant_id = target.psql(
        "SELECT id FROM tenants WHERE slug = :'slug' LIMIT 1",
        slug=slug,
    )
    if not tenant_id:
        raise SystemExit(f"No tenant with slug {slug} on the target.")
    role_id = target.psql(
        "SELECT role_id FROM authz_tenantmembership "
        "WHERE tenant_id = CAST(:'tenant_id' AS uuid) "
        "AND is_active = true AND is_deleted = false LIMIT 1",
        tenant_id=tenant_id,
    )
    if not role_id:
        raise SystemExit(f"Tenant {slug} has no active membership to copy a role from.")
    has_verified = target.psql(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = 'authentication_user' "
        "AND column_name = 'email_verified_at')"
    ) == "t"
    password_hash = django_password_hash(password)
    uid = str(uuid.uuid4())
    # Values go through psql :'name' placeholders, not into the SQL text.
    # INSERT does nothing when the email already exists. UPDATE always runs
    # so the load-test password replaces a production password the sync copied.
    if has_verified:
        target.psql(
            "INSERT INTO authentication_user "
            "(password, is_superuser, supabase_uid, email, is_active, is_staff, email_verified_at) "
            "SELECT :'password_hash', false, :'uid', :'email', true, false, now() "
            "WHERE NOT EXISTS ("
            "SELECT 1 FROM authentication_user WHERE lower(email) = :'email')",
            password_hash=password_hash,
            uid=uid,
            email=email,
        )
        target.psql(
            "UPDATE authentication_user SET password = :'password_hash', "
            "email_verified_at = now(), is_active = true "
            "WHERE lower(email) = :'email'",
            password_hash=password_hash,
            email=email,
        )
    else:
        target.psql(
            "INSERT INTO authentication_user "
            "(password, is_superuser, supabase_uid, email, is_active, is_staff) "
            "SELECT :'password_hash', false, :'uid', :'email', true, false "
            "WHERE NOT EXISTS ("
            "SELECT 1 FROM authentication_user WHERE lower(email) = :'email')",
            password_hash=password_hash,
            uid=uid,
            email=email,
        )
        target.psql(
            "UPDATE authentication_user SET password = :'password_hash', "
            "is_active = true WHERE lower(email) = :'email'",
            password_hash=password_hash,
            email=email,
        )
    user_uid = target.psql(
        "SELECT supabase_uid FROM authentication_user "
        "WHERE lower(email) = :'email' LIMIT 1",
        email=email,
    )
    target.psql(
        "INSERT INTO authz_tenantmembership "
        "(user_id, is_active, created_at, role_id, tenant_id, email, is_deleted, name) "
        "SELECT CAST(:'user_uid' AS uuid), true, now(), CAST(:'role_id' AS uuid), "
        "CAST(:'tenant_id' AS uuid), :'email', false, 'Load test' "
        "WHERE NOT EXISTS ("
        "SELECT 1 FROM authz_tenantmembership "
        "WHERE tenant_id = CAST(:'tenant_id' AS uuid) "
        "AND user_id = CAST(:'user_uid' AS uuid) "
        "AND is_deleted = false)",
        user_uid=user_uid,
        role_id=role_id,
        tenant_id=tenant_id,
        email=email,
    )
    target.psql(
        "UPDATE authz_tenantmembership SET is_active = true, is_deleted = false, "
        "role_id = CAST(:'role_id' AS uuid) "
        "WHERE tenant_id = CAST(:'tenant_id' AS uuid) AND lower(email) = :'email'",
        role_id=role_id,
        tenant_id=tenant_id,
        email=email,
    )
    print(f"load-test login {email} is active on tenant {slug}")


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def cmd_sync(source: Database, target: Database) -> int:
    source_bytes = public_bytes(source)
    needed = int(source_bytes * 1.2)
    disk = os.environ.get("TARGET_DISK_BYTES", "").strip()
    if not disk:
        raise SystemExit(
            "Set TARGET_DISK_BYTES to the preprod disk size in bytes. "
            f"The copy needs at least {pretty_bytes(needed)}."
        )
    if int(disk) < needed:
        raise SystemExit(
            f"Preprod disk is {pretty_bytes(int(disk))}. "
            f"Production data needs {pretty_bytes(needed)}. "
            "Grow the AWS disk, then run this again."
        )
    print(f"copying {pretty_bytes(source_bytes)} from {source.host} to {target.host}")
    with tempfile.TemporaryDirectory(prefix="pyro-db-sync-") as directory:
        dump_path = str(Path(directory) / "public.dump")
        dump = subprocess.run(
            [
                PG_DUMP,
                "-h", source.host,
                "-p", source.port,
                "-U", source.user,
                "-d", source.name,
                "--format=custom",
                "--no-owner",
                "--no-acl",
                "--schema=public",
                "-f", dump_path,
            ],
            env=source.env(),
        )
        if dump.returncode != 0:
            raise SystemExit("pg_dump failed. Nothing was written to preprod.")
        restore = subprocess.run(
            [
                PG_RESTORE,
                "-h", target.host,
                "-p", target.port,
                "-U", target.user,
                "-d", target.name,
                "--clean",
                "--if-exists",
                "--no-owner",
                "--no-acl",
                "--exit-on-error",
                dump_path,
            ],
            env=target.env(),
        )
        if restore.returncode != 0:
            raise SystemExit("pg_restore failed. Preprod may be partially overwritten. Run sanitize before starting the app.")
    sanitize(target)
    ensure_loadtest_user(target)
    print("preprod now has the production copy, with outbound jobs stopped.")
    return 0


# Tenants and users stay on the ids already in preprod.
# Every record is copied. Only leads are set to FRESH with assigned_to cleared.
FULL_TABLES = (
    "groups",
    "rule_sets",
    "entity_type_schemas",
    "scoring_rules",
)
PARENT_FIXES = (
    ("event_logs", "record_id", "records"),
    ("rm_call_sessions", "lead_record_id", "records"),
)
BATCH = 1000


def psql_file(db: Database, path: str) -> str:
    result = subprocess.run(
        [
            PSQL, "-h", db.host, "-p", db.port, "-U", db.user, "-d", db.name,
            "-v", "ON_ERROR_STOP=1", "-At", "-f", path,
        ],
        env=db.env(),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "script failed").strip().splitlines()
        raise SystemExit(f"{db.describe}: {detail[-1] if detail else 'script failed'}")
    return result.stdout.strip()


def export_query(source: Database, sql: str, path: str) -> int:
    with open(path, "w", encoding="utf-8") as handle:
        result = subprocess.run(
            [
                PSQL, "-h", source.host, "-p", source.port, "-U", source.user,
                "-d", source.name, "-v", "ON_ERROR_STOP=1",
                "-c", "COPY (" + sql + ") TO STDOUT WITH (FORMAT csv, HEADER true, NULL '\\N')",
            ],
            env=source.env(),
            stdout=handle,
            stderr=subprocess.PIPE,
            text=True,
        )
    if result.returncode != 0:
        detail = (result.stderr or "export failed").strip().splitlines()
        raise SystemExit(f"{source.describe}: {detail[-1] if detail else 'export failed'}")
    with open(path, encoding="utf-8") as handle:
        lines = sum(1 for _ in handle)
    return max(0, lines - 1)


def primary_key(db: Database, table: str) -> str:
    rows = db.psql(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = " + quote_literal("public." + table) + "::regclass "
        "AND i.indisprimary"
    )
    names = [line for line in rows.splitlines() if line]
    if len(names) != 1:
        return ""
    return names[0]


def column_type(db: Database, table: str, column: str) -> str:
    return db.psql(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = " + quote_literal(table)
        + " AND column_name = " + quote_literal(column)
    )


def table_ready(db: Database, table: str) -> bool:
    return db.psql("SELECT to_regclass(" + quote_literal("public." + table) + ") IS NOT NULL") == "t"


def ensure_state(target: Database) -> None:
    target.psql(
        "CREATE TABLE IF NOT EXISTS preprod_sync_state ("
        "table_name text PRIMARY KEY, "
        "cursor_at timestamptz NOT NULL, "
        "cursor_id text NOT NULL DEFAULT '', "
        "caught_up boolean NOT NULL DEFAULT false"
        ")"
    )
    target.psql(
        "ALTER TABLE preprod_sync_state "
        "ADD COLUMN IF NOT EXISTS caught_up boolean NOT NULL DEFAULT false"
    )


def read_scan(target: Database, table: str) -> tuple[bool, str, str]:
    row = target.psql(
        "SELECT caught_up::text || '|' || cursor_at::text || '|' || cursor_id "
        "FROM preprod_sync_state WHERE table_name = " + quote_literal(table)
    )
    if not row or row.count("|") < 2:
        return False, "1970-01-01 00:00:00+00", "-1"
    caught_up, cursor_at, cursor_id = row.split("|", 2)
    return caught_up == "t", cursor_at, cursor_id or "-1"


def write_scan(target: Database, table: str, cursor_at: str, cursor_id: str, caught_up: bool) -> None:
    target.psql(
        "INSERT INTO preprod_sync_state (table_name, cursor_at, cursor_id, caught_up) VALUES ("
        + quote_literal(table) + ", " + quote_literal(cursor_at) + "::timestamptz, "
        + quote_literal(cursor_id) + ", " + ("true" if caught_up else "false")
        + ") ON CONFLICT (table_name) DO UPDATE SET "
        "cursor_at = EXCLUDED.cursor_at, cursor_id = EXCLUDED.cursor_id, "
        "caught_up = EXCLUDED.caught_up"
    )


def header_columns(path: str) -> list[str]:
    with open(path, encoding="utf-8") as handle:
        header = handle.readline().rstrip("\r\n")
    return [part.strip().strip('"') for part in header.split(",") if part.strip()]


def table_columns(db: Database, table: str) -> list[str]:
    return [
        line for line in db.psql(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = " + quote_literal(table)
            + " ORDER BY ordinal_position"
        ).splitlines() if line
    ]


def common_columns(source: Database, target: Database, table: str, pk: str) -> list[str]:
    present = set(table_columns(source, table))
    columns = [name for name in table_columns(target, table) if name in present]
    if pk not in columns:
        raise SystemExit(f"{table} has no {pk} column on both databases.")
    return columns


def shared_columns(target: Database, table: str, csv_path: str, pk: str) -> list[str]:
    present = set(table_columns(target, table))
    columns = [name for name in header_columns(csv_path) if name in present]
    if pk not in columns:
        raise SystemExit(f"{table} has no {pk} column on both databases.")
    return columns


def tenant_id_pairs(source: Database, target: Database) -> list[tuple[str, str]]:
    cached = getattr(tenant_id_pairs, "cache", None)
    if cached is not None:
        return cached
    target_by_slug = {}
    for line in target.psql("SELECT slug || '|' || id::text FROM tenants").splitlines():
        slug, _, tenant_id = line.partition("|")
        if slug and tenant_id:
            target_by_slug[slug] = tenant_id
    pairs = []
    for line in source.psql("SELECT id::text || '|' || slug FROM tenants").splitlines():
        prod_id, _, slug = line.partition("|")
        preprod_id = target_by_slug.get(slug, "")
        if prod_id and preprod_id and prod_id != preprod_id:
            pairs.append((prod_id, preprod_id))
    tenant_id_pairs.cache = pairs
    return pairs


def prepare_staging_sql(source: Database, target: Database, table: str, staging: str) -> list[str]:
    """Point copied rows at the preprod tenant with the same slug. Do not change tenant or user ids."""
    statements = []
    if table == "records":
        statements.append(
            f"UPDATE {staging} SET data = jsonb_set("
            f"jsonb_set(COALESCE(data, '{{}}'::jsonb), '{{lead_stage}}', '\"FRESH\"'::jsonb, true), "
            f"'{{assigned_to}}', 'null'::jsonb, true) "
            f"WHERE entity_type = 'lead';"
        )
    if "tenant_id" in set(table_columns(target, table)):
        pairs = tenant_id_pairs(source, target)
        if pairs:
            values = ", ".join(
                f"({quote_literal(prod_id)}::uuid, {quote_literal(preprod_id)}::uuid)"
                for prod_id, preprod_id in pairs
            )
            # Leads keep the preprod tenant id for that slug. The production id is not stored.
            lead_only = " AND s.entity_type = 'lead'" if table == "records" else ""
            statements.append(
                f"UPDATE {staging} AS s SET tenant_id = v.preprod_id "
                f"FROM (VALUES {values}) AS v(prod_id, preprod_id) "
                f"WHERE s.tenant_id = v.prod_id{lead_only};"
            )
            if table == "records":
                statements.append(
                    f"UPDATE {staging} AS s SET tenant_id = v.preprod_id "
                    f"FROM (VALUES {values}) AS v(prod_id, preprod_id) "
                    f"WHERE s.tenant_id = v.prod_id AND s.entity_type IS DISTINCT FROM 'lead';"
                )
        statements.append(
            f"DELETE FROM {staging} AS s WHERE s.tenant_id IS NOT NULL "
            f"AND NOT EXISTS (SELECT 1 FROM tenants t WHERE t.id = s.tenant_id);"
        )
    for column, parent in (("role_id", "authz_role"),):
        if column not in set(table_columns(target, table)):
            continue
        statements.append(
            f"UPDATE {staging} AS s SET {column} = NULL "
            f"WHERE s.{column} IS NOT NULL "
            f"AND NOT EXISTS (SELECT 1 FROM {parent} p WHERE p.id = s.{column});"
        )
    return statements


def qualify_predicate(predicate: str, alias: str, column_names: list[str]) -> str:
    if not predicate:
        return "true"
    text = predicate
    for column in sorted(column_names, key=len, reverse=True):
        text = re.sub(rf"\b{re.escape(column)}\b", f"{alias}.{column}", text)
    return text


def keep_existing_ids_sql(target: Database, table: str, pk: str, staging: str, columns: list[str]) -> list[str]:
    """Drop a staged row when preprod already has that unique name. Leave the existing row alone."""
    indexes = target.psql(
        "SELECT i.indexrelid::text || '|' || COALESCE(pg_get_expr(i.indpred, i.indrelid), '') "
        "FROM pg_index i "
        "WHERE i.indrelid = " + quote_literal("public." + table) + "::regclass "
        "AND i.indisunique AND NOT i.indisprimary AND i.indexprs IS NULL"
    )
    statements = []
    for line in indexes.splitlines():
        if "|" not in line:
            continue
        index_id, predicate = line.split("|", 1)
        key_columns = [
            name for name in target.psql(
                "SELECT a.attname FROM pg_index i "
                "JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true "
                "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum "
                "WHERE i.indexrelid = " + quote_literal(index_id) + "::oid "
                "AND k.attnum > 0 ORDER BY k.ord"
            ).splitlines() if name
        ]
        if not key_columns:
            continue
        match = " AND ".join(
            f"t.{name} IS NOT DISTINCT FROM s.{name}" for name in key_columns
        )
        alive = qualify_predicate(predicate, "t", columns)
        statements.append(
            f"DELETE FROM {staging} AS s USING {table} AS t "
            f"WHERE {match} AND t.{pk} IS DISTINCT FROM s.{pk} AND ({alive});"
        )
    return statements


def upsert_csv(source: Database, target: Database, table: str, csv_path: str, pk: str) -> str:
    columns = shared_columns(target, table, csv_path, pk)
    assigned = ", ".join(f"{name} = EXCLUDED.{name}" for name in columns if name != pk)
    column_list = ", ".join(columns)
    # preprod_sync_catalog, not preprod_sync_batch: a parent upsert must not
    # wipe the child batch that is still waiting to be inserted.
    script = "\n".join([
        "DROP TABLE IF EXISTS preprod_sync_catalog;",
        f"CREATE UNLOGGED TABLE preprod_sync_catalog (LIKE {table} INCLUDING DEFAULTS);",
        f"\\copy preprod_sync_catalog ({column_list}) FROM '{csv_path}' WITH (FORMAT csv, HEADER true, NULL '\\N')",
        *prepare_staging_sql(source, target, table, "preprod_sync_catalog"),
        *keep_existing_ids_sql(target, table, pk, "preprod_sync_catalog", columns),
        f"INSERT INTO {table} ({column_list}) SELECT {column_list} FROM preprod_sync_catalog",
        f"ON CONFLICT ({pk}) DO UPDATE SET {assigned};",
        "DROP TABLE preprod_sync_catalog;",
    ])
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as handle:
        handle.write(script)
        script_path = handle.name
    try:
        psql_file(target, script_path)
    finally:
        Path(script_path).unlink(missing_ok=True)
    return ""


def sync_batch(source: Database, target: Database, table: str, sql: str, pk: str, cursor_col: str) -> tuple[int, str, str]:
    with tempfile.TemporaryDirectory(prefix="pyro-sync-") as directory:
        csv_path = str(Path(directory) / "batch.csv")
        count = export_query(source, sql, csv_path)
        if count == 0:
            return 0, "", ""
        columns = shared_columns(target, table, csv_path, pk)
        assigned = ", ".join(f"{name} = EXCLUDED.{name}" for name in columns if name != pk)
        column_list = ", ".join(columns)
        parent_sql = ""
        for child, fk, parent in PARENT_FIXES:
            if child != table:
                continue
            parent_sql = (
                f"SELECT {fk}::text FROM preprod_sync_batch "
                f"WHERE {fk} IS NOT NULL AND NOT EXISTS ("
                f"SELECT 1 FROM {parent} p WHERE p.id = preprod_sync_batch.{fk});"
            )
        script = "\n".join([
            "DROP TABLE IF EXISTS preprod_sync_batch;",
            f"CREATE UNLOGGED TABLE preprod_sync_batch (LIKE {table} INCLUDING DEFAULTS);",
            f"\\copy preprod_sync_batch ({column_list}) FROM '{csv_path}' WITH (FORMAT csv, HEADER true, NULL '\\N')",
            "\\echo PARENTS",
            parent_sql or "SELECT '';",
            f"SELECT {cursor_col}::text || '|' || {pk}::text FROM preprod_sync_batch "
            f"ORDER BY {cursor_col} DESC, {pk} DESC LIMIT 1;",
        ])
        with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as handle:
            handle.write(script)
            script_path = handle.name
        try:
            output = psql_file(target, script_path)
        finally:
            Path(script_path).unlink(missing_ok=True)
        lines = [line for line in output.splitlines() if line and line != "PARENTS"]
        # psql \\echo prints PARENTS. Missing parent ids come before the cursor line.
        cursor_line = lines[-1] if lines else ""
        parent_ids = lines[:-1] if cursor_line and "|" in cursor_line else lines
        if parent_ids and parent_ids != [""]:
            for child, fk, parent in PARENT_FIXES:
                if child != table:
                    continue
                id_list = ", ".join(quote_literal(item) + "::" + (
                    "uuid" if "-" in item else "bigint"
                ) for item in parent_ids if item)
                if not id_list:
                    continue
                parent_pk = primary_key(source, parent)
                parent_columns = ", ".join(common_columns(source, target, parent, parent_pk or "id"))
                with tempfile.TemporaryDirectory(prefix="pyro-sync-parent-") as parent_dir:
                    parent_csv = str(Path(parent_dir) / "parents.csv")
                    export_query(
                        source,
                        f"SELECT {parent_columns} FROM {parent} WHERE id IN ({id_list})",
                        parent_csv,
                    )
                    upsert_csv(source, target, parent, parent_csv, parent_pk or "id")
        insert_script = "\n".join([
            *prepare_staging_sql(source, target, table, "preprod_sync_batch"),
            *keep_existing_ids_sql(target, table, pk, "preprod_sync_batch", columns),
            f"INSERT INTO {table} ({column_list})",
            f"SELECT {column_list} FROM preprod_sync_batch",
            f"ON CONFLICT ({pk}) DO UPDATE SET {assigned};",
            "DROP TABLE preprod_sync_batch;",
        ])
        with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as handle:
            handle.write(insert_script)
            insert_path = handle.name
        try:
            psql_file(target, insert_path)
        finally:
            Path(insert_path).unlink(missing_ok=True)
        if "|" not in cursor_line:
            return count, "", ""
        cursor_at, cursor_id = cursor_line.split("|", 1)
        return count, cursor_at, cursor_id


def key_page(source: Database, table: str, pk: str, cursor_col: str, cast: str, after_id: str) -> list[tuple[str, str]]:
    text = source.psql(
        f"SELECT {pk}::text || '|' || COALESCE({cursor_col}::text, '') FROM {table} "
        f"WHERE {pk} > {quote_literal(after_id)}{cast} "
        f"ORDER BY {pk} LIMIT {BATCH}"
    )
    page = []
    for line in text.splitlines():
        if not line:
            continue
        row_id, _, stamp = line.partition("|")
        if row_id:
            page.append((row_id, stamp))
    return page


def ids_missing_on_target(
    target: Database, table: str, pk: str, cursor_col: str, cast: str, page: list[tuple[str, str]]
) -> list[str]:
    values = ", ".join(
        "(" + quote_literal(row_id) + cast + ", "
        + (quote_literal(stamp) + "::timestamptz" if stamp else "NULL::timestamptz")
        + ")"
        for row_id, stamp in page
    )
    text = target.psql(
        "SELECT s.id::text FROM (VALUES " + values + ") AS s(id, cursor_at) "
        f"LEFT JOIN {table} t ON t.{pk} = s.id "
        f"WHERE t.{pk} IS NULL OR ("
        "s.cursor_at IS NOT NULL AND ("
        f"t.{cursor_col} IS NULL OR t.{cursor_col} < s.cursor_at))"
    )
    return [line for line in text.splitlines() if line]


def copy_ids(
    source: Database, target: Database, table: str, columns: str, pk: str, cursor_col: str, cast: str, ids: list[str]
) -> int:
    if not ids:
        return 0
    id_list = ", ".join(quote_literal(row_id) + cast for row_id in ids)
    count, _, _ = sync_batch(
        source, target, table,
        f"SELECT {columns} FROM {table} WHERE {pk} IN ({id_list})",
        pk, cursor_col,
    )
    return count


def sync_changed_table(source: Database, target: Database, table: str, cursor_col: str) -> int:
    if not table_ready(source, table) or not table_ready(target, table):
        print(f"skip {table}, not on both databases")
        return 0
    pk = primary_key(source, table)
    if not pk:
        print(f"skip {table}, no single primary key")
        return 0
    if not column_type(source, table, cursor_col):
        print(f"skip {table}, no {cursor_col}")
        return 0
    pk_type = column_type(source, table, pk)
    cast = "::uuid" if "uuid" in pk_type else "::bigint"
    columns = ", ".join(common_columns(source, target, table, pk))
    caught_up, cursor_at, cursor_id = read_scan(target, table)
    run_started = target.psql("SELECT now()::text")
    copied = 0
    if not caught_up:
        # Walk every production id. Copy a row when preprod lacks it, or when
        # production has a newer copy. Already-matching rows are skipped.
        while True:
            page = key_page(source, table, pk, cursor_col, cast, cursor_id)
            if not page:
                max_id = source.psql(
                    f"SELECT COALESCE(max({pk})::text, {quote_literal(cursor_id)}) FROM {table}"
                ) or cursor_id
                write_scan(target, table, run_started, max_id, True)
                break
            copied += copy_ids(
                source, target, table, columns, pk, cursor_col, cast,
                ids_missing_on_target(target, table, pk, cursor_col, cast, page),
            )
            cursor_id = page[-1][0]
            write_scan(target, table, cursor_at, cursor_id, False)
    else:
        # Preprod already has every id from the last completed walk.
        # Copy edits to those rows, then ids created since that walk.
        edit_after = "-1" if "uuid" not in pk_type else "00000000-0000-0000-0000-000000000000"
        while True:
            text = source.psql(
                f"SELECT {pk}::text FROM {table} "
                f"WHERE {pk} > {quote_literal(edit_after)}{cast} "
                f"AND {pk} <= {quote_literal(cursor_id)}{cast} "
                f"AND {cursor_col} > {quote_literal(cursor_at)}::timestamptz "
                f"ORDER BY {pk} LIMIT {BATCH}"
            )
            ids = [line for line in text.splitlines() if line]
            if not ids:
                break
            copied += copy_ids(source, target, table, columns, pk, cursor_col, cast, ids)
            edit_after = ids[-1]
        while True:
            text = source.psql(
                f"SELECT {pk}::text FROM {table} "
                f"WHERE {pk} > {quote_literal(cursor_id)}{cast} "
                f"ORDER BY {pk} LIMIT {BATCH}"
            )
            ids = [line for line in text.splitlines() if line]
            if not ids:
                break
            copied += copy_ids(source, target, table, columns, pk, cursor_col, cast, ids)
            cursor_id = ids[-1]
            write_scan(target, table, cursor_at, cursor_id, True)
        max_id = source.psql(
            f"SELECT COALESCE(max({pk})::text, {quote_literal(cursor_id)}) FROM {table}"
        ) or cursor_id
        write_scan(target, table, run_started, max_id, True)
    print(f"{table}: {copied} rows")
    return copied


def sync_whole_table(source: Database, target: Database, table: str) -> int:
    if not table_ready(source, table) or not table_ready(target, table):
        print(f"skip {table}, not on both databases")
        return 0
    pk = primary_key(source, table)
    if not pk:
        print(f"skip {table}, no single primary key")
        return 0
    count_raw = source.psql(f"SELECT count(*) FROM {table}")
    if int(count_raw or "0") > 100000:
        print(f"skip {table}, {count_raw} rows is too many for a full pass")
        return 0
    columns = ", ".join(common_columns(source, target, table, pk))
    with tempfile.TemporaryDirectory(prefix="pyro-sync-full-") as directory:
        csv_path = str(Path(directory) / "full.csv")
        count = export_query(source, f"SELECT {columns} FROM {table}", csv_path)
        if count == 0:
            print(f"{table}: 0 rows")
            return 0
        upsert_csv(source, target, table, csv_path, pk)
    print(f"{table}: {count} rows")
    return count


def sync_records(source: Database, target: Database) -> int:
    table = "records"
    if not table_ready(source, table) or not table_ready(target, table):
        print("skip records, not on both databases")
        return 0
    pk = primary_key(source, table) or "id"
    columns = ", ".join(common_columns(source, target, table, pk))
    cursor = "-1"
    copied = 0
    while True:
        text = source.psql(
            "SELECT id::text FROM records WHERE id > "
            + quote_literal(cursor) + "::bigint ORDER BY id LIMIT " + str(BATCH)
        )
        ids = [line for line in text.splitlines() if line]
        if not ids:
            break
        copied += copy_ids(source, target, table, columns, pk, "updated_at", "::bigint", ids)
        cursor = ids[-1]
        if len(ids) < BATCH:
            break
    print(f"records: {copied} rows")
    return copied


def cmd_incremental(source: Database, target: Database) -> int:
    ensure_state(target)
    print("catalogs")
    for table in FULL_TABLES:
        sync_whole_table(source, target, table)
    print("records")
    sync_records(source, target)
    ensure_loadtest_user(target)
    print("incremental sync finished")
    return 0


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "incremental", "sync", "sanitize"))
    parser.add_argument(
        "--confirm-overwrite-preprod",
        action="store_true",
        help="Required for sync. Replaces the public schema on the target.",
    )
    args = parser.parse_args()
    source = require_database("source", "SOURCE_DB", "PROD_SYNC_DB")
    target = require_database("target", "TARGET_DB", "PREPROD_DB")
    if same_database(source, target):
        raise SystemExit("Source and target are the same database. Refusing.")
    if args.command == "check":
        return cmd_check(source, target)
    if args.command == "incremental":
        return cmd_incremental(source, target)
    if args.command == "sanitize":
        sanitize(target)
        ensure_loadtest_user(target)
        return 0
    if not args.confirm_overwrite_preprod:
        raise SystemExit("sync replaces preprod. Re-run with --confirm-overwrite-preprod.")
    return cmd_sync(source, target)


if __name__ == "__main__":
    sys.exit(main())
