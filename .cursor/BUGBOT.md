# pyro-backend review rules

Django 5.2 + DRF multi-tenant API. App code lives under `src/`. Base branch is usually `main` (also `TESTING` / `PROD` for CI).

## Must flag

- **Tenant leaks:** queries or writes that omit `request.tenant` / tenant scoping on tenant APIs.
- **Auth gaps:** new endpoints missing `IsAuthenticated` / `IsTenantAuthenticated` / `HasPermissionKey` / `HasTenantRole` when peers use them.
- **Soft-delete footguns:** using `all_objects` or hard deletes instead of default managers / `soft_delete_cascade` / `.hard_delete()` only when intentional.
- **Secrets:** logging or returning JWT secrets, `PYRO_SECRET`, `ApiSecretKey` values, or `.env` contents.
- **Unmanaged models:** treating `core.Tenant`, `accounts.SupabaseAuthUser`, or other unmanaged Supabase mirrors as Django-managed (migrations that alter them).
- **Raw SQL:** `objects.raw(`, `connection.cursor(`, `.execute(`, `RawSQL(` without a clear justification (prefer ORM).

## Conventions

- Tenant models inherit `core.BaseModel` / `RoleBaseModel`; uniqueness uses `alive_q()` partial constraints.
- History-tracked models use `HistoryTrackedModel` and stay registered in `object_history`.
- Async/heavy work goes through `background_jobs` (tenant-scoped), not one-off threads.
- Prefer pagination via `MetaPageNumberPagination` for list APIs.

## Tests

- New API behavior needs tests under `src/tests/` (pytest + pytest-django from repo root).
- Prefer existing fixtures (`tenant`, `user`, `auth_headers`, cross-tenant `tenant_b` / `user_b`) and assert cross-tenant isolation when relevant.

## Out of scope noise

Do not nitpick Flake8 style beyond what CI blocks (`E9`, `F63`, `F7`, `F82`). Focus on correctness, tenancy, auth, data integrity, and regressions.
