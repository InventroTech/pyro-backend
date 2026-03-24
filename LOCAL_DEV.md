# Local development

## Running the server

- **`manage.py` lives in `src/`.** From the repo root:
  ```bash
  cd pyro-backend/src
  python3 manage.py runserver
  ```
  Or from `pyro-backend`: `cd src && python3 manage.py runserver`.

## Database: "pyro_test does not exist"

When `LOCAL_TEST_MODE=True` in `.env`, Django uses your **local** PostgreSQL and the database name from `LOCAL_DB_NAME` (default: `pyro_test`). If you see:

```text
FATAL: database "pyro_test" does not exist
```

do one of the following.

### Option A: Create the local database (recommended for offline dev)

1. Start PostgreSQL (e.g. `brew services start postgresql` on macOS).
2. Create the database (use the same user/password as in `.env`):
   ```bash
   PGPASSWORD=123456 psql -h localhost -U postgres -p 5432 -c "CREATE DATABASE pyro_test;"
   ```
   If your local Postgres uses a different user or no password, adjust:
   ```bash
   createdb -h localhost -U postgres pyro_test
   ```
3. Run migrations:
   ```bash
  cd pyro-backend/src
  python3 manage.py migrate
   ```
4. Start the server again: `python3 manage.py runserver`.

### Option B: Use the remote (Supabase) database

In `.env`, set:

```env
LOCAL_TEST_MODE=False
```

Then Django will use `DB_NAME`, `DB_HOST`, etc. (Supabase). No local Postgres or `pyro_test` needed.
