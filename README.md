# pyro-backend

## Testing

Tests use **pytest** with **pytest-django**. 


2. Run the full test suite **from the `pyro` directory** (so `pytest.ini` is found):

   ```bash
   pytest
   ```

   Do **not** run pytest from `pyro/src`; run from `pyro`.

### Useful commands

- **Run all tests:** `pytest`
- **Run with coverage:** `pytest --cov=src --cov-report=term-missing`
- **Run a directory:** `pytest src/tests/rest/crm_records/`
- **Run by keyword:** `pytest -k "record"`
- **Verbose:** `pytest -v` (default in `pytest.ini`)

### Configuration

- **`pytest.ini`** – Pytest config: `pythonpath=src`, `testpaths=src/tests`. Conftest sets `DJANGO_SETTINGS_MODULE=config.settings_test`.
- **`src/config/settings_test.py`** – Test Django settings. By default uses 
- **`src/tests/conftest.py`** – Pytest hooks and fixtures (e.g. tenants table for SQLite)


