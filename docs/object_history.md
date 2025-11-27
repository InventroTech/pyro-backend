# Object History

add a mixin + a registry entry and you get actor-aware, versioned history for any model automatically. 

---

## Why
- Always know *who* changed *what*, *when*, and from *where*.
- Zero boilerplate in views/serializers/admin; history hooks into the ORM and middleware.
- One table (`object_history`) serves every tracked model and includes diffs + before/after snapshots.

## How It Works
| Piece | Responsibility |
| --- | --- |
| **HistoryMiddleware** | Runs after `AuthenticationMiddleware`, resolves actor (`SupabaseAuthUser`) + request metadata, stores them on the request/thread. Scripts can use `set_manual_context()`. |
| **HistoryTrackedModel** | Abstract mixin with custom `save()`/`delete()` that call `HistoryEngine.capture_before/after`. Any model inheriting from it automatically emits history. |
| **HistoryEngine** | Reads registry config, serializes before/after, computes diffs, applies redaction, writes `ObjectHistory`. Handles both request + non-request contexts. |
| **History Registry** | Declarative config per model (fields to track/redact, snapshot strategy, optional custom serializer). Only registered models incur overhead. |
| **ObjectHistory Model** | Stores entries with: actor info, action, version, diff, snapshots (`before_state`/`after_state`), metadata (request_id, IP, endpoint, source, etc.). Query helpers allow `ObjectHistory.objects.for_instance(instance)`. |

---

## Quickstart – Tracking a New Model
1. **Inherit the mixin**
   ```python
   from object_history.models import HistoryTrackedModel

   class MyModel(HistoryTrackedModel, BaseModel):
       ...
   ```

2. **Register it** (`object_history/registrations.py`)
   ```python
   from object_history.registry import register
   from my_app.models import MyModel

   register(
       MyModel,
       track_fields=["status", "owner_id", "payload"],
       redact_fields={"payload"},  # optional
       snapshot_strategy="minimal",  # or "full" / "custom"
   )
   ```
   - `track_fields`: whitelist; only these show up in diffs/snapshots.
   - `redact_fields`: replace values with `[REDACTED]` in snapshots & diff.
   - `snapshot_strategy`:
     - `"minimal"` – default; only changed fields captured.
     - `"full"` – store every tracked field in both snapshots.
     - `"custom"` – provide `custom_serializer(instance)` to shape the payload (e.g., flatten JSON).

3. **Deploy**: No view/serializer changes needed. History entries will start flowing to `object_history` as soon as instances are saved/deleted.

---

## Using the Data
- ORM helper: `ObjectHistory.objects.for_instance(my_model_instance)` returns all entries ordered by version.
- Each entry includes:
  - `actor_user_id` (Supabase UUID when available) + `actor_label` fallback (email/service name).
  - `version` (per object), `action` (`created`, `updated`, `deleted`).
  - `changes` JSON diff, and `before_state` / `after_state` snapshots (respecting snapshot strategy).
  - `metadata` (request_id, IP, user_agent, endpoint, `source` like `"admin"` / `"cron"` / `"api"`).

---

## Scripts / Cron Jobs
If there is no incoming request, wrap your code with manual context:
```python
from object_history.engine import set_manual_context, clear_request_context

set_manual_context(actor_label="cron:scheduler", metadata={"source": "cron"})
try:
    ... mutate tracked models ...
finally:
    clear_request_context()
```
You can also pass `HistoryEngine.capture_after(..., actor="my-script")` if you need per-call overrides.

---

## Debugging Tips
- Logging: set `object_history` logger to DEBUG (already done in dev). You’ll see actor-resolution traces and history writes in the console.
- `python manage.py test_history --tenant-id=<uuid>`: sanity check command that updates a `SupportTicket` and prints the resulting history (+ verifies redaction).
- If `actor_user_id` is `None`, confirm the user exists in `accounts.SupabaseAuthUser` (unmanaged mirror). The system still records `actor_label` as a fallback.

---

## Current Coverage
- `support_ticket.SupportTicket` – tracks resolution/call/CSE fields with `cse_remarks` redacted.
- `crm_records.Record` – tracks entity info + JSON payload (minimal snapshot). You can swap to a custom serializer if the JSON is huge.

Adding more models follows the same two-step recipe above. Keep `track_fields` tight (only what you care to diff) and redact anything sensitive. Continuous history for every save/delete comes for free once registered. 
