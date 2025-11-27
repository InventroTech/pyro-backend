# Object History Actor Resolution Debugging

## Logging Added

Comprehensive logging has been added to track actor resolution throughout the history tracking system.

## What Gets Logged

### 1. Middleware Level (`object_history.middleware`)

**On every request:**
- `INFO`: Actor resolution summary with `actor_user_id`, `actor_label`, and authentication status
- `DEBUG`: Detailed user information (email, supabase_uid, whether actor_user was found)

**When SupabaseAuthUser is not found:**
- `WARNING`: SupabaseAuthUser not found for a given supabase_uid (with user email)

**On errors:**
- `ERROR`: Exceptions during actor resolution

### 2. Engine Level (`object_history.engine`)

**When writing history:**
- `DEBUG`: Actor resolution details (explicit vs context, what values are being used)
- `INFO`: Final actor information when writing history entry (actor_user_id, actor_label)

**When setting request context:**
- `DEBUG`: Context being set from request (path, actor_user_id, actor_label)

## How to Debug Actor Issues

### Step 1: Check if Middleware is Running

Look for logs like:
```
INFO object_history.middleware process_request - HistoryMiddleware: Resolved actor for /api/path | actor_user_id=... | actor_label=... | user_authenticated=True
```

**If you don't see this log:**
- Middleware might not be running
- Check `MIDDLEWARE` setting in `settings.py`
- Verify middleware is after `AuthenticationMiddleware`

### Step 2: Check User Authentication

Look for:
```
DEBUG object_history.middleware _resolve_actor_user - HistoryMiddleware: request.user details | email=... | supabase_uid=... | actor_user_found=True/False
```

**If `user_authenticated=False`:**
- User is not authenticated
- Check authentication middleware/DRF authentication

**If `supabase_uid=None`:**
- User doesn't have supabase_uid set
- This is a data issue with the User model

### Step 3: Check SupabaseAuthUser Lookup

Look for:
```
INFO object_history.middleware _resolve_actor_user - HistoryMiddleware._resolve_actor_user: Found SupabaseAuthUser | id=... | email=...
```

**OR**

```
WARNING object_history.middleware _resolve_actor_user - HistoryMiddleware._resolve_actor_user: SupabaseAuthUser NOT found | supabase_uid=... | user_email=...
```

**If SupabaseAuthUser NOT found:**
- The user exists in `authentication.User` but not in `SupabaseAuthUser` (unmanaged mirror)
- This is expected for some users - `actor_label` will still be set, but `actor_user` will be None

### Step 4: Check Engine Actor Resolution

Look for:
```
DEBUG object_history.engine capture_after - HistoryEngine.capture_after: Resolving actor | instance=... | context_actor_user=True/False | context_actor_label=...
INFO object_history.engine capture_after - HistoryEngine.capture_after: Writing history | ... | actor_user_id=... | actor_label=...
```

**If `context_actor_user=False`:**
- Middleware didn't set actor_user in context
- Check middleware logs above

**If `actor_user_id=None` in final log:**
- Either no explicit actor_user was passed, or context didn't have one
- Check if `actor_label` is set (fallback)

## Common Issues

### Issue 1: actor_user_id is always None

**Symptoms:**
- Logs show `actor_user_id=None` in final history write
- `WARNING` logs about SupabaseAuthUser not found

**Cause:**
- User exists in `authentication.User` but not in `SupabaseAuthUser` table
- `SupabaseAuthUser` is an unmanaged model mirroring Supabase's `auth.users`

**Solution:**
- Ensure users are synced to `SupabaseAuthUser` table
- Or accept that `actor_user` will be None but `actor_label` will have email/uid

### Issue 2: No middleware logs at all

**Symptoms:**
- No `HistoryMiddleware` logs in output

**Cause:**
- Middleware not registered or not running
- Logging level too high

**Solution:**
- Check `MIDDLEWARE` in `settings.py`
- Verify `object_history` logger level is DEBUG/INFO

### Issue 3: user_authenticated=False

**Symptoms:**
- Logs show `user_authenticated=False`

**Cause:**
- Request not authenticated
- Authentication middleware not running before HistoryMiddleware

**Solution:**
- Ensure `AuthenticationMiddleware` is before `HistoryMiddleware` in MIDDLEWARE list
- Check DRF authentication is working

## Testing with API Requests

When making API requests with authentication:

1. **Enable DEBUG logging** (already set for `object_history` logger in dev)
2. **Make an authenticated API request** that updates a tracked model
3. **Check logs** for the flow:
   - Middleware resolves actor
   - Engine receives context
   - History is written with actor information

Example log flow for successful resolution:
```
INFO object_history.middleware process_request - HistoryMiddleware: Resolved actor for /api/support-tickets/123/ | actor_user_id=xxx | actor_label=user@example.com | user_authenticated=True
DEBUG object_history.middleware _resolve_actor_user - HistoryMiddleware._resolve_actor_user: Found SupabaseAuthUser | id=xxx | email=user@example.com
DEBUG object_history.engine set_request_context - HistoryEngine.set_request_context: Setting context from request | path=/api/support-tickets/123/ | actor_user_id=xxx | actor_label=user@example.com
DEBUG object_history.engine capture_after - HistoryEngine.capture_after: Resolving actor | instance=SupportTicket#123 | context_actor_user=True | context_actor_label=user@example.com
INFO object_history.engine capture_after - HistoryEngine.capture_after: Writing history | instance=SupportTicket#123 | action=updated | actor_user_id=xxx | actor_label=user@example.com
```

## Log Levels

- **DEBUG**: Detailed flow information (actor resolution steps)
- **INFO**: Important events (actor resolved, history written)
- **WARNING**: Non-critical issues (SupabaseAuthUser not found)
- **ERROR**: Exceptions and errors

## Viewing Logs

In development, logs go to console. In production, configure appropriate handlers.

To see only object_history logs:
```bash
python manage.py runserver 2>&1 | grep object_history
```

Or filter by level:
```bash
python manage.py runserver 2>&1 | grep -E "(INFO|WARNING|ERROR).*object_history"
```


