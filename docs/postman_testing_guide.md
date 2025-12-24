# Postman Testing Guide for Support Ticket APIs

## Base URL
```
http://localhost:8000/support-ticket/
```

For production/staging, replace with your actual domain.

---

## 1. Dump Ticket to `support_ticket_dump` Table

### Endpoint
```
POST http://localhost:8000/support-ticket/dump-ticket-webhook/
```

### Headers
```
Content-Type: application/json
x-webhook-secret: YOUR_WEBHOOK_SECRET
```

**Note:** The `x-webhook-secret` must match the `WEBHOOK_SECRET` environment variable.

### Request Body
```json
{
  "tenant_id": "e35e7279-d92d-4cdf-8014-98deaab639c0",
  "user_id": "USR123456",
  "name": "John Doe",
  "phone": "+919876543210",
  "source": "WhatsApp",
  "subscription_status": "active",
  "atleast_paid_once": true,
  "reason": "Need help with app installation",
  "badge": "premium",
  "poster": "paid",
  "layout_status": "pending",
  "praja_dashboard_user_link": "https://app.thepyro.ai/users/USR123456",
  "display_pic_url": "https://example.com/profile.jpg",
  "ticket_date": "2025-12-10T14:30:00Z"
}
```

### Required Fields
- `tenant_id` (UUID) - **Required**

### Optional Fields
- `user_id` (string)
- `name` (string)
- `phone` (string)
- `source` (string)
- `subscription_status` (text)
- `atleast_paid_once` (boolean)
- `reason` (text)
- `badge` (string)
- `poster` (string)
- `layout_status` (string)
- `praja_dashboard_user_link` (text)
- `display_pic_url` (text)
- `ticket_date` (datetime) - If not provided, defaults to current time

### Success Response (200)
```json
{
  "message": "Ticket created successfully in dump table",
  "ticket_id": 123
}
```

### Error Response (401)
```json
{
  "error": "Unauthorized: Invalid or missing webhook secret"
}
```

### Error Response (400)
```json
{
  "error": "Missing required field: tenant_id"
}
```

---

## 2. Process Dumped Tickets (Cron Job API)

### Endpoint
```
POST http://localhost:8000/support-ticket/process-dumped-tickets/
```

### Headers
```
Content-Type: application/json
```

**Note:** This endpoint uses `AllowAny` permission, so no authentication is required (suitable for cron jobs).

### Request Body
```json
{}
```

**Note:** No body is required. The endpoint processes all unprocessed tickets from the dump table.

### Success Response (200)
```json
{
  "message": "Tickets processed successfully",
  "total_dumped_tickets": 100,
  "unique_tickets": 85,
  "inserted_tickets": 70,
  "skipped_tickets": 15,
  "marked_processed": 100
}
```

### Response Fields Explained
- `total_dumped_tickets`: Total number of unprocessed tickets found in dump table
- `unique_tickets`: Number of unique tickets after deduplication by `user_id`
- `inserted_tickets`: Number of tickets successfully inserted into `support_ticket` table
- `skipped_tickets`: Number of tickets skipped (due to existing open tickets or missing data)
- `marked_processed`: Number of dump tickets marked as `is_processed = true`

### No Tickets Response (200)
```json
{
  "message": "No tickets to process"
}
```

### Error Response (500)
```json
{
  "error": "Error message here",
  "message": "Failed to process dumped tickets"
}
```

---

## Postman Collection Setup

### Step 1: Create a New Request - Dump Ticket

1. **Method:** POST
2. **URL:** `http://localhost:8000/support-ticket/dump-ticket-webhook/`
3. **Headers:**
   - `Content-Type`: `application/json`
   - `x-webhook-secret`: `YOUR_WEBHOOK_SECRET` (get from environment variables)
4. **Body:** Select "raw" and "JSON", then paste:
```json
{
  "tenant_id": "e35e7279-d92d-4cdf-8014-98deaab639c0",
  "user_id": "USR123456",
  "name": "John Doe",
  "phone": "+919876543210",
  "source": "WhatsApp",
  "subscription_status": "active",
  "atleast_paid_once": true,
  "reason": "Need help with app installation",
  "badge": "premium",
  "poster": "paid",
  "layout_status": "pending",
  "praja_dashboard_user_link": "https://app.thepyro.ai/users/USR123456",
  "display_pic_url": "https://example.com/profile.jpg"
}
```

### Step 2: Create a New Request - Process Dumped Tickets

1. **Method:** POST
2. **URL:** `http://localhost:8000/support-ticket/process-dumped-tickets/`
3. **Headers:**
   - `Content-Type`: `application/json`
4. **Body:** Leave empty or send `{}`

---

## Testing Workflow

### Complete Test Flow:

1. **Dump a ticket:**
   - Call `POST /support-ticket/dump-ticket-webhook/` with ticket data
   - Verify response: `{"message": "Ticket created successfully in dump table", "ticket_id": 123}`

2. **Process dumped tickets:**
   - Call `POST /support-ticket/process-dumped-tickets/`
   - Verify response shows the ticket was processed

3. **Check `support_ticket` table:**
   - Query the database to verify the ticket was inserted
   - Verify `is_processed` is `true` in `support_ticket_dump` table

### Testing Open Ticket Logic:

1. **Create an open ticket manually in `support_ticket` table:**
   - `user_id`: "USR123456"
   - `resolution_status`: `NULL`
   - `assigned_to`: `NULL`

2. **Dump a new ticket with same `user_id`:**
   - Call dump endpoint with `user_id: "USR123456"`

3. **Process dumped tickets:**
   - Call process endpoint
   - Verify in response: `"skipped_tickets": 1` (ticket was skipped because open ticket exists)

---

## Environment Variables Needed

Make sure these are set in your `.env` file:
- `WEBHOOK_SECRET` - Secret key for dump ticket webhook authentication

---

## Example cURL Commands

### Dump Ticket
```bash
curl -X POST http://localhost:8000/support-ticket/dump-ticket-webhook/ \
  -H "Content-Type: application/json" \
  -H "x-webhook-secret: YOUR_WEBHOOK_SECRET" \
  -d '{
    "tenant_id": "e35e7279-d92d-4cdf-8014-98deaab639c0",
    "user_id": "USR123456",
    "name": "John Doe",
    "phone": "+919876543210",
    "source": "WhatsApp",
    "reason": "Need help with app installation"
  }'
```

### Process Dumped Tickets
```bash
curl -X POST http://localhost:8000/support-ticket/process-dumped-tickets/ \
  -H "Content-Type: application/json"
```

---

## Notes

1. **Deduplication:** The process endpoint deduplicates tickets based on `user_id` (keeps first occurrence).

2. **Open Ticket Check:** Tickets are skipped if an open ticket exists for the same `user_id` where:
   - `resolution_status` is `NULL`
   - `assigned_to` is `NULL`

3. **Limit:** The process endpoint processes up to 5000 tickets per run.

4. **Tenant Validation:** Tickets with invalid `tenant_id` are skipped.

5. **Bulk Processing:** All dumped tickets are marked as processed, even if some were skipped during insertion.

