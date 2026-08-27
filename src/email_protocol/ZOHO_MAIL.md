"""
Zoho Mail → shipment tracking auto-fill
=======================================

One-time OAuth connect for a tenant's ops Zoho inbox. A background job then
polls for shipment emails and fills empty ``tracking_number`` / ``tracking_link``
/ ``courier_name`` / ``eta`` on matching ``inventory_request`` / ``unmannd_request``
records.

Setup (Zoho API Console)
------------------------
1. Create a **Server-based Application** at https://api-console.zoho.com/
   (separate from the Zoho login app).
2. Authorized Redirect URI must match ``ZOHO_OAUTH_REDIRECT_URI``
   (example: ``https://your-api.example.com/email/zoho/callback/``)
3. Set env:

```
ZOHO_CLIENT_ID=...
ZOHO_CLIENT_SECRET=...
ZOHO_OAUTH_REDIRECT_URI=https://your-api.example.com/email/zoho/callback/
ZOHO_ACCOUNTS_BASE_URL=https://accounts.zoho.com   # or accounts.zoho.in
ZOHO_OAUTH_SUCCESS_REDIRECT=https://your-app.example.com/settings/integrations
```

API
---
- ``GET  /email/zoho/connect/``   → ``{ authorize_url }`` (tenant JWT)
- ``GET  /email/zoho/callback/``  → Zoho redirect (stores refresh token)
- ``GET  /email/zoho/status/``    → connection status
- ``POST /email/zoho/disconnect/``
- ``POST /email/zoho/sync-now/``  → run one sync immediately

Frontend
--------
Connect UI sits on the tenant **Settings** page User Management block
(page-builder ``AddUser`` component). OAuth returns via ``/settings/integrations``
and redirects back to that page.

Matching
--------
1. **Gate:** only emails whose **From** is a known delivery / logistics partner
   (Blue Dart, Delhivery, DTDC, FedEx, DHL, Shiprocket, Amazon shipping, etc.)
2. **Item:** match the email subject/body against open requests' item name
   (``item_name_freeform`` / ``item_name`` / ``part_number_or_sku`` /
   ``product_name``). Longest unique substring wins; ambiguous item names are skipped.

Only empty tracking fields are filled (never overwrites existing values).
"""
