# Pyro product overview (CRM + ERP)

Pyro is a multi-tenant ops platform. The same backend powers:

## CRM
- Leads and dialer queues for RMs
- Support tickets and CSE analytics
- Jobs / applications and flexible JSON records

## ERP (inventory ops)
- Inventory items and inventory requests
- Receiving shipments into stock
- SKU / part_number based lookups

## How this assistant works
- **Help questions** use product documentation (RAG).
- **Live data questions** call tenant-scoped tools over CRM and ERP records.
- Answers never cross tenants; your role and membership still apply at the API layer.

Ask about features ("how do buckets work?") or live data ("inventory stock summary", "count open leads").
