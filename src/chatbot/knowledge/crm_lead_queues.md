# Lead queues and buckets

Pyro CRM uses **buckets** and **queues** to distribute leads to Relationship Managers (RMs).

## Buckets
- A bucket is a filtered set of leads (by stage, source, party, or other JSON filters).
- Users are assigned to buckets via UserBucketAssignment.
- Buckets can be scoped by entity_type (e.g. lead vs support).

## Queues
- RMs pull the **next** lead from their assigned buckets according to daily limits and scoring.
- Call attempt matrices define max attempts, SLA days, and minimum time between calls.
- Lead status values commonly include stages such as NEW, CONNECTED, NOT_CONNECTED, and closed states.

## Scoring
- Scoring rules weight lead attributes so higher-priority leads surface first in the queue.
- Attribute paths come from the tenant entity type schema (e.g. data.lead_source).

## Asking the assistant
- How-to: "How do lead buckets work?"
- Live data: "How many open leads do we have?"
