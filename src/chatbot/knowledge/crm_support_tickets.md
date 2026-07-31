# Support tickets

Pyro stores support tickets as CRM records with `entity_type = support_ticket`.

## Key concepts
- Ticket fields live in the JSON `data` column (assignee/CSE, resolution_status, resolution_time, SLA timestamps).
- Analytics boards and NL→SQL analytics answer questions like average resolution time and CSE performance.
- CSE metrics cover overview, members, and time-series views.

## Common statuses
- Use `resolution_status` (e.g. Resolved vs unresolved) when filtering tickets.
- Resolution time may be stored as `MM:SS` text on the ticket.

## Asking the assistant
- How-to: "How does ticket SLA work?"
- Live data: "How many unresolved support tickets are there?" or "Break down tickets by resolution_status"
