SUPPORT_TICKET_ENTITY_TYPE = "support_ticket"

# CRM / EventLog event names for support ticket button actions.
SUPPORT_EVENT_NOT_CONNECTED = "support.not_connected"
SUPPORT_EVENT_CALL_LATER = "support.call_later"
SUPPORT_EVENT_RESOLVED = "support.resolved"
SUPPORT_EVENT_CANNOT_RESOLVE = "support.cannot_resolve"
SUPPORT_EVENT_TAKE_BREAK = "support.take_break"

SUPPORT_TICKET_BUTTON_EVENTS = frozenset({
    SUPPORT_EVENT_NOT_CONNECTED,
    SUPPORT_EVENT_CALL_LATER,
    SUPPORT_EVENT_RESOLVED,
    SUPPORT_EVENT_CANNOT_RESOLVE,
    SUPPORT_EVENT_TAKE_BREAK,
})

SAVE_AND_CONTINUE_RESOLUTION_EVENTS = {
    "Resolved": SUPPORT_EVENT_RESOLVED,
    "Can't Resolve": SUPPORT_EVENT_CANNOT_RESOLVE,
    "WIP": SUPPORT_EVENT_CALL_LATER,
}

# Stored on ticket/record when Praja dumps a new open support ticket.
SUPPORT_RESOLUTION_STATUS_OPEN = "Open"

PRAJA_SAVE_RESOLVED_TICKET_URL = "https://api.thecircleapp.in/pyro/save_resolved_ticket"

# ``resolution_status`` values that POST to Praja ``save_resolved_ticket`` after rules run.
# ``Open`` is synced separately when a dumped ticket is created.
SUPPORT_TICKET_PRAJA_SYNC_RESOLUTION_STATUSES = frozenset({
    "Resolved",
    "Can't Resolve",
    "Closed",
})

# Button events → resolution written by rules (fallback if object_history has no diff).
SUPPORT_EVENT_TO_PRAJA_RESOLUTION_STATUS = {
    SUPPORT_EVENT_RESOLVED: "Resolved",
    SUPPORT_EVENT_CANNOT_RESOLVE: "Can't Resolve",
}
