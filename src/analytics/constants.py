"""
Event type constants for analytics tracking.
"""

# All tracked events
TRACKED_EVENTS = [
    'lead.get_next_lead',       # GET_NEXT_LEAD (for attendance)
    'lead.trial_activated',      # TRIAL_ACTIVATED
    'lead.call_not_connected',  # CALL_NOT_CONNECTED
    'lead.call_back_later',     # CALL_BACK_LATER
    'agent.take_break',         # TAKE_BREAK
    'lead.not_interested',      # NOT_INTERESTED
]

# Terminal events (always complete a lead)
TERMINAL_EVENTS = [
    'lead.trial_activated',      # Always terminal
    'lead.not_interested',       # Always terminal
    'lead.call_back_later',      # Always terminal
]

# Note: lead.call_not_connected is only terminal when it closes the lead
# (when call_attempts >= 4, sets lead_stage to "CLOSED")
# For average time calculation, we need to check if call_not_connected actually closed the lead

EVENT_DISPLAY_NAMES = {
    'lead.get_next_lead': 'Get Next Lead',
    'lead.trial_activated': 'Trial Activated',
    'lead.call_not_connected': 'Call Not Connected',
    'lead.call_back_later': 'Call Back Later',
    'agent.take_break': 'Take Break',
    'lead.not_interested': 'Not Interested',
}

