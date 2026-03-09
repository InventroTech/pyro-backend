"""
Event dispatcher module for handling event processing and rule execution.
This module provides the foundation for rule-based workflows.
"""

from typing import Dict, Any, Optional
import logging
from .models import Record, EventLog
from .rule_engine import execute_rules

logger = logging.getLogger(__name__)


def dispatch_event(event_name: str, record: Record, payload: Dict[str, Any]) -> bool:
    """
    Event dispatcher that triggers rule execution.
    
    This function is called after an event is logged to the EventLog.
    It executes all matching rules for the tenant and event.
    
    Args:
        event_name (str): The name of the event (e.g. 'lead.call_not_connected', 'lead.call_back_later')
        record (Record): The record that triggered the event
        payload (Dict[str, Any]): Additional data associated with the event
        
    Returns:
        bool: True if event was processed successfully, False otherwise
        
    Example:
        dispatch_event("lead.call_not_connected", record, {"user_id": "123"})
    """
    logger.info(f"[DISPATCH] Event '{event_name}' for Record {record.id} (tenant {record.tenant.id})")
    logger.debug(f"[DISPATCH] Payload: {payload}")
    
    try:
        # Execute rules for this event
        execute_rules(event_name, record, payload, str(record.tenant.id))
        
        logger.info(f"[DISPATCH] Successfully processed event '{event_name}' for Record {record.id}")
        return True
        
    except Exception as e:
        logger.error(f"[DISPATCH] Failed to process event '{event_name}' for Record {record.id}: {e}")
        return False


def get_event_history(record: Record, event_name: Optional[str] = None) -> list:
    """
    Get event history for a specific record.
    
    Args:
        record (Record): The record to get events for
        event_name (Optional[str]): Filter by specific event name
        
    Returns:
        list: List of EventLog objects
    """
    events = EventLog.objects.filter(record=record)
    
    if event_name:
        events = events.filter(event=event_name)
    
    return list(events.order_by('-timestamp'))


def get_tenant_events(tenant, event_name: Optional[str] = None, limit: int = 100) -> list:
    """
    Get recent events for a tenant.
    
    Args:
        tenant: The tenant to get events for
        event_name (Optional[str]): Filter by specific event name
        limit (int): Maximum number of events to return
        
    Returns:
        list: List of EventLog objects
    """
    events = EventLog.objects.filter(tenant=tenant)
    
    if event_name:
        events = events.filter(event=event_name)
    
    return list(events.order_by('-timestamp')[:limit])


def simulate_workflow_actions(event_name: str, record: Record, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulate workflow actions that would be triggered by rules.
    Uses event names that exist in the system (lead.*, agent.*).
    
    Args:
        event_name (str): The name of the event
        record (Record): The record that triggered the event
        payload (Dict[str, Any]): Additional data associated with the event
        
    Returns:
        Dict[str, Any]: Simulated action results
    """
    simulated_actions = {
        "lead.call_not_connected": {
            "action": "update_fields",
            "updates": {"lead_stage": "NOT_CONNECTED", "last_call_outcome": "not_connected"},
            "message": "Lead marked as not connected"
        },
        "lead.call_back_later": {
            "action": "update_fields",
            "updates": {"lead_stage": "SNOOZED", "next_call_at": payload.get("next_call_at", "")},
            "message": "Lead scheduled for follow-up call"
        },
        "lead.trial_activated": {
            "action": "update_fields",
            "updates": {"lead_stage": "TRIAL_ACTIVATED"},
            "message": "Lead marked as trial activated"
        },
        "lead.not_interested": {
            "action": "update_fields",
            "updates": {"lead_stage": "NOT_INTERESTED"},
            "message": "Lead marked as not interested"
        },
        "agent.take_break": {
            "action": "update_fields",
            "updates": {"lead_stage": "IN_QUEUE", "assigned_to": None},
            "message": "Lead unassigned (agent take break)"
        },
    }
    
    return simulated_actions.get(event_name, {
        "action": "no_action",
        "message": f"No workflow defined for event: {event_name}"
    })
