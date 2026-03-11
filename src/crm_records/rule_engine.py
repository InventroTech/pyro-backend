"""
Rule Engine Service Module

This module provides the core rule evaluation and execution logic for the dynamic CRM platform.
It handles fetching rules, evaluating conditions using JSONLogic, and executing actions.
"""
import time
import logging
import json
from typing import Dict, Any, List, Optional
from django.utils import timezone
from datetime import timedelta
import re
import copy
from django.core.cache import cache

from django.db.models import Q

from .models import RuleSet, RuleExecutionLog, Record
from background_jobs.queue_service import get_queue_service
from background_jobs.models import JobType
from object_history.engine import get_request_context

logger = logging.getLogger(__name__)

# Global action registry
ACTIONS = {}


def register_action(name: str):
    """
    Decorator to register action functions.
    
    Usage:
        @register_action("update_fields")
        def action_update_fields(ctx, updates):
            # implementation
    """
    def wrapper(fn):
        ACTIONS[name] = fn
        return fn
    return wrapper


# ----------------------
# Template resolution
# ----------------------
_TEMPLATE_RE = re.compile(r"\{\{\s*([^}]+)\s*\}\}")


def _get_ctx_path(ctx: Dict[str, Any], path: str) -> Any:
    """Resolve a dotted path against the context dict (supports dicts and objects)."""
    parts = path.split(".")
    value: Any = ctx
    for part in parts:
        if isinstance(value, dict):
            value = value.get(part)
        else:
            # Fallback to attribute access (e.g., record.id)
            value = getattr(value, part, None)
        if value is None:
            break
    return value


def _resolve_token(token: str, ctx: Dict[str, Any]) -> Any:
    token = token.strip()
    if token == "now":
        return timezone.now().isoformat()
    # Allow direct ctx keys (record, payload, event, record_data) and dotted paths
    if "." in token:
        return _get_ctx_path(ctx, token)
    return ctx.get(token)


def _resolve_string_templates(s: str, ctx: Dict[str, Any]) -> Any:
    """
    Resolve template expressions within a string. If the entire string is a single
    template like "{{payload.x}}", return the resolved value preserving type.
    Otherwise, perform string replacement for any embedded templates.
    """
    match = _TEMPLATE_RE.fullmatch(s)
    if match:
        return _resolve_token(match.group(1), ctx)

    def repl(m: re.Match) -> str:
        val = _resolve_token(m.group(1), ctx)
        return "" if val is None else str(val)

    return _TEMPLATE_RE.sub(repl, s)


def _resolve_templates_in(value: Any, ctx: Dict[str, Any]) -> Any:
    """Recursively resolve templates in dicts/lists/strings."""
    if isinstance(value, dict):
        return {k: _resolve_templates_in(v, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_templates_in(v, ctx) for v in value]
    if isinstance(value, str):
        return _resolve_string_templates(value, ctx)
    return value


    


def _evaluate_condition(condition: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
    """Evaluate a rule condition using template resolution + simple evaluator only."""
    if not condition:
        return True

    # Resolve templates (e.g., {{now}}, {{payload.x}}) inside the condition
    resolved_condition = _resolve_templates_in(condition, ctx)

    return _evaluate_simple_condition(resolved_condition, ctx)


@register_action("update_fields")
def action_update_fields(
    ctx: Dict[str, Any],
    updates: Dict[str, Any],
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Action to update record fields.
    
    Args:
        ctx: Context containing 'record' and 'payload'
        updates: Dictionary of field updates to apply
        
    Returns:
        Dictionary with execution result
    """
    record = ctx["record"]
    payload = ctx.get("payload", {})
    
    # Store original payload for later checks (before we modify it)
    original_payload = payload.copy() if isinstance(payload, dict) else payload

    # Check if this is a call back later event BEFORE template resolution
    event_name = ctx.get("event", "")
    button_type = payload.get("button_type", "")
    call_status = payload.get("call_status", "")
    last_call_outcome = payload.get("last_call_outcome", "")
    
    is_call_back_later_event = (
        event_name == "call_back_later" or 
        event_name.endswith(".call_back_later") or
        "call_back_later" in event_name.lower() or
        button_type == "call_later" or
        "call_later" in event_name.lower()
    )
    
    # Check if this is a "not connected" event
    is_not_connected_event = (
        "not_connected" in event_name.lower() or
        button_type.lower() in {"not_connected", "not connected", "not-connected"} or
        call_status.lower() in {"not connected", "not_connected", "notconnected"} or
        last_call_outcome.lower() in {"not connected", "not_connected", "notconnected"} or
        (updates and updates.get("lead_stage", "").upper() == "NOT_CONNECTED")
    )

    # Resolve any templates in updates based on current context
    resolved_updates = _resolve_templates_in(updates or {}, ctx)

    # Simple: if rule sends assigned_to_user_id, also set assigned_to so record.data is consistent
    if "assigned_to_user_id" in resolved_updates and "assigned_to" not in resolved_updates:
        resolved_updates["assigned_to"] = resolved_updates["assigned_to_user_id"]

    # For call_back_later: apply lead_stage from payload if present (e.g. SNOOZED)
    if is_call_back_later_event and "lead_stage" in original_payload:
        lead_stage_value = original_payload.get("lead_stage")
        if isinstance(lead_stage_value, str):
            lead_stage_value = lead_stage_value.strip().upper()
        resolved_updates["lead_stage"] = lead_stage_value

    logger.info(
        f"Before applying updates for record {record.id}: "
        f"resolved_updates_keys={list(resolved_updates.keys())}, "
        f"assigned_to={resolved_updates.get('assigned_to')}"
    )
    
    # Track first_assigned_to and first_assigned_at when assigned_to is being set
    # This ensures daily limit tracking works for all assignment methods (not just GetNextLeadView)
    # Check BEFORE applying updates to see if this is a fresh assignment
    # EXCEPTION: Don't set first_assigned_to for "not connected" retry leads (they shouldn't count toward new RM's limit)
    if "assigned_to" in resolved_updates and record.entity_type == "lead":
        new_assigned_to = resolved_updates.get("assigned_to")
        current_assigned_to = record.data.get("assigned_to")
        
        # Check if this is a "not connected" retry lead
        # (has call_attempts > 0, or last_call_outcome = 'not_connected', or lead_stage was 'IN_QUEUE')
        call_attempts = record.data.get("call_attempts", 0)
        try:
            call_attempts_int = int(call_attempts) if call_attempts is not None else 0
        except (TypeError, ValueError):
            call_attempts_int = 0
        
        last_call_outcome = record.data.get("last_call_outcome", "").lower()
        lead_stage = record.data.get("lead_stage", "").upper()
        # Check if this is a retry lead (NOT_CONNECTED only)
        # These leads should NOT set first_assigned_to when reassigned to a new RM
        # last_call_outcome in DB is exactly "not_connected"
        is_not_connected_retry = (
            call_attempts_int > 0 or
            last_call_outcome == "not_connected" or
            lead_stage == "NOT_CONNECTED"
        )
        
        # Check if this is a fresh assignment (was unassigned, now being assigned)
        is_fresh_assignment = (
            (current_assigned_to is None or current_assigned_to == '' or current_assigned_to == 'null' or current_assigned_to == 'None')
            and (new_assigned_to is not None and new_assigned_to != '' and new_assigned_to != 'null')
        )
        
        # Set first_assigned_to and first_assigned_at if this is a fresh assignment
        # BUT skip for retry leads (not connected, call back later)
        # These leads shouldn't count toward new RM's daily limit when reassigned
        if is_fresh_assignment and 'first_assigned_at' not in record.data and not is_not_connected_retry:
            record.data['first_assigned_at'] = timezone.now().isoformat()
            record.data['first_assigned_to'] = new_assigned_to
            logger.info(
                f"[action_update_fields] Set first_assigned_to={new_assigned_to} and first_assigned_at for lead_id={record.id} "
                f"(fresh assignment via rule engine)"
            )
        elif is_fresh_assignment and is_not_connected_retry:
            logger.info(
                f"[action_update_fields] Skipping first_assigned_to for lead_id={record.id} "
                f"(retry lead with previous attempts/outcome - call_attempts={call_attempts_int}, "
                f"last_call_outcome={last_call_outcome}, lead_stage={lead_stage}, won't count toward new RM's daily limit)"
            )

    # Call-back-later (sales lead only): set snooze_unassign_at so background job can unassign after 48h (if time selected) or 12h (if not).
    # Self-trial rule does not set assigned_to (stays unassigned); only the sales-lead rule sets assigned_to, so we only set snooze_unassign_at when the rule is actually assigning (truthy assigned_to), not when clearing it.
    assigned_to_value = resolved_updates.get("assigned_to") if "assigned_to" in resolved_updates else None
    if is_call_back_later_event and assigned_to_value and str(assigned_to_value).strip() not in ("", "null", "None"):
        has_next_call_time = bool(original_payload.get("next_call_at"))
        unassign_hours = 48 if has_next_call_time else 12
        resolved_updates["snooze_unassign_at"] = (timezone.now() + timedelta(hours=unassign_hours)).isoformat()
        logger.info(
            f"[action_update_fields] Set snooze_unassign_at for lead_id={record.id} "
            f"(call_back_later, has_time={has_next_call_time}, unassign in {unassign_hours}h)"
        )

    # Not-connected: set not_connected_unassign_at so background job can unassign after 12h (same pattern as snooze_unassign_at).
    # Only when the rule sets lead_stage to NOT_CONNECTED and the lead has or keeps assigned_to.
    lead_stage_after = (resolved_updates.get("lead_stage") or record.data.get("lead_stage") or "").strip().upper()
    assigned_after = resolved_updates.get("assigned_to") if "assigned_to" in resolved_updates else record.data.get("assigned_to")
    assigned_ok = assigned_after and str(assigned_after).strip() not in ("", "null", "None")
    if is_not_connected_event and lead_stage_after == "NOT_CONNECTED" and assigned_ok:
        resolved_updates["not_connected_unassign_at"] = (timezone.now() + timedelta(hours=12)).isoformat()
        logger.info(
            f"[action_update_fields] Set not_connected_unassign_at for lead_id={record.id} (not_connected, unassign in 12h)"
        )

    # Apply all updates
    for key, value in resolved_updates.items():
        record.data[key] = value
    
    logger.info(
        f"After applying updates for record {record.id}: "
        f"record.data['assigned_to']={record.data.get('assigned_to')}"
    )

    # Support numeric increments via optional args: "increments" or "$inc" or "inc"
    increments: Optional[Dict[str, Any]] = (
        kwargs.get("increments")
    )

    applied_increments: Dict[str, Any] = {}
    if increments:
        # Resolve templates inside increment values too
        resolved_increments = _resolve_templates_in(increments, ctx)
        for key, delta in resolved_increments.items():
            current_value = record.data.get(key, 0)
            try:
                new_value = (current_value or 0) + float(delta)
                # Cast back to int when appropriate to avoid 1.0 style values
                if isinstance(current_value, int) and float(delta).is_integer():
                    new_value = int(new_value)
                record.data[key] = new_value
                applied_increments[key] = new_value
            except Exception:
                # If increment fails (non-numeric), leave as-is and log warning
                logger.warning(
                    f"Increment skipped for field '{key}' on record {record.id}: current='{current_value}' delta='{delta}'"
                )

    # DORMANT (max attempts reached) is handled by rule sets; no hardcoded logic here.

    # Save only the data and updated_at fields for efficiency
    record.save(update_fields=["data", "updated_at"])

    logger.info(
        f"Updated record {record.id} fields: {resolved_updates}; increments applied: {applied_increments}"
    )
    return {"updated_fields": resolved_updates, "increments": applied_increments}


@register_action("send_webhook")
def action_send_webhook(ctx: Dict[str, Any], url: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Action to send HTTP webhook.
    
    Args:
        ctx: Context containing 'record' and 'payload'
        url: Webhook URL to send POST request to
        payload: Optional payload to send (defaults to record data)
        
    Returns:
        Dictionary with execution result
    """
    import requests
    
    record = ctx["record"]
    webhook_payload = payload or record.data
    webhook_payload = _resolve_templates_in(webhook_payload, ctx)
    
    try:
        response = requests.post(url, json=webhook_payload, timeout=5)
        response.raise_for_status()
        
        logger.info(f"Webhook sent to {url} for record {record.id}: {response.status_code}")
        return {"status_code": response.status_code, "url": url}
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Webhook failed for record {record.id} to {url}: {e}")
        raise


@register_action("send_mixpanel_event")
def action_send_mixpanel_event(
    ctx: Dict[str, Any],
    user_id: Any,
    event_name: str,
    properties: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Action to send an event to Mixpanel via the custom API used by MixpanelService.
    Supports template resolution on all arguments.

    Automatically includes all data properties from the record.
    Enqueues the event to be processed asynchronously by a background worker.

    Args:
        ctx: Rule context containing 'record', 'payload', etc.
        user_id: The Mixpanel distinct_id (will be cast to int in the service). Can be templated.
        event_name: The event name to send. Can be templated.
        properties: Dict of event properties. Can be templated. Will be merged with all record data.

    Returns:
        Dict with execution result including job_id.
    """
    record = ctx["record"]
    tenant_id = ctx.get("tenant_id") or (record.tenant_id if hasattr(record, 'tenant_id') else None)

    # Resolve templates for all arguments
    resolved_user_id = _resolve_templates_in(user_id, ctx)
    resolved_event_name = _resolve_templates_in(event_name, ctx)
    resolved_properties = _resolve_templates_in(properties or {}, ctx)
    
    # For lead events, automatically use praja_id as user_id if available
    # This ensures lead events use praja's user_id instead of pyro's record_id
    if record.entity_type == "lead":
        record_data = ctx.get("record_data") or (record.data if record.data else {})
        praja_id = record_data.get("praja_id") if isinstance(record_data, dict) else None
        
        if praja_id:
            # Convert praja_id to appropriate format for Mixpanel
            # Handle different formats: integer, numeric string, or string like "PRAJA123"
            original_resolved = resolved_user_id
            try:
                if isinstance(praja_id, int):
                    # Already an integer, use directly
                    resolved_user_id = praja_id
                elif isinstance(praja_id, str):
                    # Try to extract numeric part from strings like "PRAJA123", "PRAJA-123", or just "123"
                    # Remove common prefixes and separators
                    cleaned = praja_id.upper().replace("PRAJA", "").replace("-", "").replace("_", "").strip()
                    if cleaned.isdigit():
                        # Convert to integer if it's all digits
                        resolved_user_id = int(cleaned)
                    else:
                        # If not all digits, use as-is (might be a different format)
                        resolved_user_id = praja_id
                else:
                    # Other types (float, etc.), convert to string
                    resolved_user_id = str(praja_id)
            except (ValueError, TypeError, AttributeError) as e:
                # Fallback to original value if conversion fails
                logger.warning(
                    f"[send_mixpanel_event] Failed to convert praja_id={praja_id} to integer: {e}, "
                    f"using as-is for record {record.id}"
                )
                resolved_user_id = praja_id
            
            logger.info(
                f"[send_mixpanel_event] Lead event detected - using praja_id={resolved_user_id} "
                f"(original user_id={original_resolved}) for record {record.id}"
            )
        else:
            logger.warning(
                f"[send_mixpanel_event] Lead event detected but no praja_id found in record_data "
                f"for record {record.id}, using resolved user_id={resolved_user_id}"
            )

    # Build complete properties dict with all record data
    mixpanel_properties = {
        'record_id': record.id,
        'entity_type': record.entity_type,
        'name': (record.data or {}).get('name', '') if isinstance(record.data, dict) else '',
        'tenant_id': str(record.tenant.id),
        'tenant_slug': record.tenant.slug,
        'event_name': resolved_event_name,
    }
    
    # Add all properties from record.data field
    if record.data:
        mixpanel_properties.update(record.data)
    
    # Add event payload properties (may override data properties if same key)
    event_payload = ctx.get('payload', {})
    if event_payload:
        mixpanel_properties.update(event_payload)
    
    # Add timestamps
    if record.created_at:
        mixpanel_properties['record_created_at'] = record.created_at.isoformat()
    if record.updated_at:
        mixpanel_properties['record_updated_at'] = record.updated_at.isoformat()
    
    # Merge with resolved_properties (user-provided properties take precedence)
    mixpanel_properties.update(resolved_properties)
    
    # Add rm_email field using the actor_label from the request context
    # actor_label contains the email of the user who triggered this event
    request_context = get_request_context()
    actor_label = request_context.get('actor_label')
    
    if actor_label:
        # actor_label is the email address of the user who performed the action
        mixpanel_properties['rm_email'] = actor_label
        logger.info(f"[rm_email] Added rm_email={actor_label} to Mixpanel event for record {record.id}")
    else:
        logger.info(f"[rm_email] No actor_label found in request context for record {record.id} (actor_user={request_context.get('actor_user')})")

    try:
        # Enqueue job for async processing - send ALL data (complete mixpanel_properties)
        queue_service = get_queue_service()
        job = queue_service.enqueue_job(
            job_type=JobType.SEND_MIXPANEL_EVENT,
            payload={
                "user_id": str(resolved_user_id),
                "event_name": str(resolved_event_name),
                "properties": mixpanel_properties,  # Send ALL data, not just resolved_properties
            },
            tenant_id=tenant_id,
        )
        
        logger.info(
            f"Mixpanel event queued for record {record.id}: job_id={job.id}, "
            f"event='{resolved_event_name}' properties_count={len(mixpanel_properties)}"
        )
        
        return {
            "success": True,
            "job_id": job.id,
            "event_name": resolved_event_name,
            "user_id": resolved_user_id,
            "queued": True
        }
    except Exception as e:
        logger.error(
            f"Failed to queue Mixpanel event for record {record.id}: "
            f"event='{resolved_event_name}' error={e}"
        )
        raise

@register_action("compute_next_call_from_attempts")
def action_compute_next_call_from_attempts(
    ctx: Dict[str, Any],
    base_minutes_per_attempt: int = 30,
    attempts_field: str = "call_attempts",
    target_field: str = "next_call_at",
    fixed_minutes: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Compute next_call_at = now + (base_minutes_per_attempt * current_attempts) minutes
    using attempts from record.data[attempts_field], then write to record.data[target_field].
    
    If fixed_minutes is provided, use that instead of multiplying by attempts
    (useful for "not connected" leads that need a fixed 1-hour snooze).
    
    Args:
        base_minutes_per_attempt: Minutes to multiply by attempt count (default: 30)
        attempts_field: Field name containing attempt count (default: "call_attempts")
        target_field: Field name to write result to (default: "next_call_at")
        fixed_minutes: If provided, use this fixed value instead of multiplying (optional)
    """
    record = ctx["record"]
    
    attempts_raw = record.data.get(attempts_field, 0)
    try:
        attempts = int(attempts_raw or 0)
    except Exception:
        attempts = 0

    # If fixed_minutes is provided, use that (for "not connected" with fixed 1-hour snooze)
    # Otherwise, multiply base_minutes_per_attempt by attempts
    if fixed_minutes is not None:
        minutes = fixed_minutes
        logger.info(
            f"Using fixed_minutes={fixed_minutes} for {target_field} computation (not multiplying by attempts)"
        )
    else:
        minutes = base_minutes_per_attempt * attempts
    
    next_time = timezone.now() + timedelta(minutes=minutes)
    iso_ts = next_time.isoformat()

    record.data[target_field] = iso_ts
    record.save(update_fields=["data", "updated_at"])
    logger.info(
        f"Computed {target_field} for record {record.id} using attempts={attempts}, minutes={minutes}: {iso_ts}"
    )
    return {"attempts": attempts, "target_field": target_field, "value": iso_ts, "minutes": minutes}


@register_action("bulk_update_requests_in_cart")
def action_bulk_update_requests_in_cart(
    ctx: Dict[str, Any],
    target_status: Optional[str] = None,
    copy_invoice_and_terms: bool = True,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Find all inventory_request records whose data.cart_id matches the cart record's id,
    and update their status (and optionally invoice_number, payment_terms from cart).
    Used when PM "applies" or "approves" a cart so all requests in the cart get the same
    status and shared invoice/terms.

    Args:
        ctx: Context containing 'record' (the inventory_cart) and 'payload'
        target_status: Status to set on all requests (e.g. PAYMENT_PENDING, IN_SHIPPING).
                       If None, taken from payload.target_status.
        copy_invoice_and_terms: If True, copy cart.data.invoice_number and
                               cart.data.payment_terms to each request's data.

    Returns:
        Dict with updated_count and list of updated record ids.
    """
    record = ctx["record"]
    payload = ctx.get("payload") or {}

    if record.entity_type != "inventory_cart":
        logger.warning(
            f"[bulk_update_requests_in_cart] Record {record.id} is not inventory_cart (entity_type={record.entity_type}), skipping"
        )
        return {"updated_count": 0, "updated_ids": [], "skipped_reason": "not_inventory_cart"}

    status_to_apply = target_status or payload.get("target_status")
    if not status_to_apply:
        logger.warning(
            "[bulk_update_requests_in_cart] No target_status in args or payload, skipping bulk update"
        )
        return {"updated_count": 0, "updated_ids": [], "skipped_reason": "no_target_status"}

    copy_invoice = copy_invoice_and_terms if "copy_invoice_and_terms" not in payload else payload.get("copy_invoice_and_terms", copy_invoice_and_terms)
    cart_data = record.data or {}
    invoice_number = cart_data.get("invoice_number") if copy_invoice else None
    payment_terms = cart_data.get("payment_terms") if copy_invoice else None
    comments = cart_data.get("comments") if copy_invoice else None

    # Match cart_id as string or int for backward compatibility
    cart_id_str = str(record.id)
    requests_qs = Record.objects.filter(
        tenant_id=record.tenant_id,
        entity_type="inventory_request",
    ).filter(Q(data__cart_id=cart_id_str) | Q(data__cart_id=record.id))

    updated_ids = []
    for req in requests_qs:
        if not isinstance(req.data, dict):
            req.data = {}
        req.data["status"] = status_to_apply
        if invoice_number is not None:
            req.data["invoice_number"] = invoice_number
        if payment_terms is not None:
            req.data["payment_terms"] = payment_terms
        if comments is not None:
            req.data["comments"] = comments
        req.save(update_fields=["data", "updated_at"])
        updated_ids.append(req.id)

    logger.info(
        f"[bulk_update_requests_in_cart] Cart {record.id}: updated {len(updated_ids)} request(s) to status={status_to_apply}"
    )
    return {"updated_count": len(updated_ids), "updated_ids": updated_ids}


@register_action("receive_add_to_inventory")
def action_receive_add_to_inventory(
    ctx: Dict[str, Any],
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    For an inventory_request (e.g. status IN_SHIPPING): add received quantity to inventory.
    - If an inventory_item exists with matching part_number_or_sku (or name), add quantity to
      that item's available_quantity and set request status to FULFILLED.
    - If no matching inventory_item exists, create a new inventory_item with data from the
      request (part_number_or_sku, name, quantity as available_quantity) and set request to FULFILLED.
    """
    record = ctx["record"]
    if record.entity_type != "inventory_request":
        logger.warning(
            f"[receive_add_to_inventory] Record {record.id} is not inventory_request (entity_type={record.entity_type}), skipping"
        )
        return {"success": False, "reason": "not_inventory_request", "inventory_item_id": None}

    data = record.data or {}
    part_number = (data.get("part_number_or_sku") or "").strip()
    name = (data.get("item_name_freeform") or data.get("name") or "").strip()
    qty = data.get("quantity_required") or data.get("quantity")
    if qty is None:
        try:
            qty = int(data.get("quantity_required", 0) or 0)
        except (TypeError, ValueError):
            qty = 0
    else:
        try:
            qty = int(qty)
        except (TypeError, ValueError):
            qty = 0

    if qty < 1:
        logger.warning(f"[receive_add_to_inventory] Request {record.id} has no positive quantity, skipping")
        return {"success": False, "reason": "no_quantity", "inventory_item_id": None}

    # Prefer match by part_number_or_sku; fallback to name
    lookup_value = part_number or name
    if not lookup_value:
        logger.warning(f"[receive_add_to_inventory] Request {record.id} has no part_number_or_sku or item name, skipping")
        return {"success": False, "reason": "no_identifier", "inventory_item_id": None}

    inventory_item = None
    if part_number:
        inventory_item = (
            Record.objects.filter(
                tenant_id=record.tenant_id,
                entity_type="inventory_item",
            )
            .filter(data__part_number_or_sku=part_number)
            .first()
        )
    if inventory_item is None and name:
        inventory_item = (
            Record.objects.filter(
                tenant_id=record.tenant_id,
                entity_type="inventory_item",
            )
            .filter(Q(data__name=name) | Q(data__part_number_or_sku=name))
            .first()
        )

    if inventory_item is not None:
        if not isinstance(inventory_item.data, dict):
            inventory_item.data = {}
        avail = inventory_item.data.get("available_quantity")
        alloc = inventory_item.data.get("allocated_quantity")
        try:
            avail = int(avail) if avail is not None else 0
        except (TypeError, ValueError):
            avail = 0
        try:
            alloc = int(alloc) if alloc is not None else 0
        except (TypeError, ValueError):
            alloc = 0
        inventory_item.data["available_quantity"] = avail + qty
        # Keep total_quantity = allocated_quantity + available_quantity
        inventory_item.data["total_quantity"] = alloc + (avail + qty)
        inventory_item.save(update_fields=["data", "updated_at"])
        logger.info(
            f"[receive_add_to_inventory] Request {record.id}: added {qty} to inventory_item {inventory_item.id} (available_quantity={inventory_item.data['available_quantity']}, total_quantity={inventory_item.data['total_quantity']})"
        )
    else:
        # Create new inventory_item from request data (total_quantity = allocated + available)
        new_data = {
            "part_number_or_sku": part_number or name,
            "name": name or part_number or "Received item",
            "available_quantity": qty,
            "allocated_quantity": 0,
            "total_quantity": qty,
            "status": "IN_STOCK",
        }
        # Copy optional fields from request if present
        for key in ("default_vendor", "location", "default_cost_per_unit"):
            if data.get(key) is not None:
                new_data[key] = data[key]
        if data.get("vendor_name") and "default_vendor" not in new_data:
            new_data["default_vendor"] = data["vendor_name"]
        inventory_item = Record.objects.create(
            tenant_id=record.tenant_id,
            entity_type="inventory_item",
            data=new_data,
        )
        logger.info(
            f"[receive_add_to_inventory] Request {record.id}: created inventory_item {inventory_item.id} with available_quantity={qty}"
        )

    # Mark request as FULFILLED
    if not isinstance(record.data, dict):
        record.data = {}
    record.data["status"] = "FULFILLED"
    record.save(update_fields=["data", "updated_at"])

    return {
        "success": True,
        "inventory_item_id": inventory_item.id,
        "quantity_added": qty,
    }


@register_action("roll_back_to_pm")
def action_roll_back_to_pm(
    ctx: Dict[str, Any],
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Set inventory_request status to PENDING_PM (e.g. defective product, problem with shipment).
    Used by inventory manager from Receive Shipments flow.
    """
    record = ctx["record"]
    if record.entity_type != "inventory_request":
        logger.warning(
            f"[roll_back_to_pm] Record {record.id} is not inventory_request (entity_type={record.entity_type}), skipping"
        )
        return {"success": False, "reason": "not_inventory_request"}
    if not isinstance(record.data, dict):
        record.data = {}
    record.data["status"] = "PENDING_PM"
    record.save(update_fields=["data", "updated_at"])
    logger.info(f"[roll_back_to_pm] Request {record.id} set to PENDING_PM")
    return {"success": True}


def _evaluate_simple_condition(condition: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
    """
    Simple condition evaluation without JSONLogic.
    Supports basic equality checks for now.
    
    Args:
        condition: Condition dictionary
        ctx: Context with record data
        
    Returns:
        True if condition matches, False otherwise
    """
    if not condition:
        return True  # Empty condition always matches
    
    def _resolve_operand(operand: Any) -> Any:
        if isinstance(operand, dict) and "var" in operand:
            field_path = operand["var"]
            parts = field_path.split(".")
            value: Any = ctx
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    value = getattr(value, part, None)
                if value is None:
                    break
            return value
        return operand

    # Logical NOT
    if "!" in condition:
        arg = condition["!"]
        return not _evaluate_simple_condition(arg if isinstance(arg, dict) else {"==": [arg, True]}, ctx)

    # Logical AND
    if "and" in condition:
        args = condition["and"]
        if isinstance(args, list):
            return all(_evaluate_simple_condition(a, ctx) if isinstance(a, dict) else bool(a) for a in args)
        return False

    # Logical OR
    if "or" in condition:
        args = condition["or"]
        if isinstance(args, list):
            return any(_evaluate_simple_condition(a, ctx) if isinstance(a, dict) else bool(a) for a in args)
        return False

    # Equality
    if "==" in condition:
        args = condition["=="]
        if isinstance(args, list) and len(args) == 2:
            left = _resolve_operand(args[0])
            right = _resolve_operand(args[1])
            return left == right

    # Less than
    if "<" in condition:
        args = condition["<"]
        if isinstance(args, list) and len(args) == 2:
            left = _resolve_operand(args[0])
            right = _resolve_operand(args[1])
            try:
                return left < right
            except Exception:
                return False

    # Greater than
    if ">" in condition:
        args = condition[">"]
        if isinstance(args, list) and len(args) == 2:
            left = _resolve_operand(args[0])
            right = _resolve_operand(args[1])
            try:
                return left > right
            except Exception:
                return False

    # Less than or equal
    if "<=" in condition:
        args = condition["<="]
        if isinstance(args, list) and len(args) == 2:
            left = _resolve_operand(args[0])
            right = _resolve_operand(args[1])
            try:
                return left <= right
            except Exception:
                return False

    # Greater than or equal
    if ">=" in condition:
        args = condition[">="]
        if isinstance(args, list) and len(args) == 2:
            left = _resolve_operand(args[0])
            right = _resolve_operand(args[1])
            try:
                return left >= right
            except Exception:
                return False
    
    # Default to False for unknown/unsupported conditions to avoid false positives
    return False


def _is_simple_condition(condition: Any) -> bool:
    """Return True if the condition only uses simple ops we support."""
    if not isinstance(condition, dict) or len(condition) != 1:
        return False
    (op, value), = condition.items()
    simple_ops = {"==", "<", ">", "<=", ">=", "and", "or", "!"}
    if op not in simple_ops:
        return False
    if op in {"and", "or"}:
        if not isinstance(value, list):
            return False
        return all(_is_simple_condition(v) if isinstance(v, dict) else True for v in value)
    if op == "!":
        return _is_simple_condition(value) if isinstance(value, dict) else True
    # Comparators & equality expect 2 args which can be literals or {"var": path}
    if not (isinstance(value, list) and len(value) == 2):
        return False
    for v in value:
        if isinstance(v, dict) and "var" in v:
            # var path must be string
            if not isinstance(v.get("var"), str):
                return False
        elif isinstance(v, dict):
            # Nested condition not supported inside operands
            return False
    return True


def execute_rules(event_name: str, record: Record, payload: Dict[str, Any], tenant_id: str) -> None:
    """
    Execute all matching rules for a given event.
    
    This is the main entry point for rule execution. It:
    1. Fetches all enabled rules for the tenant and event
    2. Evaluates each rule's condition
    3. Executes actions for matching rules
    4. Logs all executions to RuleExecutionLog
    
    Args:
        event_name: Name of the event that triggered rule evaluation
        record: Record instance that triggered the event
        payload: Additional payload data from the event
        tenant_id: ID of the tenant (for isolation)
    """
    start_time = time.time()
    cache_key = f"rules:{tenant_id}:{event_name}"
    rules_data = cache.get(cache_key)
    
    if rules_data is None:
        # Fetch rules from DB if not in cache
        rules = RuleSet.objects.filter(
            tenant_id=tenant_id,
            event_name=event_name,
            enabled=True
        ).only('id', 'condition', 'actions')
        # Cache the data as a list of dicts, not the queryset
        rules_data = list(rules.values('id', 'condition', 'actions'))
        cache.set(cache_key, rules_data, 60 * 5)  # 5 minutes

    if not rules_data:  # Early exit if no rules
        return
    
    logger.info(f"Evaluating {len(rules_data)} rules for event '{event_name}' on record {record.id}")

    # Snapshot record data once per event so all rule conditions see pre-update state
    record_data_snapshot = copy.deepcopy(record.data)
    
    for rule_data in rules_data:
        # Re-hydrate a RuleSet instance from cached data.
        # This is an in-memory object, not hitting the DB.
        # It has an ID, so it can be used for FK relations.
        rule = RuleSet(**rule_data)
        rule_start = time.time()
        
        # Create context for rule evaluation
        # JSONLogic expects a flat dictionary structure
        ctx = {
            "record": record,
            "payload": payload,
            "event": event_name,
            # Add record data directly for easier access
            "record_data": record_data_snapshot
        }
        
        # Initialize log data
        log_data = {
            "matched": False,
            "actions": [],
            "errors": []
        }
        
        try:
            # Evaluate rule condition using JSONLogic with template resolution
            # Log evaluation context and condition (shortened for readability)
            def _short(obj: Any, limit: int = 400) -> str:
                try:
                    s = json.dumps(obj, default=str)
                except Exception:
                    s = str(obj)
                return s if len(s) <= limit else s[:limit] + "... (truncated)"

            snapshot_attempts = record_data_snapshot.get("call_attempts") if isinstance(record_data_snapshot, dict) else None
            payload_keys = list(payload.keys()) if isinstance(payload, dict) else []
            logger.info(
                f"Rule {rule.id} evaluate: event='{event_name}' record={record.id} attempts_snapshot={snapshot_attempts} "
                f"condition={_short(rule.condition)} payload_keys={payload_keys}"
            )

            # Also log resolved condition at debug level
            resolved_for_log = _resolve_templates_in(rule.condition or {}, ctx)
            logger.debug(
                f"Rule {rule.id} resolved_condition={_short(resolved_for_log)}"
            )

            matched = _evaluate_condition(rule.condition or {}, ctx)
            log_data["matched"] = bool(matched)
            
            logger.info(f"Rule {rule.id} condition result: matched={matched}")
            
            # Execute actions if condition matched
            if matched:
                logger.info(f"Rule {rule.id} executing {len(rule.actions)} action(s)")
                for action_config in rule.actions:
                    action_name = action_config.get("action")
                    action_args = action_config.get("args", {})
                    
                    if action_name not in ACTIONS:
                        error_msg = f"Unknown action '{action_name}'"
                        log_data["errors"].append(error_msg)
                        logger.warning(f"Rule {rule.id}: {error_msg}")
                        continue
                    
                    try:
                        # Execute the action
                        action_func = ACTIONS[action_name]
                        logger.info(
                            f"Rule {rule.id} -> action '{action_name}' args={_short(action_args)}"
                        )
                        result = action_func(ctx, **action_args)
                        log_data["actions"].append({
                            "action": action_name,
                            "result": result
                        })
                        
                        logger.info(f"Rule {rule.id} executed action '{action_name}' successfully")
                        
                    except Exception as e:
                        error_msg = f"Action '{action_name}' failed: {str(e)}"
                        log_data["errors"].append(error_msg)
                        logger.error(f"Rule {rule.id}: {error_msg}")
            else:
                logger.info(f"Rule {rule.id} did not match. Skipping actions.")
            
        except Exception as e:
            error_msg = f"Rule evaluation failed: {str(e)}"
            log_data["errors"].append(error_msg)
            logger.error(f"Rule {rule.id}: {error_msg}")
        
        # Calculate execution duration
        duration_ms = (time.time() - rule_start) * 1000
        
        # Log the rule execution
        RuleExecutionLog.objects.create(
            tenant_id=tenant_id,
            record=record,
            rule=rule,
            event_name=event_name,
            matched=log_data["matched"],
            actions=log_data["actions"],
            errors=log_data["errors"],
            duration_ms=duration_ms
        )
        
        logger.debug(f"Rule {rule.id} execution logged in {duration_ms:.2f}ms")
    
    total_duration = (time.time() - start_time) * 1000
    logger.info(f"Rule execution completed for event '{event_name}' in {total_duration:.2f}ms")


def get_available_actions() -> List[str]:
    """
    Get list of all registered action names.
    
    Returns:
        List of action names that can be used in rules
    """
    return list(ACTIONS.keys())


def validate_rule_condition(condition: Dict[str, Any]) -> bool:
    """Validate a rule condition for syntax supported by the simple evaluator."""
    try:
        return _is_simple_condition(condition) or _evaluate_simple_condition(condition, {"record_data": {}, "payload": {}, "event": "test"}) in [True, False]
    except Exception as e:
        logger.warning(f"Invalid rule condition: {e}")
        return False
