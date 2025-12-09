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

from .models import RuleSet, RuleExecutionLog, Record
from support_ticket.services import MixpanelService
from background_jobs.queue_service import get_queue_service
from background_jobs.models import JobType

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

    # Resolve any templates in updates based on current context
    resolved_updates = _resolve_templates_in(updates or {}, ctx)

    # Apply direct field updates
    for key, value in resolved_updates.items():
        record.data[key] = value

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

    # Build complete properties dict with all record data
    mixpanel_properties = {
        'record_id': record.id,
        'entity_type': record.entity_type,
        'name': record.name,
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

    try:

        service = MixpanelService()
        success = service.send_to_mixpanel_sync(
            str(resolved_user_id),
            str(resolved_event_name),
            mixpanel_properties,
        # Enqueue job for async processing
        queue_service = get_queue_service()
        job = queue_service.enqueue_job(
            job_type=JobType.SEND_MIXPANEL_EVENT,
            payload={
                "user_id": str(resolved_user_id),
                "event_name": str(resolved_event_name),
                "properties": resolved_properties,
            },
            tenant_id=tenant_id,
        )
        
        logger.info(

            f"Mixpanel event sent for record {record.id}: event='{resolved_event_name}' success={success} properties_count={len(mixpanel_properties)}"
            f"Mixpanel event queued for record {record.id}: job_id={job.id}, "
            f"event='{resolved_event_name}'"
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
) -> Dict[str, Any]:
    """
    Compute next_call_at = now + (base_minutes_per_attempt * current_attempts) minutes
    using attempts from record.data[attempts_field], then write to record.data[target_field].
    """
    record = ctx["record"]
    attempts_raw = record.data.get(attempts_field, 0)
    try:
        attempts = int(attempts_raw or 0)
    except Exception:
        attempts = 0

    minutes = base_minutes_per_attempt * attempts
    next_time = timezone.now() + timedelta(minutes=minutes)
    iso_ts = next_time.isoformat()

    record.data[target_field] = iso_ts
    record.save(update_fields=["data", "updated_at"])
    logger.info(
        f"Computed {target_field} for record {record.id} using attempts={attempts}: {iso_ts}"
    )
    return {"attempts": attempts, "target_field": target_field, "value": iso_ts}


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
