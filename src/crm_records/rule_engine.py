"""
Rule Engine Service Module

This module provides the core rule evaluation and execution logic for the dynamic CRM platform.
It handles fetching rules, evaluating conditions using JSONLogic, and executing actions.
"""
import time
import logging
from typing import Dict, Any, List, Optional
from django.utils import timezone
from json_logic import jsonLogic
import re

from .models import RuleSet, RuleExecutionLog, Record

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


def _build_jsonlogic_data(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Build a JSON-serializable data context for JSONLogic evaluation."""
    record = ctx.get("record")
    data: Dict[str, Any] = {
        "record_data": ctx.get("record_data"),
        "payload": ctx.get("payload"),
        "event": ctx.get("event"),
    }
    # Only expose safe/serializable parts of record
    if record is not None:
        data["record"] = {
            "id": getattr(record, "id", None),
        }
    else:
        data["record"] = {"id": None}
    return data


def _evaluate_condition(condition: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
    """
    Evaluate a rule condition using JSONLogic with template resolution.

    Falls back to the simple evaluator on errors for robustness.
    """
    if not condition:
        return True

    # Resolve templates (e.g., {{now}}, {{payload.x}}) inside the condition
    resolved_condition = _resolve_templates_in(condition, ctx)

    try:
        data = _build_jsonlogic_data(ctx)
        result = jsonLogic(resolved_condition, data)
        return bool(result)
    except Exception as e:
        logger.debug(f"JSONLogic evaluation failed, falling back to simple evaluator: {e}")
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
    
    # Simple equality check: {"==": [{"var": "field"}, "value"]}
    if "==" in condition:
        eq_parts = condition["=="]
        if len(eq_parts) == 2:
            var_part = eq_parts[0]
            expected_value = eq_parts[1]
            
            if "var" in var_part:
                field_path = var_part["var"]
                # Handle nested field paths like "record_data.status"
                if "." in field_path:
                    parts = field_path.split(".")
                    value = ctx
                    for part in parts:
                        if isinstance(value, dict):
                            value = value.get(part)
                        else:
                            value = None
                            break
                    actual_value = value
                else:
                    # Get value from context
                    actual_value = ctx.get(field_path)
                return actual_value == expected_value
    
    # Default to True for unknown conditions (for now)
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
    
    # Fetch all enabled rules for this tenant and event
    rules = RuleSet.objects.filter(
        tenant_id=tenant_id,
        event_name=event_name,
        enabled=True
    )
    
    logger.info(f"Evaluating {rules.count()} rules for event '{event_name}' on record {record.id}")
    
    for rule in rules:
        rule_start = time.time()
        
        # Create context for rule evaluation
        # JSONLogic expects a flat dictionary structure
        ctx = {
            "record": record,
            "payload": payload,
            "event": event_name,
            # Add record data directly for easier access
            "record_data": record.data
        }
        
        # Initialize log data
        log_data = {
            "matched": False,
            "actions": [],
            "errors": []
        }
        
        try:
            # Evaluate rule condition using JSONLogic with template resolution
            matched = _evaluate_condition(rule.condition or {}, ctx)
            log_data["matched"] = bool(matched)
            
            logger.debug(f"Rule {rule.id} condition evaluated to: {matched}")
            
            # Execute actions if condition matched
            if matched:
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
    """
    Validate a rule condition without executing it.
    
    Args:
        condition: JSONLogic condition to validate
        
    Returns:
        True if condition is valid, False otherwise
    """
    try:
        # Test with dummy context
        dummy_ctx = {
            "record": {"data": {}},
            "payload": {},
            "event": "test"
        }
        # Just try to parse the condition, don't actually evaluate it
        jsonLogic(condition, dummy_ctx)
        return True
    except Exception as e:
        logger.warning(f"Invalid rule condition: {e}")
        return False
