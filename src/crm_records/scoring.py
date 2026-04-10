"""
Dynamic Scoring Utility Module

This module provides reusable functions for calculating lead scores based on rules
stored in the EntityTypeSchema table. Can be called from anywhere in the codebase.
"""
import logging
from typing import Dict, Any, Optional, List

from django.db import transaction, connection
from django.db.utils import OperationalError

from .models import Record, EntityTypeSchema

logger = logging.getLogger(__name__)


def _get_attribute_value(record: Record, attr_path: str) -> Any:
    """
    Get attribute value from Record, checking both direct Record fields and nested data fields.
    
    Handles:
    - Direct Record fields: id, entity_type, created_at, updated_at, tenant_id
    - Nested data fields: data.assigned_to, data.user.profile.name
    
    Args:
        record: Record instance
        attr_path: Attribute path (e.g., 'id', 'entity_type', 'data.assigned_to', 'data.user.name')
        
    Returns:
        The value at the path, or None if not found
        
    Examples:
        >>> _get_attribute_value(lead, 'id')  # Returns lead.id
        >>> _get_attribute_value(lead, 'entity_type')  # Returns lead.entity_type
        >>> _get_attribute_value(lead, 'data.assigned_to')  # Returns lead.data['assigned_to']
        >>> _get_attribute_value(lead, 'assigned_to')  # Returns lead.data['assigned_to'] (if data. prefix missing)
    """
    if not attr_path or not record:
        return None
    
    # List of direct Record model fields (not in data JSONB)
    direct_fields = {
        'id', 'entity_type', 'created_at', 'updated_at', 
        'tenant', 'tenant_id', 'pyro_data'
    }
    
    # Check if it's a direct Record field (no 'data.' prefix and matches direct fields)
    if not attr_path.startswith('data.') and attr_path in direct_fields:
        try:
            # Handle tenant specially (it's a ForeignKey object)
            if attr_path == 'tenant_id' and hasattr(record, 'tenant'):
                return str(record.tenant.id) if record.tenant else None
            elif attr_path == 'tenant' and hasattr(record, 'tenant'):
                return record.tenant.id if record.tenant else None
            else:
                value = getattr(record, attr_path, None)
                # Convert datetime to string for comparison
                if hasattr(value, 'isoformat'):
                    return value.isoformat()
                return value
        except (AttributeError, TypeError):
            return None
    
    # Handle nested data fields (with or without 'data.' prefix)
    # Remove 'data.' prefix if present
    if attr_path.startswith('data.'):
        attr_path = attr_path[5:]  # Remove 'data.' prefix
    
    # Get data dict
    data = record.data if record.data else {}
    if not data:
        return None
    
    # Navigate nested path
    keys = attr_path.split('.')
    value = data
    
    try:
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        return value
    except (TypeError, KeyError, AttributeError):
        return None


def _evaluate_rule(record: Record, rule: Dict[str, Any]) -> bool:
    """
    Evaluate if a rule matches the record.
    
    Args:
        record: Record instance (lead, ticket, etc.)
        rule: Dict with 'attr', 'operator', 'value', 'weight'
    
    Returns:
        True if rule matches, False otherwise
    """
    attr_path = rule.get('attr', '')
    operator = str(rule.get('operator') or '==')
    expected_value = rule.get('value', '')
    
    # Get the actual value from record (checks both direct fields and data JSONB)
    actual_value = _get_attribute_value(record, attr_path)


    if operator == "isNull":
        return actual_value is None
    if operator == "isNotNull":
        return actual_value is not None

    

    if actual_value is None:
        return False

    # Convert to string for comparison (handles different types)
    actual_str = str(actual_value).lower() if actual_value is not None else ''
    expected_str = str(expected_value).lower() if expected_value is not None else ''
    
    try:
        if operator == '==':
            return actual_str == expected_str
        elif operator == '!=':
            return actual_str != expected_str
        elif operator == '>':
            return float(actual_value) > float(expected_value)
        elif operator == '<':
            return float(actual_value) < float(expected_value)
        elif operator == '>=':
            return float(actual_value) >= float(expected_value)
        elif operator == '<=':
            return float(actual_value) <= float(expected_value)
        elif operator == 'contains':
            return expected_str in actual_str
        elif operator == 'in':
            # expected_value should be a comma-separated list or list
            if isinstance(expected_value, list):
                return actual_str in [str(v).lower() for v in expected_value]
            else:
                values = [v.strip().lower() for v in str(expected_value).split(',')]
                return actual_str in values
        else:
            return False
    except (ValueError, TypeError):
        # If conversion fails, fall back to string comparison
        if operator in ['==', '!=']:
            return actual_str == expected_str if operator == '==' else actual_str != expected_str
        return False


def calculate_lead_score(lead: Record, tenant_id: Optional[str] = None) -> float:
    """
    Calculate the score for a lead based on rules stored in EntityTypeSchema.
    
    This function:
    1. Fetches rules from EntityTypeSchema for the lead's entity_type
    2. Evaluates each rule against the lead's data
    3. Sums up weights for matching rules
    4. Returns the total score
    
    Args:
        lead: Record instance (should have entity_type='lead')
        tenant_id: Optional tenant ID. If not provided, uses lead.tenant_id
        
    Returns:
        Total score (float) calculated from matching rules. Returns 0.0 if:
        - No rules found for the entity_type
        - No rules match
        - Lead data is empty
        
    Example:
        >>> from crm_records.models import Record
        >>> from crm_records.scoring import calculate_lead_score
        >>> 
        >>> lead = Record.objects.get(id=123)
        >>> score = calculate_lead_score(lead)
        >>> print(f"Lead score: {score}")
    """
    if not lead:
        logger.warning("calculate_lead_score: lead is None")
        return 0.0
    
    # Get tenant_id
    tenant = lead.tenant if hasattr(lead, 'tenant') else None
    tenant_id = tenant_id or (tenant.id if tenant else None)
    
    if not tenant_id:
        logger.warning(f"calculate_lead_score: No tenant_id for lead {lead.id}")
        return 0.0
    
    # Get entity_type from lead
    entity_type = lead.entity_type if hasattr(lead, 'entity_type') else 'lead'
    
    # Fetch rules using get_scoring_rules (which prioritizes ScoringRule table)
    rules = get_scoring_rules(entity_type, tenant_id)
    
    if not rules or len(rules) == 0:
        logger.debug(f"calculate_lead_score: No rules found for entity_type '{entity_type}'")
        return 0.0
    
    # Calculate total score
    total_score = 0.0
    matched_rules_count = 0
    
    for rule in rules:
        if _evaluate_rule(lead, rule):
            weight = rule.get('weight', 0)
            try:
                total_score += float(weight)
                matched_rules_count += 1
            except (ValueError, TypeError):
                logger.warning(f"calculate_lead_score: Invalid weight in rule: {rule}")
                continue
    
    logger.debug(
        f"calculate_lead_score: Lead {lead.id} scored {total_score} "
        f"(matched {matched_rules_count} out of {len(rules)} rules)"
    )
    
    return total_score


def calculate_and_update_lead_score(lead: Record, tenant_id: Optional[str] = None, save: bool = True) -> float:
    """
    Calculate the score for a lead and optionally update it in the database.
    
    This is a convenience function that:
    1. Calculates the score using calculate_lead_score()
    2. Updates lead.data['lead_score'] with the calculated score
    3. Optionally saves the lead to the database
    
    Args:
        lead: Record instance (should have entity_type='lead')
        tenant_id: Optional tenant ID. If not provided, uses lead.tenant_id
        save: If True, saves the lead to database. If False, only updates in memory.
        
    Returns:
        Total score (float) calculated from matching rules
        
    Example:
        >>> from crm_records.models import Record
        >>> from crm_records.scoring import calculate_and_update_lead_score
        >>> 
        >>> lead = Record.objects.get(id=123)
        >>> score = calculate_and_update_lead_score(lead, save=True)
        >>> print(f"Updated lead score: {score}")
    """
    score = calculate_lead_score(lead, tenant_id)
    
    # Update lead data
    if not lead.data:
        lead.data = {}
    
    lead.data['lead_score'] = score
    logger.info(f"calculate_and_update_lead_score: Lead {lead.id} score: {score}")
    
    # Save if requested
    if save:
        lead.save(update_fields=['data', 'updated_at'])
        logger.debug(f"calculate_and_update_lead_score: Updated and saved lead {lead.id} with score {score}")
    else:
        logger.debug(f"calculate_and_update_lead_score: Updated lead {lead.id} with score {score} (not saved)")
    
    return score


def _build_rule_sql_expression(rule: Dict[str, Any]) -> tuple[str, list]:
    """
    Translate a scoring rule into a SQL fragment that returns a numeric weight.

    The fragment is designed to operate on the `data` JSONB column:
    - string comparisons: use lower() normalization (to match Python logic)
    - numeric comparisons: cast to float only when the value matches a numeric regex

    Returns:
        (fragment_sql, params_list)

    Notes:
        - Only `data.*` attributes are supported. Rules targeting non-JSON fields
          (id, entity_type, etc.) are treated as non-matching and contribute 0.
    """
    attr_path: str = str(rule.get("attr") or "").strip()
    operator: str = str(rule.get("operator") or "==")
    expected_value = rule.get("value", "")
    weight = float(rule.get("weight", 0) or 0)

    if not attr_path:
        return "0", []

    if attr_path.startswith("data."):
        attr_path = attr_path[5:]

    # Skip rules that reference model-level columns (not JSONB).
    direct_fields = {"id", "entity_type", "created_at", "updated_at", "tenant", "tenant_id", "pyro_data"}
    if attr_path in direct_fields:
        return "0", []

    keys = [k for k in attr_path.split(".") if k]
    if not keys:
        return "0", []

    # Escape JSON path key literals for safe embedding into Postgres string literals.
    def _escape_key(k: str) -> str:
        return str(k).replace("'", "''")

    if len(keys) == 1:
        json_accessor = f"data->>'{_escape_key(keys[0])}'"
    else:
        pg_path = ",".join(_escape_key(k) for k in keys)
        json_accessor = f"data#>>'{{{pg_path}}}'"

    params: list = []

    if operator == "isNull":
        condition = f"({json_accessor} IS NULL)"
        fragment = f"CASE WHEN {condition} THEN {weight} ELSE 0 END"
        return fragment, []
    if operator == "isNotNull":
        condition = f"({json_accessor} IS NOT NULL)"
        fragment = f"CASE WHEN {condition} THEN {weight} ELSE 0 END"
        return fragment, []

    if operator == "==":
        condition = f"lower({json_accessor}) = lower(%s)"
        params = [str(expected_value)]
    elif operator == "!=":
        condition = f"lower({json_accessor}) != lower(%s)"
        params = [str(expected_value)]
    elif operator == "contains":
        condition = f"lower({json_accessor}) LIKE '%%' || lower(%s) || '%%'"
        params = [str(expected_value)]
    elif operator == "in":
        if isinstance(expected_value, list):
            values = [str(v).lower() for v in expected_value]
        else:
            values = [v.strip().lower() for v in str(expected_value).split(",")]
        if not values:
            return "0", []
        placeholders = ", ".join(["%s"] * len(values))
        condition = f"lower({json_accessor}) IN ({placeholders})"
        params = values
    elif operator in {">", "<", ">=", "<="}:
        # Guard against cast errors by checking the numeric pattern first.
        # This matches Python's behavior: if float conversion fails, treat as non-match.
        numeric_regex = r"^-?\d+(\.\d+)?$"
        op = operator
        condition = (
            f"CASE WHEN ({json_accessor}) ~ %s "
            f"THEN (({json_accessor})::float {op} %s) "
            f"ELSE false END"
        )
        try:
            numeric_value = float(expected_value)
        except (ValueError, TypeError):
            return "0", []
        params = [numeric_regex, numeric_value]
    else:
        # Unsupported operator contributes 0.
        return "0", []

    fragment = f"CASE WHEN {condition} THEN {weight} ELSE 0 END"
    return fragment, params


def score_chunk_sql(
    tenant_id: Any,
    entity_type: str,
    rules: List[Dict[str, Any]],
    id_gte: int,
    id_lt: int,
) -> Dict[str, Any]:
    """
    Score a chunk of records (by ID range) using a single set-based SQL UPDATE.

    Updates:
        records.data->'lead_score' for rows matching tenant/entity_type and id range.
    Returns:
        processed_leads, updated_leads, total_score_added, plus id range metadata.
    """
    import time as _time

    start = _time.monotonic()

    if id_gte is None or id_lt is None:
        raise ValueError("score_chunk_sql requires id_gte and id_lt")

    fragments: list[str] = []
    rule_params: list = []
    for rule in rules or []:
        frag, params = _build_rule_sql_expression(rule)
        fragments.append(frag)
        rule_params.extend(params)

    score_expr = " + ".join(fragments) if fragments else "0"

    sql = f"""
        WITH scored AS (
            UPDATE records
            SET data = jsonb_set(
                COALESCE(data, '{{}}'::jsonb),
                ARRAY['lead_score'],
                to_jsonb(({score_expr})::float),
                true
            ),
            updated_at = NOW()
            WHERE tenant_id = %s
              AND entity_type = %s
              AND id >= %s
              AND id < %s
              AND UPPER(COALESCE(data->>'lead_stage','')) NOT IN ('CLOSED','TRIAL_ACTIVATED','NOT_INTERESTED')
            RETURNING
                id,
                (data->>'lead_score')::float AS new_score
        )
        SELECT
            COUNT(*) AS processed,
            COUNT(*) FILTER (WHERE new_score > 0) AS updated,
            COALESCE(SUM(new_score), 0) AS total_score
        FROM scored;
    """

    # Placeholder ordering:
    # - score_expr parameters appear first (rule_params)
    # - then tenant/entity/range placeholders in the WHERE clause
    params = rule_params + [str(tenant_id), entity_type, int(id_gte), int(id_lt)]

    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        row = cursor.fetchone()

    processed, updated, total_score = row
    elapsed = _time.monotonic() - start

    return {
        "id_gte": int(id_gte),
        "id_lt": int(id_lt),
        "processed_leads": int(processed),
        "updated_leads": int(updated),
        "total_score_added": float(total_score),
        "progress_percentage": 100,
        "status": "completed",
        "execution_time_seconds": round(elapsed, 3),
    }


def score_all_records_for_tenant(
    tenant_id: Any,
    entity_type: str = "lead",
    batch_size: int = 100,
) -> Dict[str, Any]:
    """
    Recompute data.lead_score for every Record of entity_type for the tenant.

    Per-row atomic transaction with select_for_update. Used by SCORE_LEADS jobs, rule/schema
    endpoints that enqueue bulk rescoring, and any explicit call sites—not by Record.save().
    """
    qs = Record.objects.filter(tenant_id=tenant_id, entity_type=entity_type)
    total_leads = qs.count()
    updated_count = 0
    total_score_added = 0.0
    processed_count = 0

    for i in range(0, total_leads, batch_size):
        batch = qs[i : i + batch_size]
        for lead in batch:
            try:
                with transaction.atomic():
                    locked_lead = Record.objects.select_for_update(skip_locked=True).get(
                        pk=lead.pk,
                        tenant_id=tenant_id,
                        entity_type=entity_type,
                    )
                    score = calculate_and_update_lead_score(
                        locked_lead,
                        tenant_id=tenant_id,
                        save=True,
                    )
                    processed_count += 1
                    if score > 0:
                        updated_count += 1
                        total_score_added += score
            except Record.DoesNotExist:
                logger.warning("score_all_records_for_tenant: record %s missing, skip", lead.pk)
                processed_count += 1
                continue
            except OperationalError as e:
                err = str(e).lower()
                if "deadlock" in err or "lock" in err:
                    logger.warning(
                        "score_all_records_for_tenant: lock issue record %s: %s",
                        lead.pk,
                        e,
                    )
                    processed_count += 1
                    continue
                raise
            except Exception as e:
                logger.error(
                    "score_all_records_for_tenant: error scoring record %s: %s",
                    lead.pk,
                    e,
                    exc_info=True,
                )
                processed_count += 1
                continue

    return {
        "total_leads": total_leads,
        "processed_leads": processed_count,
        "updated_leads": updated_count,
        "total_score_added": total_score_added,
        "progress_percentage": 100,
        "status": "completed",
    }


def get_scoring_rules(entity_type: str, tenant_id: str) -> List[Dict[str, Any]]:
    """
    Get scoring rules for a specific entity type and tenant.
    
    Prioritizes ScoringRule table, falls back to EntityTypeSchema for backward compatibility.
    
    Args:
        entity_type: The entity type (e.g., 'lead', 'ticket')
        tenant_id: The tenant ID
        
    Returns:
        List of rule dictionaries. Returns empty list if no rules found.
        
    Example:
        >>> from crm_records.scoring import get_scoring_rules
        >>> 
        >>> rules = get_scoring_rules('lead', tenant_id='123e4567-...')
        >>> print(f"Found {len(rules)} rules")
    """
    from .models import ScoringRule
    
    # First, try to fetch from ScoringRule table
    try:
        scoring_rules = ScoringRule.objects.filter(
            tenant_id=tenant_id,
            entity_type=entity_type,
            is_active=True
        ).order_by('order', 'created_at')
        
        if scoring_rules.exists():
            # Convert ScoringRule instances to rule dictionaries
            rules = []
            for rule in scoring_rules:
                rule_dict = {
                    'attr': rule.attribute,
                    'operator': (
                        str(rule.data.get('operator') or '==')
                        if isinstance(rule.data, dict)
                        else '=='
                    ),
                    'value': rule.data.get('value', '') if isinstance(rule.data, dict) else '',
                    'weight': rule.weight,
                }
                rules.append(rule_dict)
            
            logger.debug(f"get_scoring_rules: Found {len(rules)} active rules from ScoringRule table for entity_type '{entity_type}' and tenant {tenant_id}")
            return rules
    except Exception as e:
        logger.error(f"get_scoring_rules: Error fetching ScoringRule: {e}")
    
    # Fallback to EntityTypeSchema for backward compatibility
    try:
        schema = EntityTypeSchema.objects.get(
            tenant_id=tenant_id,
            entity_type=entity_type
        )
        rules = schema.rules if schema.rules else []
        if rules:
            logger.debug(f"get_scoring_rules: Found {len(rules)} rules from EntityTypeSchema (fallback) for entity_type '{entity_type}' and tenant {tenant_id}")
        return rules
    except EntityTypeSchema.DoesNotExist:
        logger.debug(f"get_scoring_rules: No rules found for entity_type '{entity_type}' and tenant {tenant_id}")
        return []
    except Exception as e:
        logger.error(f"get_scoring_rules: Error fetching schema: {e}")
        return []

