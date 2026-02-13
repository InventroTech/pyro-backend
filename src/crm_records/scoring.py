"""
Dynamic Scoring Utility Module

This module provides reusable functions for calculating lead scores based on rules
stored in the EntityTypeSchema table. Can be called from anywhere in the codebase.
"""
import logging
from typing import Dict, Any, Optional, List
from .models import Record, EntityTypeSchema

logger = logging.getLogger(__name__)


def _get_nested_value(data: Dict[str, Any], attr_path: str) -> Any:
    """
    Get nested value from data dict using dot notation path.
    
    Example: data.assigned_to -> data['assigned_to']
    Example: data.user.profile.name -> data['user']['profile']['name']
    
    Args:
        data: Dictionary to extract value from
        attr_path: Dot-separated path to the value
        
    Returns:
        The value at the path, or None if not found
    """
    if not attr_path or not data:
        return None
    
    # Remove 'data.' prefix if present
    if attr_path.startswith('data.'):
        attr_path = attr_path[5:]  # Remove 'data.' prefix
    
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


def _evaluate_rule(lead_data: Dict[str, Any], rule: Dict[str, Any]) -> bool:
    """
    Evaluate if a rule matches the lead data.
    
    Args:
        lead_data: The data dict from the lead record
        rule: Dict with 'attr', 'operator', 'value', 'weight'
    
    Returns:
        True if rule matches, False otherwise
    """
    attr_path = rule.get('attr', '')
    operator = rule.get('operator', '==')
    expected_value = rule.get('value', '')
    
    # Get the actual value from lead data
    actual_value = _get_nested_value(lead_data, attr_path)
    
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
    
    # Fetch rules from EntityTypeSchema
    try:
        schema = EntityTypeSchema.objects.get(
            tenant_id=tenant_id,
            entity_type=entity_type
        )
        rules = schema.rules if schema.rules else []
    except EntityTypeSchema.DoesNotExist:
        logger.debug(f"calculate_lead_score: No schema found for entity_type '{entity_type}' and tenant {tenant_id}")
        return 0.0
    except Exception as e:
        logger.error(f"calculate_lead_score: Error fetching schema: {e}")
        return 0.0
    
    if not rules or len(rules) == 0:
        logger.debug(f"calculate_lead_score: No rules found for entity_type '{entity_type}'")
        return 0.0
    
    # Get lead data
    lead_data = lead.data if lead.data else {}
    
    # Calculate total score
    total_score = 0.0
    matched_rules_count = 0
    
    for rule in rules:
        if _evaluate_rule(lead_data, rule):
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


def get_scoring_rules(entity_type: str, tenant_id: str) -> List[Dict[str, Any]]:
    """
    Get scoring rules for a specific entity type and tenant.
    
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
    try:
        schema = EntityTypeSchema.objects.get(
            tenant_id=tenant_id,
            entity_type=entity_type
        )
        return schema.rules if schema.rules else []
    except EntityTypeSchema.DoesNotExist:
        logger.debug(f"get_scoring_rules: No schema found for entity_type '{entity_type}' and tenant {tenant_id}")
        return []
    except Exception as e:
        logger.error(f"get_scoring_rules: Error fetching schema: {e}")
        return []

