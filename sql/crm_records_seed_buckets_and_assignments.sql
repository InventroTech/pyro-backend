-- Seed system sales buckets (3 per tenant) and UserBucketAssignment rows for sales RMs.
-- Prerequisites: tables exist (migrate). Does not use UPDATE — INSERT only (+ ON CONFLICT DO NOTHING).
-- Re-run safe.

BEGIN;

-- ---------------------------------------------------------------------------
-- 1) Buckets: one row per (tenant, slug) with generic-pipeline filter_conditions
-- ---------------------------------------------------------------------------
INSERT INTO crm_records_bucket (
    created_at,
    updated_at,
    tenant_id,
    name,
    slug,
    description,
    filter_conditions,
    is_system,
    is_active
)
SELECT
    NOW(),
    NOW(),
    t.id,
    v.name,
    v.slug,
    '',
    v.filter_conditions::jsonb,
    v.is_system,
    TRUE
FROM tenants AS t
CROSS JOIN (
    VALUES
        (
            'Fresh Leads',
            'fresh_leads',
            '{
                "assigned_scope": "unassigned",
                "lead_stage": ["FRESH", "IN_QUEUE"],
                "call_attempts": {"lte": 0},
                "apply_routing_rule": true,
                "next_call_due": false,
                "daily_limit_applies": true
            }',
            FALSE
        ),
        (
            'Not Connected Retry',
            'not_connected_retry',
            '{
                "assigned_scope": "me",
                "lead_stage": ["NOT_CONNECTED", "IN_QUEUE"],
                "call_attempts": {"gte": 1, "lte": 6},
                "apply_routing_rule": false,
                "next_call_due": true
            }',
            TRUE
        ),
        (
            'Followup Callback',
            'followup_callback',
            '{
                "assigned_scope": "me",
                "lead_stage": ["SNOOZED", "IN_QUEUE"],
                "call_attempts": {"lt": 6},
                "apply_routing_rule": true,
                "next_call_due": true,
                "fallback_assigned_scope": "unassigned"
            }',
            TRUE
        )
) AS v(name, slug, filter_conditions, is_system)
ON CONFLICT (tenant_id, slug) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 2) Assignments: sales RMs from user_settings (LEAD_TYPE_ASSIGNMENT)
--    Exclude RMs who have SELF TRIAL in lead_sources or lead_statuses.
-- ---------------------------------------------------------------------------
INSERT INTO crm_records_userbucketassignment (
    created_at,
    updated_at,
    tenant_id,
    user_id,
    bucket_id,
    priority,
    pull_strategy,
    is_active
)
SELECT
    NOW(),
    NOW(),
    us.tenant_id,
    us.tenant_membership_id,
    b.id,
    CASE b.slug
        WHEN 'followup_callback' THEN 1
        WHEN 'not_connected_retry' THEN 2
        WHEN 'fresh_leads' THEN 3
        ELSE 100
    END,
    CASE b.slug
        WHEN 'fresh_leads' THEN '{"order_by": "score_desc", "include_snoozed_due": true, "ignore_score_for_sources": [], "tiebreaker": "desc", "tiebreaker_field": "created_at"}'::jsonb
        WHEN 'followup_callback' THEN '{"order_by": "score_desc", "include_snoozed_due": true, "ignore_score_for_sources": [], "tiebreaker": "desc", "tiebreaker_field": "created_at"}'::jsonb
        WHEN 'not_connected_retry' THEN '{"order_by": "score_desc", "include_snoozed_due": false, "ignore_score_for_sources": [], "tiebreaker": "desc", "tiebreaker_field": "created_at"}'::jsonb
        ELSE '{}'::jsonb
    END,
    TRUE
FROM user_settings AS us
INNER JOIN crm_records_bucket AS b
    ON b.tenant_id = us.tenant_id
    AND b.slug IN ('fresh_leads', 'not_connected_retry', 'followup_callback')
WHERE us.key = 'LEAD_TYPE_ASSIGNMENT'
  AND us.tenant_membership_id IS NOT NULL
  AND NOT EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(COALESCE(us.lead_sources, '[]'::jsonb)) AS elem
        WHERE elem = 'SELF TRIAL'
    )
  AND NOT EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(COALESCE(us.lead_statuses, '[]'::jsonb)) AS elem
        WHERE elem = 'SELF TRIAL'
    )
ON CONFLICT (tenant_id, user_id, bucket_id) DO NOTHING;

COMMIT;
