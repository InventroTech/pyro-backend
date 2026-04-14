from django.db import models
from django.db.models import Q
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.core.cache import cache
from core.models import BaseModel, TenantModel
from object_history.models import HistoryTrackedModel


class Record(HistoryTrackedModel, BaseModel):
    """
    Universal record model that can hold any tenant's data dynamically using JSONB.
    All future entities (leads, tickets, job applications, etc.) will be built on top of this.

    Lead queue / reassignment (``entity_type="lead"``, ``data`` JSON) — notable keys:

    - ``first_assigned_today_at``: ISO timestamp when the lead was assigned in the current
      stint (unassigned→assigned). Used with ``ReleaseLeadsAfter12hJobHandler``: 12h after
      this moment, NOT_CONNECTED leads are unassigned but stay NOT_CONNECTED.
    - ``first_assignment_today_date``: Local calendar date (``YYYY-MM-DD``) when the anchor
      was set (audit / debugging).
    - ``first_assigned_at`` / ``first_assigned_to``: Original fresh assignment (daily limit);
      not cleared by the 12h NOT_CONNECTED release job.
    - Legacy ``not_connected_unassign_at``: honored by the release job only if
      ``first_assigned_today_at`` is absent.
    """
    entity_type = models.CharField(max_length=100, db_index=True)
    data = models.JSONField(default=dict, blank=True)
    pyro_data = models.JSONField(default=dict, blank=True, null=True, help_text="Additional JSON data for Pyro-specific fields")

    class Meta:
        db_table = "records"
        indexes = [
            models.Index(fields=["tenant", "entity_type", "-created_at"]),
            # GIN index on JSONB data for generic key lookups
            GinIndex(fields=["data"], name="records_data_gin_idx"),
            # GIN index on JSONB pyro_data for generic key lookups
            GinIndex(fields=["pyro_data"], name="records_pyro_data_gin_idx"),
            # Note: Expression indexes for JSON fields are created via migration
            # See migrations: lead_stage, lead_source, lead_status, lead_score (btree on data->>…);
            # plus assigned_to, affiliated_party, praja_id, next_call_at
        ]

    def __str__(self):
        name = (self.data or {}).get('name', '') if isinstance(self.data, dict) else ''
        return f"{self.entity_type}: {name or 'Unnamed'}"


class EventLog(HistoryTrackedModel, BaseModel):
    """
    Event logging model for tracking all record-related events.
    Stores events triggered by user actions or system processes.
    """
    record = models.ForeignKey("Record", on_delete=models.SET_NULL, null=True, blank=True, related_name="events")
    event = models.CharField(max_length=100, db_index=True)
    payload = models.JSONField(default=dict, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = "event_logs"
        indexes = [
            models.Index(fields=["tenant", "record", "event", "-timestamp"]),
        ]

    def __str__(self):
        return f"{self.event} for {self.record} at {self.timestamp}"


class RuleSet(HistoryTrackedModel, BaseModel):
    """
    Rule configuration model for declarative event-driven workflows.
    Allows tenants to define rules that trigger actions when specific events occur.
    """
    event_name = models.CharField(max_length=100, db_index=True)
    condition = models.JSONField(default=dict, blank=True)   # e.g. JSONLogic
    actions = models.JSONField(default=list, blank=True)     # list of action objects
    enabled = models.BooleanField(default=True)
    description = models.TextField(null=True, blank=True)

    class Meta:
        db_table = "rule_sets"
        indexes = [
            models.Index(fields=["tenant", "event_name", "enabled"]),
        ]

    def __str__(self):
        return f"Rule: {self.event_name} ({'enabled' if self.enabled else 'disabled'})"


class RuleExecutionLog(HistoryTrackedModel, BaseModel):
    """
    Execution logging model for tracking rule executions.
    Stores every time a rule is evaluated and executed for debugging and auditing.
    """
    record = models.ForeignKey("Record", on_delete=models.CASCADE, related_name="rule_executions")
    rule = models.ForeignKey("RuleSet", on_delete=models.SET_NULL, null=True, blank=True)
    event_name = models.CharField(max_length=100, db_index=True)
    matched = models.BooleanField(default=False)
    actions = models.JSONField(default=list, blank=True)
    errors = models.JSONField(default=list, blank=True)
    duration_ms = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "rule_exec_logs"
        indexes = [
            models.Index(fields=["tenant", "event_name", "-created_at"]),
        ]

    def __str__(self):
        return f"Rule execution: {self.event_name} ({'matched' if self.matched else 'no match'}) at {self.created_at}"


class PartnerEvent(HistoryTrackedModel, BaseModel):
    """
    Stores every incoming partner webhook event (e.g. Halocom work_on_lead) with full payload.
    Gives a durable audit trail and debugging source independent of background_jobs.
    """
    partner_slug = models.CharField(max_length=64, db_index=True)
    event = models.CharField(max_length=100, db_index=True)
    payload = models.JSONField(default=dict, blank=True, help_text="Full request payload (praja_id, email_id, etc.)")
    status = models.CharField(
        max_length=20,
        db_index=True,
        default="pending",
        help_text="pending, processing, completed, failed",
    )
    record = models.ForeignKey(
        Record,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="partner_events",
        help_text="Resolved lead record when applicable",
    )
    job_id = models.PositiveIntegerField(
        null=True,
        blank=True,
        db_index=True,
        help_text="Background job id that processed this event",
    )
    processed_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)

    class Meta:
        db_table = "partner_events"
        indexes = [
            models.Index(fields=["tenant", "partner_slug", "-created_at"]),
            models.Index(fields=["tenant", "status", "-created_at"]),
        ]
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.partner_slug} {self.event} ({self.status}) at {self.created_at}"


class EntityTypeSchema(HistoryTrackedModel, BaseModel):
    """
    Schema definition for entity types - stores the list of attributes for each entity type.
    This allows defining the structure/schema of each entity type.
    """
    entity_type = models.CharField(max_length=100, db_index=True, help_text="The entity type (e.g., 'lead', 'ticket', 'job')")
    attributes = ArrayField(
        models.CharField(max_length=255),
        default=list,
        blank=True,
        help_text="List of all attribute paths for this entity type (e.g., ['id', 'name', 'data.email', 'data.phone'])"
    )
    rules = models.JSONField(
        default=list,
        blank=True,
        help_text="List of scoring rules for this entity type. Each rule has 'attr', 'operator', 'value', and 'weight'."
    )
    description = models.TextField(null=True, blank=True, help_text="Optional description of this entity type schema")
    
    class Meta:
        db_table = "entity_type_schemas"
        unique_together = [['tenant', 'entity_type']]  # One schema per entity_type per tenant
        indexes = [
            models.Index(fields=["tenant", "entity_type"]),
        ]
    
    def __str__(self):
        return f"{self.entity_type} ({len(self.attributes)} attributes, {len(self.rules)} rules)"


class ScoringRule(HistoryTrackedModel, BaseModel):
    """
    Individual scoring rule model for managing lead scoring rules.
    Each rule can be created, edited, and deleted independently.
    
    Flexible structure: Stores rule configuration in JSON 'data' field.
    This allows any rule structure without migration changes.
    """
    entity_type = models.CharField(
        max_length=100,
        db_index=True,
        default='lead',
        help_text="The entity type this rule applies to (e.g., 'lead', 'ticket')"
    )
    attribute = models.CharField(
        max_length=255,
        help_text="Attribute path in dot notation (e.g., 'data.assigned_to', 'data.affiliated_party')"
    )
    data = models.JSONField(
        default=dict,
        blank=True,
        help_text="Rule configuration data (operator, value, and any other fields). Structure can be anything."
    )
    weight = models.FloatField(
        help_text="Score weight/points added when this rule matches"
    )
    order = models.IntegerField(
        default=0,
        help_text="Display order for rules (lower numbers appear first)"
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Whether this rule is active and should be evaluated"
    )
    description = models.TextField(
        null=True,
        blank=True,
        help_text="Optional description of what this rule does"
    )
    
    class Meta:
        db_table = "scoring_rules"
        indexes = [
            models.Index(fields=["tenant", "entity_type", "is_active"], name='scoring_rules_te_active_idx'),
            models.Index(fields=["tenant", "entity_type", "order"], name='scoring_rules_te_order_idx'),
        ]
        ordering = ['order', 'created_at']
    
    def __str__(self):
        operator = self.data.get('operator', 'N/A') if isinstance(self.data, dict) else 'N/A'
        value = self.data.get('value', 'N/A') if isinstance(self.data, dict) else 'N/A'
        return f"{self.entity_type}: {self.attribute} {operator} {value} (weight: {self.weight})"


class ApiSecretKey(HistoryTrackedModel, BaseModel):
    """
    Model to store API secret keys and their associated tenants.
    Secret is stored plainly; lookup is simple equality match (no hashing).
    """
    secret = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        db_index=True,
        help_text="API secret value (plain). Used for X-Secret-Pyro; set by set_raw_secret().",
    )
    secret_key_last4 = models.CharField(
        max_length=4,
        null=True,
        blank=True,
        help_text="Last 4 chars of raw secret for identification (non-sensitive)."
    )
    tenant = models.ForeignKey(
        'core.Tenant',
        on_delete=models.CASCADE,
        related_name='api_secret_keys',
        help_text="The tenant this secret key maps to"
    )
    description = models.TextField(
        null=True,
        blank=True,
        help_text="Optional description for this secret key (e.g., client name, purpose)"
    )
    is_active = models.BooleanField(
        default=True,
        db_index=True,
        help_text="Whether this secret key is currently active"
    )
    last_used_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Timestamp of last successful API request using this secret key"
    )
    
    class Meta:
        db_table = "api_secret_keys"
        indexes = [
            models.Index(fields=["secret", "is_active"]),
            models.Index(fields=["tenant", "is_active"]),
        ]
        verbose_name = "API Secret Key"
        verbose_name_plural = "API Secret Keys"

    def set_raw_secret(self, raw_secret: str) -> None:
        raw_secret = (raw_secret or "").strip()
        if not raw_secret:
            raise ValueError("raw_secret cannot be empty")
        self.secret = raw_secret
        self.secret_key_last4 = raw_secret[-4:] if len(raw_secret) >= 4 else raw_secret

    def __str__(self):
        last4 = self.secret_key_last4 or "????"
        return f"****{last4} -> {self.tenant.slug if self.tenant else 'No Tenant'}"


class CallAttemptMatrix(HistoryTrackedModel, BaseModel):
    """
    Stores call attempt configuration per lead type.
    Defines max call attempts, SLA in days, and minimum time difference between calls.
    """
    lead_type = models.CharField(
        max_length=100,
        db_index=True,
        help_text="Lead type (e.g., 'BJP', 'AAP', 'Congress', 'TDP', 'TMC', 'CITU', 'CPIM')"
    )
    max_call_attempts = models.PositiveSmallIntegerField(
        default=5,
        help_text="Maximum number of call attempts (m)"
    )
    sla_days = models.PositiveSmallIntegerField(
        default=2,
        help_text="SLA in days (n)"
    )
    min_time_between_calls_hours = models.PositiveSmallIntegerField(
        default=3,
        help_text="Minimum time difference between calls in hours (K)"
    )

    class Meta:
        db_table = "call_attempt_matrix"
        unique_together = [['tenant', 'lead_type']]  # One configuration per lead type per tenant
        indexes = [
            models.Index(fields=["tenant", "lead_type"]),
        ]
        verbose_name = "Call Attempt Matrix"
        verbose_name_plural = "Call Attempt Matrices"

    def __str__(self):
        return f"{self.lead_type}: {self.max_call_attempts} attempts, {self.sla_days} days SLA, {self.min_time_between_calls_hours}h min interval"


class Bucket(HistoryTrackedModel, BaseModel):
    """
    A named, filterable slice of the lead pool for routing/assignment.

    ``filter_conditions`` is interpreted by ``BucketQuerysetBuilder.build()`` (lead pipeline).
    Vocabulary (all keys optional unless noted):

    - ``assigned_scope`` (str): ``"unassigned"`` | ``"me"`` | ``"any"``.
      Who may own ``data.assigned_to`` for rows in this bucket.
    - ``fallback_assigned_scope`` (str): If set, the pipeline tries the primary
      ``assigned_scope`` first, then this scope (e.g. ``"me"`` then ``"unassigned"``
      for follow-up callbacks).
    - ``lead_stage`` (list[str]): Uppercase stage names matched against
      ``UPPER(data->>'lead_stage')``.
    - ``call_attempts`` (dict): Range on ``COALESCE((data->>'call_attempts')::int, 0)``.
      Supported keys: ``lte``, ``gte``, ``lt``, ``gt`` (int).
    - ``next_call_due`` (bool): If true, require ``next_call_at`` set and ``<= NOW()``
      (and attempts ``< 6`` when combined with snoozed-style buckets).
    - ``apply_routing_rule`` (bool, default True): Apply ``apply_routing_rule_to_queryset``
      for this RM when ``user_uuid`` is set.
    - ``daily_limit_applies`` (bool): If true, this bucket is skipped when the user has
      hit their daily pull limit (fresh pool only in typical setups).
    - ``exclude_other_assignees`` (bool, default True when ``assigned_scope`` is
      ``"unassigned"``): Exclude rows clearly assigned to another user (legacy pool safety).
    """

    name = models.CharField(max_length=100)
    slug = models.SlugField()
    description = models.TextField(blank=True)

    # What leads belong in this bucket (interpreted by the pipeline services).
    filter_conditions = models.JSONField(default=dict)

    is_system = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)

    class Meta:
        unique_together = [("tenant", "slug")]
        indexes = [models.Index(fields=["tenant", "slug"]), models.Index(fields=["tenant", "is_active"])]

    def __str__(self):
        return f"{self.tenant_id}:{self.slug}"


class UserBucketAssignment(HistoryTrackedModel, BaseModel):
    """
    Assigns a priority-ordered pull bucket.

    - **Tenant-wide** (``user`` is NULL): same bucket order/strategy for every RM in the tenant.
    - **Per-user** (``user`` set): optional overrides for a specific ``TenantMembership`` (legacy).
    """

    user = models.ForeignKey(
        "authz.TenantMembership",
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        help_text="If null, this row applies to all RMs in the tenant.",
    )
    bucket = models.ForeignKey(Bucket, on_delete=models.CASCADE)

    priority = models.IntegerField(default=100)
    pull_strategy = models.JSONField(default=dict)

    is_active = models.BooleanField(default=True)

    class Meta:
        ordering = ["priority"]
        indexes = [models.Index(fields=["tenant", "user"]), models.Index(fields=["tenant", "bucket", "priority"])]
        constraints = [
            models.UniqueConstraint(
                fields=["tenant", "bucket"],
                condition=Q(user__isnull=True),
                name="crm_records_uba_tenant_bucket_tenant_default_uniq",
            ),
            models.UniqueConstraint(
                fields=["tenant", "user", "bucket"],
                condition=Q(user__isnull=False),
                name="crm_records_uba_tenant_user_bucket_uniq",
            ),
        ]

    def __str__(self):
        who = f"user={self.user_id}" if self.user_id else "tenant-wide"
        return f"{self.tenant_id}:{who}->{self.bucket_id}({self.priority})"

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        # Keep in sync with `crm_records.lead_pipeline.bucket_resolver.BucketResolver`.
        cache.delete(f"bucket_assignments_tenant:{self.tenant_id}:v4")
        if self.user_id:
            cache.delete(f"bucket_assignments:{self.tenant_id}:{self.user_id}:v2")

    def delete(self, *args, **kwargs):
        tid, uid = self.tenant_id, self.user_id
        super().delete(*args, **kwargs)
        cache.delete(f"bucket_assignments_tenant:{tid}:v4")
        if uid:
            cache.delete(f"bucket_assignments:{tid}:{uid}:v2")