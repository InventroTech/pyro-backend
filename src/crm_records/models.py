from django.db import models
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from core.models import BaseModel
from object_history.models import HistoryTrackedModel
from django.db import connection


class Record(HistoryTrackedModel, BaseModel):
    """
    Universal record model that can hold any tenant's data dynamically using JSONB.
    All future entities (leads, tickets, job applications, etc.) will be built on top of this.
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
            # See migration file for: lead_stage, assigned_to, affiliated_party, praja_id, next_call_at
        ]

    def __str__(self):
        name = (self.data or {}).get('name', '') if isinstance(self.data, dict) else ''
        return f"{self.entity_type}: {name or 'Unnamed'}"


class EventLog(BaseModel):
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


class RuleSet(BaseModel):
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


class RuleExecutionLog(BaseModel):
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


class EntityTypeSchema(BaseModel):
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


class ApiSecretKey(BaseModel):
    """
    Model to store API secret keys and their associated tenants.
    Allows dynamic mapping of secret keys to tenants without requiring code changes.
    Each secret key maps to a specific tenant for external API access.
    """
    # We store only a one-way bcrypt hash of the secret (never plaintext) for security.
    # Hash format: pgcrypto crypt(raw_secret, gen_salt('bf', ...)) (salt embedded in the hash).
    secret_key_hash = models.CharField(
        max_length=128,
        db_index=True,
        help_text="bcrypt hash of the raw secret (pgcrypto crypt). Do NOT store plaintext secrets."
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
            models.Index(fields=["secret_key_hash", "is_active"]),
            models.Index(fields=["tenant", "is_active"]),
        ]
        verbose_name = "API Secret Key"
        verbose_name_plural = "API Secret Keys"
    
    def set_raw_secret(self, raw_secret: str) -> None:
        raw_secret = (raw_secret or "").strip()
        if not raw_secret:
            raise ValueError("raw_secret cannot be empty")
        # Compute bcrypt hash using Postgres pgcrypto (keeps format aligned with DB inserts)
        with connection.cursor() as cursor:
            cursor.execute("SELECT crypt(%s, gen_salt('bf', 12))", [raw_secret])
            self.secret_key_hash = cursor.fetchone()[0]
        self.secret_key_last4 = raw_secret[-4:] if len(raw_secret) >= 4 else raw_secret

    def __str__(self):
        last4 = self.secret_key_last4 or "????"
        return f"****{last4} -> {self.tenant.slug if self.tenant else 'No Tenant'}"
