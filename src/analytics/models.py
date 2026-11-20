import uuid
from django.contrib.postgres.indexes import GinIndex
from django.db import models

from core.models import BaseModel, TenantModel
from core.soft_delete import alive_q


class AnalyticsBoard(BaseModel):
    """
    A single saved analytics board (one report card), shared by role.

    Boards are scoped by ``(tenant, role, board_type)`` so everyone with the same
    role shares the same set (e.g. all GMs see the same boards), while different
    roles (CSE, ASM, ...) keep their own. Each row is one board/report, so
    creating a board inserts a row and deleting a board removes its row.
    ``config`` stores that board's definition (title, chart type, breakdown,
    metrics and filters) as an opaque JSON object owned by the frontend.
    ``report_id`` is the frontend-generated identifier; ``user_id`` records who
    created it (informational only, not used for scoping).
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    role = models.CharField(
        max_length=64,
        default="",
        db_index=True,
        help_text="Role key that shares this board, e.g. 'GM', 'ASM', 'CSE'.",
    )
    user_id = models.CharField(
        max_length=128,
        blank=True,
        default="",
        db_index=True,
        help_text="User who created the board (informational only).",
    )
    board_type = models.CharField(
        max_length=64,
        default="cse",
        db_index=True,
        help_text="Analytics type this board belongs to, e.g. 'cse', 'rm'.",
    )
    report_id = models.CharField(
        max_length=128,
        default="",
        db_index=True,
        help_text="Frontend-generated identifier for this board/report.",
    )
    config = models.JSONField(
        default=dict,
        blank=True,
        help_text="This board's definition (title, chart type, metrics, filters).",
    )

    class Meta(BaseModel.Meta):
        db_table = "analytics_boards"
        constraints = [
            models.UniqueConstraint(
                fields=["tenant", "role", "board_type", "report_id"],
                condition=alive_q(),
                name="analytics_board_tenant_role_type_report_uniq_alive",
            ),
        ]
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["tenant", "role", "board_type"]),
        ]

    def __str__(self):
        return (
            f"AnalyticsBoard({self.tenant_id}:{self.role}:"
            f"{self.board_type}:{self.report_id})"
        )


from object_history.models import HistoryTrackedModel


class AnalyticsRunCore(HistoryTrackedModel, BaseModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    completed_at = models.DateTimeField(null=True, blank=True)
    user_id = models.CharField(max_length=128, db_index=True)

    question = models.TextField()
    sql_query = models.TextField(null=True, blank=True)
    validation_ok = models.BooleanField(default=False)
    validation_reason = models.TextField(null=True, blank=True)
    execution_ok = models.BooleanField(default=False)
    final_result = models.JSONField(null=True, blank=True)
    status = models.CharField(max_length=32, default="started", db_index=True)

    error_summary = models.TextField(null=True, blank=True)
    rows_returned = models.IntegerField(null=True, blank=True)

    class Meta:
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["created_at"]),
            models.Index(fields=["user_id", "created_at"]),
            models.Index(fields=["status"]),
        ]


class RmActivityEvent(BaseModel):
    """
    One row per real thing that happened to an RM while working leads — a
    call touch finishing, logging in/out, starting or ending a break. This is
    the only table the RM PRD analytics dashboard reads from: every card and
    "by RM" row is just this table grouped and summed in different ways.

    Shaped like crm_records.Record: a couple of real columns (tenant,
    event_type) plus one JSONB event_data blob holding everything else,
    instead of a dedicated column per field. event_data keys:
    rm_user_id, rm_name, manager_name, team, state, lead_record_id,
    praja_id, updated_status, lead_group, party, started_at, ended_at,
    duration_seconds.

    Only the keys relevant to CALL_TOUCH rows (lead_record_id, praja_id,
    updated_status, lead_group, party) are filled in for that event_type — they're absent
    for LOGIN/LOGOUT/BREAK_START/BREAK_END rows, which is expected, not a
    data-quality problem.

    rm_name/manager_name are copied in at write time (instead of always
    joining back to the membership table) so a dashboard query never needs
    anything besides this one table.
    """

    EVENT_TYPE_CHOICES = [
        ("CALL_TOUCH", "Call touch"),
        ("LOGIN", "Login"),
        ("LOGOUT", "Logout"),
        ("BREAK_START", "Break start"),
        ("BREAK_END", "Break end"),
    ]
    UPDATED_STATUS_CHOICES = [
        ("NOT_CONNECTED", "Not connected"),
        ("CALL_BACK", "Call back"),
        ("NOT_INTERESTED", "Not interested"),
        ("TRIAL_ACTIVATED", "Trial activated"),
    ]

    event_type = models.CharField(max_length=20, choices=EVENT_TYPE_CHOICES, db_index=True)
    event_data = models.JSONField(default=dict, blank=True)

    class Meta(BaseModel.Meta):
        db_table = "rm_activity_events"
        # RmActivityEventListView's date-range/rm_user_id filters are served by
        # two expression indexes on event_data, added via raw SQL (CONCURRENTLY)
        # in migration 0019 — not declared here, same as this codebase's other
        # JSONB query indexes (see crm_records' records_support_ticket_*_idx /
        # event_logs_tenant_*_idx), since Django can't build them without
        # locking the table and a declared Index always does a plain CREATE INDEX.
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["tenant", "event_type"], name="rm_events_tenant_type_idx"),
            GinIndex(fields=["event_data"], name="rm_events_data_gin_idx"),
        ]

    def __str__(self):
        data = self.event_data or {}
        return f"RmActivityEvent({data.get('rm_name')}:{self.event_type}:{data.get('started_at')})"


class RmCallSession(BaseModel):
    """
    Marks the real instant a lead became an RM's current lead card —
    written synchronously at that moment (see rm_activity.record_lead_opened),
    so it survives the browser tab being backgrounded or reclaimed for the
    length of the actual phone call (dialing hands off to the native Phone
    app; the RM PRD dashboard previously measured "time on lead" with a
    client-side JS clock that silently reset whenever that happened,
    truncating real call durations down to a few seconds).

    record_lead_touch_event reads the matching row back (by tenant +
    rm_user_id + lead_record) when the RM logs a disposition, uses it as the
    authoritative start time instead of the client-supplied duration, and
    deletes it — so a row only exists between "RM opened the lead" and "RM
    logged the outcome" for that lead.
    """

    rm_user_id = models.UUIDField(db_index=True, help_text="Supabase user id of the RM who opened this lead.")
    lead_record = models.ForeignKey(
        "crm_records.Record", on_delete=models.CASCADE, related_name="rm_call_sessions"
    )
    started_at = models.DateTimeField()
    opened_from = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text="Page/carousel title the RM opened this lead from (e.g. 'All Leads', 'Pending Leads') — copied onto the CALL_TOUCH row's event_data when the disposition is logged.",
    )

    class Meta(BaseModel.Meta):
        db_table = "rm_call_sessions"
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["tenant", "rm_user_id", "lead_record"], name="rm_call_sessions_lookup_idx"),
        ]

    def __str__(self):
        return f"RmCallSession(rm={self.rm_user_id}:lead={self.lead_record_id}:started={self.started_at})"


class RmPerformanceDailySummary(TenantModel):
    """
    One pre-aggregated row per (tenant, rm_user_id, date, state, party,
    lead_group) — touches, unique leads, latest-disposition counts —
    computed once by a scheduled refresh job (see
    analytics.management.commands.refresh_rm_performance_summary) instead
    of live, per-request, over raw rm_activity_events rows.

    Why this exists: a busy tenant logs 15-20k+ CALL_TOUCH rows a day — a
    30-day Performance view means summing ~640k raw rows. Doing that live,
    per request, uncached is what caused a real production incident: CPU/
    memory climbed and p90 response times hit ~28s the first morning real
    concurrent traffic hit it. Reading pre-summed day-buckets here answers
    any date range by summing a handful of rows instead of hundreds of
    thousands. This table is now the base layer for the RM Leaderboard
    (see RmLeaderboardSnapshot) — it is NOT currently wired back into the
    live Performance tab, which still reads raw events; that decision is
    separate from this table's existence.

    `date` is the IST calendar day the touches happened on (see
    get_utc_datetime_range_for_ist_date), not a UTC day. The original
    live endpoint this replaced used UTC-day-aligned bounds the frontend
    sent without ever trimming them back down, which silently queried up
    to ~2x the intended window for IST users and produced wrong numbers.
    Storing one row per real local day and summing whichever days fall in
    a range sidesteps that whole bug class — same precedent as
    useRmDailyTargets/toLocalDateParam on the frontend.

    state/party/lead_group are part of the uniqueness key rather than
    folded into one row per RM per day, because the dashboard's filter bar
    can slice by any of them. This is still a tiny fraction of raw touch
    volume in practice: one row per (RM, day, combination actually
    worked), not one row per touch. manager_name (free text, not a
    resolved hierarchy id) matches the raw events' own field and the
    filter bar's "Manager" dropdown.
    """

    rm_user_id = models.CharField(max_length=64, db_index=True)
    rm_name = models.CharField(max_length=255, blank=True, default="")
    manager_name = models.CharField(max_length=255, blank=True, default="")
    state = models.CharField(max_length=64, blank=True, default="")
    party = models.CharField(max_length=128, blank=True, default="")
    lead_group = models.CharField(max_length=128, blank=True, default="")
    date = models.DateField()
    touches = models.PositiveIntegerField(default=0)
    unique_leads = models.PositiveIntegerField(default=0)
    # Per-disposition counts for this bucket's leads whose LATEST touch
    # (within this local day) landed on each status, e.g.
    # {"TRIAL_ACTIVATED": 3, "CALL_BACK": 7}.
    disposition_counts = models.JSONField(default=dict, blank=True)
    # Bumped every time the refresh job upserts this row.
    computed_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "rm_performance_daily_summary"
        constraints = [
            models.UniqueConstraint(
                fields=["tenant", "rm_user_id", "date", "state", "party", "lead_group"],
                name="rm_perf_summary_unique_bucket",
            )
        ]
        indexes = [
            # Whole-team / whole-tenant date-range reads — the unique
            # constraint's own index leads with rm_user_id, which doesn't
            # help a query that spans every RM, so this is not redundant.
            models.Index(fields=["tenant", "date"], name="rm_perf_sum_tenant_date_idx"),
        ]

    def __str__(self):
        return f"RmPerformanceDailySummary(rm={self.rm_user_id}:date={self.date}:touches={self.touches})"


class RmPerformanceDailyLeadLatest(TenantModel):
    """
    One row per (tenant, date, lead_record_id) — whichever CALL_TOUCH was
    the LATEST for that lead on that local day, carrying just enough
    (rm_user_id, state, party, lead_group, updated_status) to answer a
    "team" total correctly for ANY subset of RMs — the whole tenant, or
    one manager's resolved team, or the leaderboard's own ranking set —
    not only the whole-tenant case.

    Why this is a separate table from RmPerformanceDailySummary: a lead
    reassigned between two RMs on the same day is correctly counted once
    in EACH RM's own per-RM bucket there (right for "this RM's own
    numbers"), but summing those buckets across RMs for a team total would
    double-count that lead. Storing one row per unique lead per day (not
    per RM, not per touch) means filtering this table to any RM subset and
    counting rows/grouping by updated_status is correct dedup for that
    subset, with no double-counting risk, because a lead can only ever
    appear in ONE row regardless of how many RMs touched it. "touches"
    (raw, not deduped) has no such ambiguity — it's always just the sum of
    RmPerformanceDailySummary.touches across RMs — so it's not duplicated
    here.
    """

    lead_record_id = models.BigIntegerField()
    rm_user_id = models.CharField(max_length=64, db_index=True)
    state = models.CharField(max_length=64, blank=True, default="")
    party = models.CharField(max_length=128, blank=True, default="")
    lead_group = models.CharField(max_length=128, blank=True, default="")
    date = models.DateField()
    updated_status = models.CharField(max_length=32, blank=True, default="")

    class Meta:
        db_table = "rm_performance_daily_lead_latest"
        constraints = [
            models.UniqueConstraint(
                fields=["tenant", "date", "lead_record_id"],
                name="rm_perf_lead_latest_unique",
            )
        ]
        indexes = [
            models.Index(fields=["tenant", "date", "rm_user_id"], name="rm_perf_lead_latest_rm_idx"),
        ]

    def __str__(self):
        return f"RmPerformanceDailyLeadLatest(lead={self.lead_record_id}:date={self.date}:rm={self.rm_user_id})"


class RmLeaderboardSnapshot(TenantModel):
    """
    One row per (tenant, rm_user_id, period_type, period_start) — the RM
    Leaderboard's ranked numbers for one calendar-aligned period, computed
    by analytics.management.commands.refresh_rm_leaderboard_snapshot from
    RmPerformanceDailySummary / RmPerformanceDailyLeadLatest. The read API
    never aggregates — it SELECTs these rows ordered by `rank`.

    period_type is one of 'day' | 'week' | 'month'. period_start is the
    IST-calendar-aligned anchor for that period: for 'day' it's that day;
    for 'week' it's that ISO week's Monday; for 'month' it's the 1st of
    that calendar month. Only the CURRENT period of each type ever needs
    recomputing on a given run — a completed week/month is final.

    `rank` is always a definite 1..N integer — even an RM with no target
    configured (score is NULL, see `score`) still gets ranked, just last.
    `score` is nullable (achieved/target*100, a percentage — NOT a ratio;
    NULL rather than a sentinel like -infinity, which isn't valid JSON) —
    the refresh job assigns `rank` with NULLS LAST once, at compute time,
    specifically so this table's own read-path index never has to deal
    with NULL ordering at all: `ORDER BY rank` is always a plain integer
    sort.

    Excludes inactive/deleted RMs: the refresh job only ranks RMs whose
    TenantMembership is currently active, regardless of what historical
    rows they have in RmPerformanceDailySummary.
    """

    PERIOD_DAY = "day"
    PERIOD_WEEK = "week"
    PERIOD_MONTH = "month"
    PERIOD_CHOICES = [
        (PERIOD_DAY, "Day"),
        (PERIOD_WEEK, "Week"),
        (PERIOD_MONTH, "Month"),
    ]

    rm_user_id = models.CharField(max_length=64, db_index=True)
    period_type = models.CharField(max_length=8, choices=PERIOD_CHOICES)
    period_start = models.DateField()
    touches = models.PositiveIntegerField(default=0)
    unique_leads = models.PositiveIntegerField(default=0)
    # "conversions" elsewhere in this codebase's RM PRD vocabulary — the
    # TRIAL_ACTIVATED count, same meaning as RmPerformanceRow.achieved.
    achieved = models.PositiveIntegerField(default=0)
    target = models.PositiveIntegerField(default=0)
    # achieved/target*100 as a percentage, NULL when target=0 (no target
    # configured — never a false 0%, and never -infinity, which `json`
    # can't serialize). See the class docstring for why `rank` is still
    # always a definite integer regardless.
    score = models.FloatField(null=True, blank=True)
    rank = models.PositiveIntegerField()
    computed_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "rm_leaderboard_snapshot"
        constraints = [
            models.UniqueConstraint(
                fields=["tenant", "rm_user_id", "period_type", "period_start"],
                name="rm_leaderboard_snapshot_unique",
            )
        ]
        indexes = [
            # The entire read path: "give me this tenant's this-period
            # board, ordered by rank" — a plain integer sort, nothing else
            # needed. New, empty table at creation time, so a normal
            # declarative index is just as lock-free as
            # CREATE INDEX CONCURRENTLY would be here; CONCURRENTLY only
            # earns its keep against an existing, populated, live table
            # (like rm_activity_events's own expression indexes).
            models.Index(fields=["tenant", "period_type", "period_start", "rank"], name="rm_leaderboard_rank_idx"),
        ]

    def __str__(self):
        return (
            f"RmLeaderboardSnapshot(rm={self.rm_user_id}:period={self.period_type}:"
            f"{self.period_start}:rank={self.rank})"
        )
