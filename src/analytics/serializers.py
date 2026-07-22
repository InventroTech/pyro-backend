from rest_framework import serializers

from support_ticket.records import record_to_ticket_dict


class SupportTicketSerializer(serializers.Serializer):
    """Serialize support ticket ``Record`` rows (or pre-built dicts)."""

    def to_representation(self, instance):
        if isinstance(instance, dict):
            data = instance
        else:
            data = record_to_ticket_dict(instance)

        if data.get("resolution_status"):
            data["resolution_status"] = data["resolution_status"].upper()

        return data


# Analytics serializers
class TeamOverviewSerializer(serializers.Serializer):
    """Serializer for team overview metrics."""
    attendance = serializers.IntegerField()
    total_team_size = serializers.IntegerField()
    calls_made = serializers.IntegerField()
    trials_activated = serializers.IntegerField()
    connected_to_trial_ratio = serializers.FloatField(allow_null=True)
    average_time_spent_seconds = serializers.FloatField(allow_null=True)
    trail_target = serializers.IntegerField()
    allotted_leads = serializers.IntegerField()
    unassigned_leads = serializers.IntegerField()


class MemberBreakdownSerializer(serializers.Serializer):
    """Serializer for per-member metrics breakdown."""
    user_id = serializers.CharField()
    email = serializers.EmailField()
    daily_target = serializers.IntegerField()
    attendance = serializers.IntegerField()
    total_events = serializers.IntegerField()
    calls_made = serializers.IntegerField()
    calls_connected = serializers.IntegerField()
    trials_activated = serializers.IntegerField()
    connected_to_trial_ratio = serializers.FloatField(allow_null=True)
    get_next_lead_count = serializers.IntegerField()
    take_break_count = serializers.IntegerField()
    not_interested_count = serializers.IntegerField()
    average_time_spent_seconds = serializers.FloatField()


class EventBreakdownSerializer(serializers.Serializer):
    """Serializer for event type breakdown."""
    event_type = serializers.CharField()
    count = serializers.IntegerField()


class TimeSeriesSerializer(serializers.Serializer):
    """Serializer for daily time series data."""
    date = serializers.DateField()
    attendance = serializers.IntegerField()
    calls_made = serializers.IntegerField()
    trials_activated = serializers.IntegerField()
    total_events = serializers.IntegerField()


class CseOverviewSerializer(serializers.Serializer):
    open_call_back = serializers.IntegerField()
    open_not_connected = serializers.IntegerField()
    leads_assigned = serializers.IntegerField()
    resolved = serializers.IntegerField()
    not_connected = serializers.IntegerField()
    call_later = serializers.IntegerField()
    cant_resolve = serializers.IntegerField()
    resolve_rate = serializers.FloatField(allow_null=True)
    average_handling_time_seconds = serializers.FloatField(allow_null=True)
    handling_time_ticket_count = serializers.IntegerField()


class CseMemberBreakdownSerializer(serializers.Serializer):
    cse_name = serializers.CharField(allow_blank=True)
    manager_i_name = serializers.CharField(allow_blank=True)
    open_call_back = serializers.IntegerField()
    open_not_connected = serializers.IntegerField()
    leads_assigned = serializers.IntegerField()
    resolved = serializers.IntegerField()
    resolve_rate = serializers.FloatField(allow_null=True)
    average_handling_time_seconds = serializers.FloatField(allow_null=True)
    handling_time_ticket_count = serializers.IntegerField()


class CseFilterOptionsSerializer(serializers.Serializer):
    ticket_types = serializers.ListField(child=serializers.CharField())
    cse_names = serializers.ListField(child=serializers.CharField())
    handling_time_statuses = serializers.ListField(child=serializers.CharField())
    attributes = serializers.ListField(child=serializers.DictField(), required=False)
    visibility_scope = serializers.CharField()


class CseTimeSeriesSerializer(serializers.Serializer):
    date = serializers.CharField()
    assigned = serializers.IntegerField()
    resolved = serializers.IntegerField()
    not_connected = serializers.IntegerField()
    call_later = serializers.IntegerField()
    resolve_rate = serializers.FloatField(allow_null=True)
    average_handling_time_seconds = serializers.FloatField(allow_null=True)
    handling_time_ticket_count = serializers.IntegerField()
    stacked_resolved = serializers.IntegerField()
    stacked_unresolved = serializers.IntegerField()


class AnalyticsBoardSerializer(serializers.Serializer):
    """Validates a single saved analytics board (one report card)."""

    board_type = serializers.CharField(max_length=64, required=False, default="cse")
    config = serializers.DictField()


class RmFilterOptionsSerializer(serializers.Serializer):
    attributes = serializers.ListField(child=serializers.DictField(), required=False)
    visibility_scope = serializers.CharField()


class RmOverviewSerializer(serializers.Serializer):
    attendance = serializers.IntegerField()
    calls_made = serializers.IntegerField()
    calls_connected = serializers.IntegerField()
    trials_activated = serializers.IntegerField()
    connected_to_trial_ratio = serializers.FloatField(allow_null=True)
    average_time_spent_seconds = serializers.FloatField(allow_null=True)
    handling_time_volume = serializers.IntegerField()
    take_break_count = serializers.IntegerField()
    not_interested_count = serializers.IntegerField()
    allotted_leads = serializers.IntegerField()
    unassigned_leads = serializers.IntegerField()


class RmMemberBreakdownSerializer(serializers.Serializer):
    rm_name = serializers.CharField(allow_blank=True)
    manager_i_name = serializers.CharField(allow_blank=True)
    user_id = serializers.CharField()
    attendance = serializers.IntegerField()
    calls_made = serializers.IntegerField()
    calls_connected = serializers.IntegerField()
    trials_activated = serializers.IntegerField()
    connected_to_trial_ratio = serializers.FloatField(allow_null=True)
    average_time_spent_seconds = serializers.FloatField(allow_null=True)
    handling_time_volume = serializers.IntegerField()
    take_break_count = serializers.IntegerField()
    not_interested_count = serializers.IntegerField()
    allotted_leads = serializers.IntegerField()


class RmTimeSeriesSerializer(serializers.Serializer):
    date = serializers.CharField()
    attendance = serializers.IntegerField()
    calls_made = serializers.IntegerField()
    calls_connected = serializers.IntegerField()
    trials_activated = serializers.IntegerField()
    connected_to_trial_ratio = serializers.FloatField(allow_null=True)
    average_time_spent_seconds = serializers.FloatField(allow_null=True)
    handling_time_volume = serializers.IntegerField()
    take_break_count = serializers.IntegerField()
    not_interested_count = serializers.IntegerField()