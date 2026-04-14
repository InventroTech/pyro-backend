from rest_framework import serializers
from support_ticket.models import SupportTicket

class SupportTicketSerializer(serializers.ModelSerializer):
    class Meta:
        model = SupportTicket
        fields = [
            "id","created_at","ticket_date","user_id","name","phone","source",
            "subscription_status","atleast_paid_once","reason","other_reasons",
            "badge","poster","tenant_id","assigned_to","layout_status","state",
            "resolution_status","resolution_time","cse_name","cse_remarks",
            "call_status","call_attempts","rm_name","completed_at","snooze_until",
            "praja_dashboard_user_link","display_pic_url","dumped_at","review_requested"
        ]


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