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