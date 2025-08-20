from rest_framework import serializers
from .models import ScheduledTask, AttemptLog, AttemptOutcome

class ScheduleCallRequest(serializers.Serializer):
    app_label = serializers.CharField()
    model = serializers.CharField()
    object_id = serializers.CharField()
    policy_key = serializers.CharField()
    due_at = serializers.DateTimeField(required=False)
    payload = serializers.JSONField(required=False)

class RecordOutcomeRequest(serializers.Serializer):
    outcome = serializers.ChoiceField(choices=AttemptOutcome.choices)
    notes = serializers.CharField(required=False, allow_blank=True)

class ScheduledTaskSerializer(serializers.ModelSerializer):
    class Meta:
        model = ScheduledTask
        fields = "__all__"
