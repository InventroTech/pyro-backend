from django.contrib.contenttypes.models import ContentType
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.utils import timezone
from .serializers import ScheduleCallRequest, RecordOutcomeRequest, ScheduledTaskSerializer
from .services import schedule_call_for, record_call_outcome
from .models import ScheduledTask

class ScheduleCallView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        s = ScheduleCallRequest(data=request.data)
        s.is_valid(raise_exception=True)
        v = s.validated_data

        ct = ContentType.objects.get(app_label=v["app_label"], model=v["model"].lower())
        obj = ct.get_object_for_this_type(pk=v["object_id"])
        task = schedule_call_for(obj, v["policy_key"], v.get("due_at"), v.get("payload"))
        if task is None:
            return Response({"message": "Call already scheduled or in progress."}, status=200)
        return Response(ScheduledTaskSerializer(task).data, status=201)

class RecordOutcomeView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, task_id: int):
        s = RecordOutcomeRequest(data=request.data)
        s.is_valid(raise_exception=True)
        verdict, next_due = record_call_outcome(task_id, s.validated_data["outcome"], s.validated_data.get("notes",""))
        return Response({"status": verdict, "next_due_at": next_due}, status=200)
