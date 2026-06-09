from support_ticket.records import build_ticket_task_progress, record_to_ticket_dict
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_dump_factory import dump_data
from crm_records.models import Record
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE


class TicketTaskProgressTest(BaseAPITestCase):
    def test_build_ticket_task_progress_marks_first_pending_as_current(self):
        raw = [
            {"task": "Sending a Demo", "status": "Yes"},
            {"task": "App Installation", "status": "No"},
            {"task": "Create/Update Layout", "status": "Null"},
        ]
        steps = build_ticket_task_progress(raw)
        self.assertEqual(len(steps), 3)
        self.assertEqual(steps[0]["status"], "completed")
        self.assertEqual(steps[1]["status"], "current")
        self.assertEqual(steps[2]["status"], "pending")

    def test_record_to_ticket_dict_includes_task_progress(self):
        record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={
                **dump_data(user_id="u1", name="User"),
                "tasks": [
                    {"task": "Sending a Demo", "status": "Yes"},
                    {"task": "App Installation", "status": "No"},
                ],
            },
        )
        payload = record_to_ticket_dict(record)
        self.assertEqual(len(payload["tasks"]), 2)
        self.assertEqual(payload["task_progress"][0]["status"], "completed")
        self.assertEqual(payload["task_progress"][1]["status"], "current")

    def test_empty_tasks_yields_empty_progress(self):
        self.assertEqual(build_ticket_task_progress(None), [])
        self.assertEqual(build_ticket_task_progress([]), [])
