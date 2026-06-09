from datetime import datetime, time

from django.db.models import Count, Q
from django.utils import timezone

from crm_records.models import Record
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from support_ticket.records import (
    annotate_ticket_datetimes,
    q_record_pending_resolution,
    q_record_unassigned,
    support_ticket_records_qs,
)
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_dump_factory import dump_data


class AnnotateTicketDatetimesTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        Record.objects.filter(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
        ).delete()

    def test_annotate_ticket_datetimes_aggregate_does_not_error(self):
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="user-1",
                name="Test User",
                completed_at=timezone.now().isoformat(),
            ),
        )
        today = timezone.now().date()
        start = timezone.make_aware(datetime.combine(today, time.min))
        end = timezone.make_aware(datetime.combine(today, time.max))
        qs = annotate_ticket_datetimes(support_ticket_records_qs(tenant=self.tenant))
        agg = qs.aggregate(
            resolved=Count(
                "id",
                filter=Q(
                    ticket_completed_at__gte=start,
                    ticket_completed_at__lte=end,
                ),
            ),
        )
        self.assertGreaterEqual(agg["resolved"], 0)

    def test_pending_aggregate_filter_combines_q_objects_with_and(self):
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(user_id="user-pending", name="Pending User"),
        )
        qs = annotate_ticket_datetimes(support_ticket_records_qs(tenant=self.tenant))
        agg = qs.aggregate(
            pending=Count(
                "id",
                filter=q_record_pending_resolution() & q_record_unassigned(),
            ),
        )
        self.assertEqual(agg["pending"], 1)
