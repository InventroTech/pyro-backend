import factory
import uuid
from support_ticket.models import SupportTicket
from datetime import datetime, timedelta

class SupportTicketFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = SupportTicket
        django_get_or_create = ("id",)

    id = factory.Sequence(lambda n: n + 1)
    created_at = factory.LazyFunction(lambda: datetime.now() - timedelta(days=3))
    ticket_date = factory.LazyFunction(datetime.now)
    user_id = factory.Faker("uuid4")
    name = factory.Faker("name")
    phone = factory.Faker("phone_number")
    source = factory.Iterator(["email", "web", "call"])
    subscription_status = factory.LazyFunction(lambda: None)
    atleast_paid_once = factory.LazyFunction(lambda: False)
    reason = factory.LazyFunction(lambda: None)
    other_reasons = factory.LazyFunction(list)  # returns []
    badge = factory.LazyFunction(lambda: None)
    poster = factory.LazyFunction(lambda: None)
    tenant_id = factory.Faker("uuid4")
    assigned_to = factory.LazyFunction(lambda: None)
    layout_status = factory.LazyFunction(lambda: None)
    resolution_status = factory.LazyFunction(lambda: "Resolved")
    resolution_time = factory.LazyFunction(lambda: None)
    cse_name = factory.LazyFunction(lambda: None)
    cse_remarks = factory.LazyFunction(lambda: None)
    call_status = factory.LazyFunction(lambda: None)
    call_attempts = factory.LazyFunction(lambda: None)
    rm_name = factory.LazyFunction(lambda: None)
    completed_at = factory.LazyFunction(lambda: datetime.now() - timedelta(days=1))
    snooze_until = factory.LazyFunction(lambda: None)
    praja_dashboard_user_link = factory.LazyFunction(lambda: None)
    display_pic_url = factory.LazyFunction(lambda: None)
    dumped_at = factory.LazyFunction(lambda: datetime.now() - timedelta(days=3))
