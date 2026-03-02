import factory
from datetime import timedelta
from django.utils import timezone
from support_ticket.models import SupportTicket


class SupportTicketFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = SupportTicket

    created_at = factory.LazyFunction(lambda: timezone.now() - timedelta(days=3))
    ticket_date = factory.LazyFunction(timezone.now)

    user_id = factory.Faker("uuid4")
    name = factory.Faker("name")
    phone = factory.Faker("numerify", text="##########")
    source = factory.Iterator(["email", "web", "call"])

    subscription_status = factory.Iterator(["active", "inactive", "trial"])
    atleast_paid_once = False

    reason = factory.Faker("sentence", nb_words=6)
    other_reasons = factory.LazyFunction(list)

    badge = factory.Iterator(["premium", "standard", "basic", None])
    poster = factory.Iterator(["support_agent", "customer", "system"])

    tenant = factory.SubFactory("tests.factories.core_factory.TenantFactory")
    assigned_to = factory.SubFactory("tests.factories.user_factory.SupabaseAuthUserFactory")

    layout_status = factory.Iterator(["pending", "in_progress", "completed"])
    state = None
    resolution_status = "Resolved"
    resolution_time = None

    cse_name = factory.Faker("name")
    cse_remarks = None
    call_status = None
    call_attempts = 0

    rm_name = None
    completed_at = factory.LazyFunction(lambda: timezone.now() - timedelta(days=1))
    snooze_until = None
    praja_dashboard_user_link = None
    display_pic_url = None
    dumped_at = factory.LazyFunction(lambda: timezone.now() - timedelta(days=3))
    review_requested = None


class UnassignedSupportTicketFactory(SupportTicketFactory):
    """Ticket with no CSE assigned — useful for routing/assignment tests."""
    assigned_to = None
    cse_name = None
    resolution_status = None
    completed_at = None


class SnoozedSupportTicketFactory(SupportTicketFactory):
    """Ticket snoozed until a future time."""
    snooze_until = factory.LazyFunction(lambda: timezone.now() + timedelta(hours=2))
    resolution_status = None
    completed_at = None
