import factory
import uuid
from datetime import datetime, timedelta
from django.utils import timezone

from support_ticket.models import SupportTicketDump


class SupportTicketDumpFactory(factory.django.DjangoModelFactory):
    """Factory for creating SupportTicketDump instances for testing"""
    
    class Meta:
        model = SupportTicketDump
    
    # Required fields
    tenant_id = factory.LazyFunction(uuid.uuid4)
    
    # Optional fields with realistic defaults
    ticket_date = factory.LazyFunction(lambda: timezone.now() - timedelta(hours=1))
    user_id = factory.Sequence(lambda n: f"user_{n}")
    name = factory.Faker('name')
    phone = factory.Faker('phone_number')
    source = factory.Faker('random_element', elements=('mobile_app', 'web', 'phone', 'email'))
    subscription_status = factory.Faker('random_element', elements=('active', 'inactive', 'trial', 'expired'))
    atleast_paid_once = factory.Faker('boolean')
    reason = factory.Faker('sentence', nb_words=10)
    badge = factory.Faker('random_element', elements=('premium', 'standard', 'basic', 'vip'))
    poster = factory.Faker('random_element', elements=('support_agent', 'customer', 'system'))
    layout_status = factory.Faker('random_element', elements=('pending', 'in_progress', 'completed'))
    praja_dashboard_user_link = factory.LazyAttribute(
        lambda obj: f"https://www.thecircleapp.in/admin/users/{factory.Faker('uuid4').generate()}"
    )
    display_pic_url = factory.Faker('image_url')
    is_processed = False
    
    # Timestamps
    created_at = factory.LazyFunction(timezone.now)


class ProcessedSupportTicketDumpFactory(SupportTicketDumpFactory):
    """Factory for creating processed SupportTicketDump instances"""
    is_processed = True


class MinimalSupportTicketDumpFactory(SupportTicketDumpFactory):
    """Factory for creating minimal SupportTicketDump instances with only required fields"""
    
    # Clear all optional fields
    ticket_date = None
    user_id = None
    name = None
    phone = None
    source = None
    subscription_status = None
    atleast_paid_once = None
    reason = None
    badge = None
    poster = None
    layout_status = None
    praja_dashboard_user_link = None
    display_pic_url = None
