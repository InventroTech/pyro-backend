import factory
from crm.models import Lead
from datetime import date


class LeadFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Lead

    tenant = factory.SubFactory("tests.factories.core_factory.TenantFactory")
    name = factory.Faker("name")
    phone_no = factory.Sequence(lambda n: f"90000{n:05d}")
    assigned_to = factory.SubFactory(
        "tests.factories.user_factory.UserFactory",
        tenant_id=factory.SelfAttribute("..tenant.id"),
    )
    lead_status = "new"
    lead_description = factory.Faker("text", max_nb_chars=200)
    other_description = factory.Faker("text", max_nb_chars=200)
    badge = factory.Iterator(["Hot", "Warm", "Cold", None])
    lead_creation_date = factory.LazyFunction(date.today)
