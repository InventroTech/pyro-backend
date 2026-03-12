import factory
from faker import Faker as RawFaker
from crm_records.models import Record, EventLog, EntityTypeSchema, ApiSecretKey, PartnerEvent
from background_jobs.models import BackgroundJob, JobType, JobStatus

_fake = RawFaker()


class RecordFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Record

    tenant = factory.SubFactory("tests.factories.core_factory.TenantFactory")
    entity_type = "lead"
    data = factory.LazyFunction(lambda: {
        "name": _fake.name(),
        "phone": _fake.numerify("##########"),
        "email": _fake.email(),
        "lead_stage": "FRESH",
    })
    pyro_data = factory.LazyFunction(dict)


class EventLogFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = EventLog

    tenant = factory.SubFactory("tests.factories.core_factory.TenantFactory")
    record = factory.SubFactory(RecordFactory, tenant=factory.SelfAttribute("..tenant"))
    event = factory.Iterator(["created", "updated", "stage_changed", "assigned"])
    payload = factory.LazyFunction(dict)


class EntityTypeSchemaFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = EntityTypeSchema
        django_get_or_create = ("tenant", "entity_type")

    tenant = factory.SubFactory("tests.factories.core_factory.TenantFactory")
    entity_type = "lead"
    attributes = factory.LazyFunction(lambda: ["id", "name", "data.email", "data.phone", "data.lead_stage"])
    rules = factory.LazyFunction(list)
    description = "Default lead schema"


class PartnerEventFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = PartnerEvent

    tenant = factory.SubFactory("tests.factories.core_factory.TenantFactory")
    partner_slug = "halocom"
    event = "work_on_lead"
    payload = factory.LazyFunction(lambda: {
        "event": "work_on_lead",
        "praja_id": f"PRAJA-{_fake.random_int(100, 9999)}",
        "email_id": _fake.email(),
        "partner_slug": "halocom",
    })
    status = "pending"
    record = factory.SubFactory(RecordFactory, tenant=factory.SelfAttribute("..tenant"))
    job_id = None
    processed_at = None
    error_message = None


class BackgroundJobFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = BackgroundJob

    tenant = factory.SubFactory("tests.factories.core_factory.TenantFactory")
    job_type = JobType.PARTNER_LEAD_ASSIGN
    status = JobStatus.PENDING
    priority = 5
    payload = factory.LazyFunction(dict)
    attempts = 0
    max_attempts = 3


class ApiSecretKeyFactory(factory.django.DjangoModelFactory):
    """
    Creates an ApiSecretKey with a pre-computed bcrypt-style hash.
    For tests that don't need real bcrypt verification, this avoids
    the pgcrypto dependency. Use set_raw_secret() in integration tests
    that need actual hash verification.
    """

    class Meta:
        model = ApiSecretKey

    tenant = factory.SubFactory("tests.factories.core_factory.TenantFactory")
    secret_key_hash = factory.Sequence(lambda n: f"$2b$12$fakehash{n:040d}")
    secret_key_last4 = factory.Sequence(lambda n: f"{n:04d}"[-4:])
    description = factory.Faker("sentence", nb_words=4)
    is_active = True
    last_used_at = None
