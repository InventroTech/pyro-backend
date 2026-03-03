import factory
from authz.models import Role, TenantMembership


class RoleFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Role
        django_get_or_create = ("tenant", "key")

    tenant = factory.SubFactory("tests.factories.core_factory.TenantFactory")
    key = factory.Sequence(lambda n: f"role-{n}")
    name = factory.Sequence(lambda n: f"Role {n}")
    description = ""


class TenantMembershipFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = TenantMembership

    tenant = factory.SubFactory("tests.factories.core_factory.TenantFactory")
    user_id = factory.Faker("uuid4")
    email = factory.Faker("email")
    role = factory.SubFactory(RoleFactory, tenant=factory.SelfAttribute("..tenant"))
    is_active = True
    name = factory.Faker("name")
    company_name = factory.Faker("company")
