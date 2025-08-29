import factory
from crm.models import CRM
from authentication.models import User
from datetime import date

class CRMFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = CRM
        django_get_or_create = ("id",)

    id = factory.Sequence(lambda n: n + 1)
    name = factory.Faker("name")
    phone_no = factory.Faker("phone_number")
    user = factory.SubFactory('tests.factories.user_factory.UserFactory')
    lead_description = factory.Faker("text", max_nb_chars=200)
    other_description = factory.Faker("text", max_nb_chars=200)
    badge = factory.Iterator(["Hot", "Warm", "Cold", None])
    lead_creation_date = factory.LazyFunction(lambda: date.today())
