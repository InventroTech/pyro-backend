import factory
import uuid
from authentication.models import User

class UserFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = User
        django_get_or_create = ("email",)

    supabase_uid = factory.LazyFunction(lambda: str(uuid.uuid4()))
    email = factory.Faker("email")
    tenant_id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    role = "authenticated"
