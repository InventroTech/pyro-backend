import factory
import uuid
from authentication.models import User
from accounts.models import SupabaseAuthUser


class UserFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = User
        django_get_or_create = ("email",)

    supabase_uid = factory.LazyFunction(lambda: str(uuid.uuid4()))
    email = factory.Faker("email")
    tenant_id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    role = "authenticated"


class SupabaseAuthUserFactory(factory.django.DjangoModelFactory):
    """Factory for auth.users (unmanaged mirror)."""

    class Meta:
        model = SupabaseAuthUser

    id = factory.LazyFunction(uuid.uuid4)
    email = factory.Faker("email")
    raw_app_meta_data = factory.LazyFunction(lambda: {"provider": "email", "providers": ["email"]})
    raw_user_meta_data = factory.LazyFunction(dict)
