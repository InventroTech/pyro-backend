from rest_framework import serializers
from .models import User

class UserMiniSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ("email", "role")


# Request/response shapes for the /auth/ endpoints. Used for API docs (Swagger) only;
# the views validate input themselves so error bodies keep the {error, message} format.

class AuthErrorSerializer(serializers.Serializer):
    error = serializers.CharField()
    message = serializers.CharField()


class AuthUserSerializer(serializers.Serializer):
    id = serializers.CharField()
    email = serializers.EmailField()
    email_confirmed_at = serializers.DateTimeField(allow_null=True)
    user_metadata = serializers.DictField()


class SessionSerializer(serializers.Serializer):
    access_token = serializers.CharField()
    token_type = serializers.CharField(default="bearer")
    expires_in = serializers.IntegerField()
    expires_at = serializers.IntegerField()
    refresh_token = serializers.CharField()
    user = AuthUserSerializer()


class LoginRequestSerializer(serializers.Serializer):
    email = serializers.EmailField()
    password = serializers.CharField()


class RefreshRequestSerializer(serializers.Serializer):
    refresh_token = serializers.CharField()


class SupabaseExchangeRequestSerializer(serializers.Serializer):
    access_token = serializers.CharField(help_text="A current Supabase Auth access token")


class LogoutRequestSerializer(serializers.Serializer):
    refresh_token = serializers.CharField()
    scope = serializers.ChoiceField(choices=["local", "global"], default="local")


class SignupRequestSerializer(serializers.Serializer):
    email = serializers.EmailField()
    password = serializers.CharField()
    data = serializers.DictField(required=False, help_text="Saved as user_metadata, e.g. {\"tenant_slug\": \"acme\"}")
    redirect_to = serializers.URLField(required=False, help_text="Where the confirmation link should send the user")


class SignupResponseSerializer(serializers.Serializer):
    user = serializers.DictField(allow_null=True)
    session = serializers.DictField(allow_null=True)
    message = serializers.CharField()


class VerifyEmailRequestSerializer(serializers.Serializer):
    token = serializers.CharField(help_text="The confirmation_token value from the emailed link")


class ResendConfirmationRequestSerializer(serializers.Serializer):
    email = serializers.EmailField()
    redirect_to = serializers.URLField(required=False)


class UpdateMeRequestSerializer(serializers.Serializer):
    data = serializers.DictField(help_text="Keys merged into user_metadata")


class ChangePasswordRequestSerializer(serializers.Serializer):
    current_password = serializers.CharField(required=False, help_text="Required when the account already has a password")
    password = serializers.CharField()


class OAuthExchangeRequestSerializer(serializers.Serializer):
    code = serializers.CharField(help_text="The `code` query parameter the frontend received after sign-in")


class OkSerializer(serializers.Serializer):
    ok = serializers.BooleanField()
