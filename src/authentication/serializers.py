from rest_framework import serializers
from .models import User

class UserMiniSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ("email", "role")