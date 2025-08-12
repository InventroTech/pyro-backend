from rest_framework import serializers
from .models import CRM

class CRMSerializer(serializers.ModelSerializer):
    class Meta:
        model = CRM
        fields = '__all__'
        read_only_fields = ('id', 'created_at')

class CRMListSerializer(serializers.ModelSerializer):
    user_email = serializers.CharField(source='user.email', read_only=True)
    
    class Meta:
        model = CRM
        fields = [
            'id', 'name', 'phone_no', 'user_email', 'created_at',
            'lead_description', 'other_description', 'badge', 'lead_creation_date'
        ]
