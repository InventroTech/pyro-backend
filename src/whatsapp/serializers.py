from rest_framework import serializers
from .models import WhatsAppTemplate


class WhatsAppTemplateSerializer(serializers.ModelSerializer):
    """Serializer for WhatsAppTemplate model"""
    
    class Meta:
        model = WhatsAppTemplate
        fields = ['id', 'tenant', 'title', 'description', 'created_at', 'updated_at']
        read_only_fields = ['id', 'tenant', 'created_at', 'updated_at']

    def validate_title(self, value):
        """Validate that title is not empty"""
        if not value or not value.strip():
            raise serializers.ValidationError("Title cannot be empty")
        return value.strip()

    def validate_description(self, value):
        """Validate that description is not empty"""
        if not value or not value.strip():
            raise serializers.ValidationError("Description cannot be empty")
        return value.strip()
