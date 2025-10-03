from rest_framework import serializers
from .models import CustomTable, CustomColumn, CustomRow, Page, Card


class CustomColumnSerializer(serializers.ModelSerializer):
    class Meta:
        model = CustomColumn
        fields = ['id', 'name', 'type', 'ordinal_position', 'is_required', 'default_value', 'created_at', 'updated_at']


class CustomTableSerializer(serializers.ModelSerializer):
    columns = CustomColumnSerializer(many=True, read_only=True)
    
    class Meta:
        model = CustomTable
        fields = ['id', 'name', 'description', 'columns', 'created_at', 'updated_at']


class CustomRowSerializer(serializers.ModelSerializer):
    class Meta:
        model = CustomRow
        fields = ['id', 'data', 'created_at', 'updated_at']


class PageSerializer(serializers.ModelSerializer):
    user_email = serializers.CharField(source='user.email', read_only=True)
    
    class Meta:
        model = Page
        fields = ['id', 'name', 'title', 'content', 'is_published', 'user_email', 'created_at', 'updated_at']
        read_only_fields = ['user']


class CardSerializer(serializers.ModelSerializer):
    user_email = serializers.CharField(source='user.email', read_only=True)
    
    class Meta:
        model = Card
        fields = ['id', 'name', 'title', 'content', 'card_type', 'user_email', 'created_at', 'updated_at']
        read_only_fields = ['user']
