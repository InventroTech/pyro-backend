from django.contrib import admin
from .models import UserSettings


@admin.register(UserSettings)
class UserSettingsAdmin(admin.ModelAdmin):
    list_display = ['tenant', 'user_id', 'key', 'value', 'created_at', 'updated_at']
    list_filter = ['tenant', 'key', 'created_at']
    search_fields = ['user_id', 'key', 'value']
    readonly_fields = ['created_at', 'updated_at']
    
    fieldsets = (
        (None, {
            'fields': ('tenant', 'user_id', 'key', 'value')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )