from django.contrib import admin
from .models import UserSettings


@admin.register(UserSettings)
class UserSettingsAdmin(admin.ModelAdmin):
    list_display = ['tenant', 'tenant_membership', 'key', 'value', 'created_at', 'updated_at']
    list_filter = ['tenant', 'key', 'created_at']
    search_fields = ['tenant_membership__user_id', 'key', 'value']
    readonly_fields = ['created_at', 'updated_at']
    
    fieldsets = (
        (None, {
            'fields': ('tenant', 'tenant_membership', 'key', 'value')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )