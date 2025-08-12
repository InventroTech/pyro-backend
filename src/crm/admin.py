from django.contrib import admin
from .models import CRM

@admin.register(CRM)
class CRMAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'phone_no', 'user', 'created_at', 'badge', 'lead_creation_date')
    list_filter = ('badge', 'lead_creation_date', 'created_at', 'user')
    search_fields = ('name', 'phone_no', 'lead_description', 'other_description')
    readonly_fields = ('id', 'created_at')
    ordering = ('-created_at',)
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'phone_no', 'user')
        }),
        ('Lead Details', {
            'fields': ('lead_description', 'other_description', 'badge', 'lead_creation_date')
        }),
        ('System Information', {
            'fields': ('id', 'created_at'),
            'classes': ('collapse',)
        }),
    )
