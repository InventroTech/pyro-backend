from django.contrib import admin
from .models import SupportTicketDump


@admin.register(SupportTicketDump)
class SupportTicketDumpAdmin(admin.ModelAdmin):
    list_display = [
        'id', 'name', 'user_id', 'tenant_id', 'is_processed', 
        'created_at', 'dumped_at'
    ]
    list_filter = [
        'is_processed', 'created_at', 'dumped_at', 'tenant_id', 'source'
    ]
    search_fields = [
        'name', 'user_id', 'reason', 'phone', 'cse_name'
    ]
    readonly_fields = [
        'id', 'created_at', 'dumped_at'
    ]
    list_per_page = 50
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('id', 'tenant_id', 'name', 'user_id', 'phone', 'reason')
        }),
        ('Status & Processing', {
            'fields': ('is_processed', 'ticket_date', 'dumped_at')
        }),
        ('Additional Details', {
            'fields': ('rm_name', 'layout_status', 'badge', 'poster', 'subscription_status', 'atleast_paid_once')
        }),
        ('Resolution Info', {
            'fields': ('resolution_status', 'resolution_time', 'cse_name', 'cse_remarks')
        }),
        ('Call Information', {
            'fields': ('call_status', 'call_attempts', 'completed_at', 'snooze_until')
        }),
        ('Metadata', {
            'fields': ('source', 'praja_dashboard_user_link', 'display_pic_url', 'assigned_to')
        }),
        ('Timestamps', {
            'fields': ('created_at',),
            'classes': ('collapse',)
        }),
    )
    
    actions = ['mark_as_processed', 'mark_as_unprocessed']
    
    def mark_as_processed(self, request, queryset):
        """Mark selected tickets as processed"""
        updated = queryset.update(is_processed=True)
        self.message_user(request, f'{updated} tickets marked as processed.')
    mark_as_processed.short_description = "Mark selected tickets as processed"
    
    def mark_as_unprocessed(self, request, queryset):
        """Mark selected tickets as unprocessed"""
        updated = queryset.update(is_processed=False)
        self.message_user(request, f'{updated} tickets marked as unprocessed.')
    mark_as_unprocessed.short_description = "Mark selected tickets as unprocessed"
