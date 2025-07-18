from django.contrib import admin
from .models import SupportTicket
# Register your models here.

@admin.register(SupportTicket)
class SupportTicketAdmin(admin.ModelAdmin):
    list_display = (
        'id', 
        'user_id', 
        'reason',
        'resolution_status',
        'created_at', 
        'completed_at'  
    )
    list_filter = (
        'resolution_status',
        'created_at', 
        'completed_at'
    )
    search_fields = ('reason', 'user_id', 'name', 'phone')
