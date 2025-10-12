from django.contrib import admin
from .models import Record


@admin.register(Record)
class RecordAdmin(admin.ModelAdmin):
    list_display = ('entity_type', 'name', 'tenant', 'created_at', 'updated_at')
    list_filter = ('entity_type', 'tenant', 'created_at')
    search_fields = ('name', 'entity_type')
    readonly_fields = ('created_at', 'updated_at')
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('tenant')
