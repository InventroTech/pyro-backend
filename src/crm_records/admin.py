from django.contrib import admin
from .models import Record, EntityTypeSchema


@admin.register(EntityTypeSchema)
class EntityTypeSchemaAdmin(admin.ModelAdmin):
    list_display = ['entity_type', 'tenant', 'attribute_count', 'created_at', 'updated_at']
    list_filter = ['entity_type', 'created_at']
    search_fields = ['entity_type', 'description']
    readonly_fields = ['id', 'tenant', 'created_at', 'updated_at']
    
    def attribute_count(self, obj):
        return len(obj.attributes) if obj.attributes else 0
    attribute_count.short_description = 'Attributes Count'

