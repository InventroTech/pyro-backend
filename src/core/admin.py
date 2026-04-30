from django.contrib import admin

from core.models import TenantSettings


@admin.register(TenantSettings)
class TenantSettingsAdmin(admin.ModelAdmin):
    list_display = ("tenant_id", "persistent_object_history")
    list_filter = ("persistent_object_history",)
    search_fields = ("tenant_id",)
