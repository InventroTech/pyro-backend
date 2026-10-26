from django.contrib import admin

from core.models import TenantSettings


@admin.register(TenantSettings)
class TenantSettingsAdmin(admin.ModelAdmin):
    list_display = (
        "tenant_id",
        "persistent_object_history",
        "chatbot_page_owner_email",
        "inactive_emails_enabled",
        "inactive_login_hours",
    )
    list_filter = ("persistent_object_history", "inactive_emails_enabled")
    search_fields = ("tenant_id", "chatbot_page_owner_email")
    fields = (
        "tenant",
        "persistent_object_history",
        "chatbot_page_owner_email",
        "inactive_emails_enabled",
        "inactive_login_hours",
    )
