from django.contrib import admin
from django import forms
from .models import Record, EntityTypeSchema, ApiSecretKey


@admin.register(EntityTypeSchema)
class EntityTypeSchemaAdmin(admin.ModelAdmin):
    list_display = ['entity_type', 'tenant', 'attribute_count', 'created_at', 'updated_at']
    list_filter = ['entity_type', 'created_at']
    search_fields = ['entity_type', 'description']
    readonly_fields = ['id', 'tenant', 'created_at', 'updated_at']
    
    def attribute_count(self, obj):
        return len(obj.attributes) if obj.attributes else 0
    attribute_count.short_description = 'Attributes Count'


@admin.register(ApiSecretKey)
class ApiSecretKeyAdmin(admin.ModelAdmin):
    list_display = ['secret_key_display', 'tenant', 'is_active', 'last_used_at', 'created_at']
    list_filter = ['is_active', 'created_at', 'last_used_at']
    search_fields = ['secret_key_hash', 'secret_key_last4', 'description', 'tenant__name', 'tenant__slug']
    readonly_fields = ['id', 'created_at', 'updated_at', 'last_used_at']
    fields = ['raw_secret_key', 'tenant', 'description', 'is_active', 'secret_key_last4', 'last_used_at', 'created_at', 'updated_at']

    class ApiSecretKeyAdminForm(forms.ModelForm):
        raw_secret_key = forms.CharField(
            required=False,
            widget=forms.PasswordInput(render_value=False),
            help_text="Set/rotate the secret (plaintext is NOT stored). Leave blank to keep existing secret."
        )

        class Meta:
            model = ApiSecretKey
            fields = ['raw_secret_key', 'tenant', 'description', 'is_active']

    form = ApiSecretKeyAdminForm
    
    def secret_key_display(self, obj):
        """Display a non-sensitive identifier."""
        last4 = obj.secret_key_last4 or "????"
        return f"****{last4}"
    secret_key_display.short_description = 'Secret Key'

    def save_model(self, request, obj, form, change):
        raw = form.cleaned_data.get("raw_secret_key")
        if raw:
            obj.set_raw_secret(raw)
        super().save_model(request, obj, form, change)

