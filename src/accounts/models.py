from django.db import models
import uuid

# Create your models here.

class LegacyUser(models.Model):
    id=models.BigAutoField(primary_key=True)
    name = models.TextField()
    email = models.EmailField(null=True, blank=True)
    tenant = models.ForeignKey(
        'core.Tenant',
        db_column='tenant_id',
        to_field='id',
        on_delete=models.DO_NOTHING,
        null=True,
        blank=True,
    )
    company_name = models.TextField(null=True, blank=True)
    role_id = models.UUIDField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    uid = models.UUIDField(null=True, blank=True)

    class Meta:
        db_table = 'users'
        managed = False
    
    def __str__(self):
        return f"{self.email or self.name} ({self.tenant_id})"
