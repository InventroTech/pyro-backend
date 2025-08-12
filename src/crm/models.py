from django.db import models
from authentication.models import User

class CRM(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.TextField()
    phone_no = models.TextField(unique=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    lead_description = models.TextField(null=True, blank=True)
    other_description = models.TextField(null=True, blank=True)
    badge = models.TextField(null=True, blank=True)
    lead_creation_date = models.DateField(null=True, blank=True)

    class Meta:
        db_table = 'crm'
        managed = True

    def __str__(self):
        return f"{self.name} - {self.phone_no}"
