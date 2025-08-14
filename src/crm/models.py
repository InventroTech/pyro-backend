from django.db import models
from core.models import BaseModel 

class Lead(BaseModel):
    id = models.BigAutoField(primary_key=True)
    name = models.TextField()
    phone_no = models.TextField(unique=True)
    user_id = models.CharField(max_length=255, null=True, blank=True)
    lead_description = models.TextField(null=True, blank=True)
    other_description = models.TextField(null=True, blank=True)
    badge = models.TextField(null=True, blank=True)
    lead_creation_date = models.DateField(null=True, blank=True)
    praja_dashboard_user_link = models.TextField(null=True, blank=True)
    lead_score = models.FloatField(null=True, blank=True)
    atleast_paid_once = models.BooleanField(null=True, blank=True)
    reason = models.TextField(null=True, blank=True) 
    badge = models.CharField(max_length=255, null=True, blank=True)
    display_pic_url = models.TextField(null=True, blank=True)
    assigned_to = models.UUIDField(null=True, blank=True)
    lead_status = models.CharField(max_length=50, null=True, blank=True)  



    class Meta:
        db_table = 'leads'
        managed = True
        indexes = BaseModel.Meta.indexes + [
            models.Index(fields=['assigned_to', 'lead_status', '-created_at']),
        ]

    def __str__(self):
        return f"{self.name} - {self.phone_no}"
