from django.db import models
from core.models import BaseModel


class FileUpload(BaseModel):
    """File uploads for the page builder"""
    name = models.CharField(max_length=255)
    original_name = models.CharField(max_length=255)
    file_path = models.CharField(max_length=500)
    file_size = models.BigIntegerField()
    content_type = models.CharField(max_length=100)
    user = models.ForeignKey('authentication.User', on_delete=models.CASCADE, related_name='file_uploads')
    
    class Meta:
        db_table = 'file_uploads'
        indexes = [
            models.Index(fields=['tenant', 'user', '-created_at']),
        ]

    def __str__(self):
        return f"{self.original_name} ({self.tenant})"
