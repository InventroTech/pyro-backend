from django.db import models
from core.models import BaseModel


class CustomTable(BaseModel):
    """Custom tables for page builder"""
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    
    class Meta:
        db_table = 'custom_tables'
        indexes = [
            models.Index(fields=['tenant', 'name']),
        ]

    def __str__(self):
        return f"{self.name} ({self.tenant})"


class CustomColumn(BaseModel):
    """Columns for custom tables"""
    table = models.ForeignKey(CustomTable, on_delete=models.CASCADE, related_name='columns')
    name = models.CharField(max_length=255)
    type = models.CharField(max_length=50)  # text, number, date, etc.
    ordinal_position = models.PositiveIntegerField(default=0)
    is_required = models.BooleanField(default=False)
    default_value = models.TextField(blank=True, null=True)
    
    class Meta:
        db_table = 'custom_columns'
        indexes = [
            models.Index(fields=['table', 'ordinal_position']),
        ]
        ordering = ['ordinal_position']

    def __str__(self):
        return f"{self.table.name}.{self.name}"


class CustomRow(BaseModel):
    """Rows for custom tables"""
    table = models.ForeignKey(CustomTable, on_delete=models.CASCADE, related_name='rows')
    data = models.JSONField(default=dict)  # Store column values as JSON
    
    class Meta:
        db_table = 'custom_rows'
        indexes = [
            models.Index(fields=['table', '-created_at']),
        ]

    def __str__(self):
        return f"Row {self.id} in {self.table.name}"


class Page(BaseModel):
    """Pages for the page builder"""
    name = models.CharField(max_length=255)
    title = models.CharField(max_length=255)
    content = models.JSONField(default=dict)  # Page builder content
    is_published = models.BooleanField(default=False)
    user = models.ForeignKey('authentication.User', on_delete=models.CASCADE, related_name='pages')
    
    class Meta:
        db_table = 'pages'
        indexes = [
            models.Index(fields=['tenant', 'user', '-created_at']),
            models.Index(fields=['is_published']),
        ]

    def __str__(self):
        return f"{self.name} ({self.tenant})"


class Card(BaseModel):
    """Cards for the page builder"""
    name = models.CharField(max_length=255)
    title = models.CharField(max_length=255)
    content = models.JSONField(default=dict)  # Card content/configuration
    card_type = models.CharField(max_length=50, default='basic')
    user = models.ForeignKey('authentication.User', on_delete=models.CASCADE, related_name='cards')
    
    class Meta:
        db_table = 'cards'
        indexes = [
            models.Index(fields=['tenant', 'user', '-created_at']),
            models.Index(fields=['card_type']),
        ]

    def __str__(self):
        return f"{self.name} ({self.tenant})"
