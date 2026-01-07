from django.urls import path
from .views import WhatsAppTemplateListCreateView, WhatsAppTemplateDetailView

app_name = "whatsapp"

urlpatterns = [
    path(
        'templates/',
        WhatsAppTemplateListCreateView.as_view(),
        name='whatsapp-template-list-create'
    ),
    path(
        'templates/<int:pk>/',
        WhatsAppTemplateDetailView.as_view(),
        name='whatsapp-template-detail'
    ),
]
