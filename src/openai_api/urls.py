from django.urls import path
from .views import OpenAIFileAnalysisView, OpenAITextAnalysisView

app_name = 'openai_api'

urlpatterns = [
    # File upload and analysis
    path('analyze-file/', OpenAIFileAnalysisView.as_view(), name='analyze-file'),
    
    # Text-only analysis
    path('analyze-text/', OpenAITextAnalysisView.as_view(), name='analyze-text'),
]
