"""
URL configuration for config project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from rest_framework import permissions
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
)

def trigger_error(request):
    division_by_zero = 1 / 0


urlpatterns = [
    path('admin/', admin.site.urls),
    path('analytics/', include('analytics.urls')),
    path('auth/', include('authentication.urls')),
    path('membership/', include('authz.urls')),
    path('cron-jobs/',include('cron_jobs.urls')),
    path('crm/', include('crm.urls')),
    path('crm-records/', include('crm_records.urls')),
    path('accounts/', include('accounts.urls')),
    path('support-ticket/', include('support_ticket.urls')),
    # OpenAPI schema
    path("api/schema/", SpectacularAPIView.as_view(
        permission_classes=[permissions.AllowAny]
    ), name="schema"),

    # Swagger UI
    path("", SpectacularSwaggerView.as_view(
        url_name="schema",
        permission_classes=[permissions.AllowAny] 
    ), name="swagger-ui"),

    # Redoc
    path("api/redoc/", SpectacularRedocView.as_view(
        url_name="schema",
        permission_classes=[permissions.AllowAny]
    ), name="redoc"),
    path('sentry-debug/', trigger_error),
]
