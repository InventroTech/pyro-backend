from django.urls import path
from . import views

urlpatterns = [
    path('link-user-uid/', views.link_user_uid, name='link_user_uid'),
]
