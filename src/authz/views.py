from django.shortcuts import render
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework import status
from authz.service import link_user_uid_and_activate
import logging

logger = logging.getLogger(__name__)


@api_view(['POST'])
@permission_classes([AllowAny])
def link_user_uid(request):
    """
    API endpoint to link Supabase UID to user and activate tenant memberships.
    This replaces the functionality of the edge function.
    """
    try:
        data = request.data
        email = data.get('email')
        uid = data.get('uid')
        
        if not email or not uid:
            return Response(
                {'error': 'Email and UID are required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        result = link_user_uid_and_activate(email, uid)
        
        if result['success']:
            return Response(result, status=status.HTTP_200_OK)
        else:
            return Response(result, status=status.HTTP_400_BAD_REQUEST)
            
    except Exception as e:
        logger.error(f"Error in link_user_uid: {str(e)}")
        return Response(
            {'error': 'Internal server error', 'message': str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
