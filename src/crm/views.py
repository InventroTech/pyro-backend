from django.shortcuts import render
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.db.models import Q
from .models import CRM
from .serializers import CRMListSerializer, CRMSerializer

# Create your views here.

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_all_leads(request):
    """
    Get all leads with optional filtering
    """
    try:
        # Get query parameters for filtering
        search = request.query_params.get('search', '')
        user_id = request.query_params.get('user_id', '')
        
        # Start with all CRM records
        queryset = CRM.objects.all()
        
        # Apply search filter if provided
        if search:
            queryset = queryset.filter(
                Q(name__icontains=search) |
                Q(phone_no__icontains=search) |
                Q(lead_description__icontains=search) |
                Q(other_description__icontains=search)
            )
        
        # Apply user filter if provided
        if user_id:
            queryset = queryset.filter(user_id=user_id)
        
        # Order by created_at descending (newest first)
        queryset = queryset.order_by('-created_at')
        
        # Serialize the data
        serializer = CRMListSerializer(queryset, many=True)
        
        return Response({
            'status': 'success',
            'message': 'Leads retrieved successfully',
            'data': serializer.data,
            'count': queryset.count()
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        return Response({
            'status': 'error',
            'message': f'Error retrieving leads: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def create_lead(request):
    """
    Create a new lead
    """
    try:
        serializer = CRMSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response({
                'status': 'success',
                'message': 'Lead created successfully',
                'data': serializer.data
            }, status=status.HTTP_201_CREATED)
        else:
            return Response({
                'status': 'error',
                'message': 'Invalid data provided',
                'errors': serializer.errors
            }, status=status.HTTP_400_BAD_REQUEST)
            
    except Exception as e:
        return Response({
            'status': 'error',
            'message': f'Error creating lead: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_lead_by_id(request, lead_id):
    """
    Get a specific lead by ID
    """
    try:
        lead = CRM.objects.get(id=lead_id)
        serializer = CRMSerializer(lead)
        
        return Response({
            'status': 'success',
            'message': 'Lead retrieved successfully',
            'data': serializer.data
        }, status=status.HTTP_200_OK)
        
    except CRM.DoesNotExist:
        return Response({
            'status': 'error',
            'message': 'Lead not found'
        }, status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response({
            'status': 'error',
            'message': f'Error retrieving lead: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
