import os
import uuid
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import MultiPartParser, FormParser
from django.conf import settings
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from django.http import HttpResponse

from .models import FileUpload
from authz.permissions import IsTenantAuthenticated


class FileUploadView(APIView):
    """Upload files"""
    permission_classes = [IsTenantAuthenticated]
    parser_classes = [MultiPartParser, FormParser]
    
    def post(self, request):
        if 'file' not in request.FILES:
            return Response({'error': 'No file provided'}, status=status.HTTP_400_BAD_REQUEST)
        
        file = request.FILES['file']
        
        # Generate unique filename
        file_extension = os.path.splitext(file.name)[1]
        unique_filename = f"{uuid.uuid4()}{file_extension}"
        
        # Save file
        file_path = default_storage.save(unique_filename, file)
        
        # Create database record
        file_upload = FileUpload.objects.create(
            name=unique_filename,
            original_name=file.name,
            file_path=file_path,
            file_size=file.size,
            content_type=file.content_type,
            user=request.user,
            tenant=request.tenant
        )
        
        return Response({
            'success': True,
            'data': {
                'id': str(file_upload.id),
                'name': file_upload.name,
                'original_name': file_upload.original_name,
                'file_path': file_upload.file_path,
                'file_size': file_upload.file_size,
                'content_type': file_upload.content_type,
                'created_at': file_upload.created_at
            }
        })


class FileDownloadView(APIView):
    """Download/get file"""
    permission_classes = [IsTenantAuthenticated]
    
    def get(self, request, file_id):
        try:
            file_upload = FileUpload.objects.get(
                id=file_id,
                tenant=request.tenant
            )
        except FileUpload.DoesNotExist:
            return Response({'error': 'File not found'}, status=status.HTTP_404_NOT_FOUND)
        
        # Check if file exists in storage
        if not default_storage.exists(file_upload.file_path):
            return Response({'error': 'File not found in storage'}, status=status.HTTP_404_NOT_FOUND)
        
        # Get file content
        file_content = default_storage.open(file_upload.file_path, 'rb').read()
        
        # Return file as response
        response = HttpResponse(file_content, content_type=file_upload.content_type)
        response['Content-Disposition'] = f'attachment; filename="{file_upload.original_name}"'
        return response


class FileDeleteView(APIView):
    """Delete file"""
    permission_classes = [IsTenantAuthenticated]
    
    def delete(self, request, file_id):
        try:
            file_upload = FileUpload.objects.get(
                id=file_id,
                tenant=request.tenant
            )
        except FileUpload.DoesNotExist:
            return Response({'error': 'File not found'}, status=status.HTTP_404_NOT_FOUND)
        
        # Delete file from storage
        if default_storage.exists(file_upload.file_path):
            default_storage.delete(file_upload.file_path)
        
        # Delete database record
        file_upload.delete()
        
        return Response({'success': True, 'message': 'File deleted successfully'})


class FileListView(APIView):
    """List user's files"""
    permission_classes = [IsTenantAuthenticated]
    
    def get(self, request):
        files = FileUpload.objects.filter(
            tenant=request.tenant,
            user=request.user
        ).order_by('-created_at')
        
        file_list = []
        for file_upload in files:
            file_list.append({
                'id': str(file_upload.id),
                'name': file_upload.name,
                'original_name': file_upload.original_name,
                'file_path': file_upload.file_path,
                'file_size': file_upload.file_size,
                'content_type': file_upload.content_type,
                'created_at': file_upload.created_at
            })
        
        return Response({
            'success': True,
            'data': file_list
        })
