import os
import tempfile
import json
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from rest_framework.parsers import MultiPartParser, FormParser
from openai import OpenAI
import PyPDF2


class OpenAIFileAnalysisView(APIView):
    """
    Simple OpenAI file analysis - upload PDF, get AI response.
    """
    permission_classes = [AllowAny]
    parser_classes = [MultiPartParser, FormParser]
    
    def post(self, request):
        # Validate inputs
        if 'file' not in request.FILES:
            return Response({'error': 'No file provided'}, status=400)
        
        file = request.FILES['file']
        prompt = request.data.get('prompt', 'Analyze this document')
        
        if not file.name.lower().endswith('.pdf'):
            return Response({'error': 'Only PDF files supported'}, status=400)
        
        # Get OpenAI client
        api_key = os.getenv('PYRO_OPEN_AI_API')
        if not api_key:
            return Response({'error': 'OpenAI API key not configured'}, status=500)
        
        client = OpenAI(api_key=api_key)
        
        # Extract text from PDF
        try:
            pdf_text = ""
            # Handle both old and new PyPDF2 versions
            try:
                # PyPDF2 >= 3.0 (newer version)
                pdf_reader = PyPDF2.PdfReader(file)
                for page in pdf_reader.pages:
                    pdf_text += page.extract_text() + "\n"
            except AttributeError:
                # PyPDF2 < 3.0 (older version like 1.26.0)
                pdf_reader = PyPDF2.PdfFileReader(file)
                for i in range(pdf_reader.numPages):
                    page = pdf_reader.getPage(i)
                    pdf_text += page.extractText() + "\n"
            
            if not pdf_text.strip():
                return Response({'error': 'Could not extract text from PDF'}, status=400)
            
        except Exception as e:
            return Response({'error': f'PDF processing failed: {str(e)}'}, status=400)
        
        # Send to OpenAI
        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Analyze documents and provide helpful responses."},
                    {"role": "user", "content": f"{prompt}\n\nDocument:\n{pdf_text[:8000]}"}
                ],
                max_tokens=2000,
                temperature=0.3
            )
            
            ai_response = response.choices[0].message.content
            
            # Try to parse as JSON if it looks like JSON
            try:
                if ai_response.strip().startswith('{'):
                    ai_response = json.loads(ai_response)
            except:
                pass  # Keep as text if not valid JSON
            
            return Response({
                'success': True,
                'prompt': prompt,
                'response': ai_response
            })
            
        except Exception as e:
            return Response({'error': f'OpenAI analysis failed: {str(e)}'}, status=500)


class OpenAITextAnalysisView(APIView):
    """
    Simple OpenAI text analysis - send text, get AI response.
    """
    permission_classes = [AllowAny]
    
    def post(self, request):
        # Validate inputs
        text = request.data.get('text', '').strip()
        prompt = request.data.get('prompt', 'Analyze this text')
        
        if not text:
            return Response({'error': 'No text provided'}, status=400)
        
        # Get OpenAI client
        api_key = os.getenv('PYRO_OPEN_AI_API')
        if not api_key:
            return Response({'error': 'OpenAI API key not configured'}, status=500)
        
        client = OpenAI(api_key=api_key)
        
        # Send to OpenAI
        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Analyze text and provide helpful responses."},
                    {"role": "user", "content": f"{prompt}\n\nText:\n{text}"}
                ],
                max_tokens=2000,
                temperature=0.3
            )
            
            ai_response = response.choices[0].message.content
            
            # Try to parse as JSON if it looks like JSON
            try:
                if ai_response.strip().startswith('{'):
                    ai_response = json.loads(ai_response)
            except:
                pass  # Keep as text if not valid JSON
            
            return Response({
                'success': True,
                'prompt': prompt,
                'response': ai_response
            })
            
        except Exception as e:
            return Response({'error': f'OpenAI analysis failed: {str(e)}'}, status=500)
