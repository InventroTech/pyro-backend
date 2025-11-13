import os
import tempfile
import logging
from django.conf import settings
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from rest_framework.parsers import MultiPartParser, FormParser
from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiParameter
from drf_spectacular.types import OpenApiTypes
from openai import OpenAI

logger = logging.getLogger(__name__)


def parse_json_response(response_text):
    """
    Try to parse OpenAI response as JSON. If it's valid JSON, return parsed object.
    If not, return the original text.
    """
    import json
    
    if not response_text:
        return response_text
    
    # Clean up the response text
    cleaned_text = response_text.strip()
    
    # Remove markdown code blocks if present
    if cleaned_text.startswith('```json'):
        cleaned_text = cleaned_text[7:]  # Remove ```json
    if cleaned_text.startswith('```'):
        cleaned_text = cleaned_text[3:]   # Remove ```
    if cleaned_text.endswith('```'):
        cleaned_text = cleaned_text[:-3]  # Remove trailing ```
    
    cleaned_text = cleaned_text.strip()
    
    # Try to parse as JSON
    try:
        parsed_json = json.loads(cleaned_text)
        logger.info("Successfully parsed OpenAI response as JSON")
        return parsed_json
    except json.JSONDecodeError as e:
        logger.warning(f"Could not parse OpenAI response as JSON: {str(e)}")
        # Return original text if JSON parsing fails
        return response_text


class OpenAIFileAnalysisView(APIView):
    """
    Simple OpenAI file upload and analysis API.
    
    Upload a PDF file and get AI analysis with a custom prompt.
    No database storage - direct processing only.
    """
    permission_classes = [AllowAny]
    parser_classes = [MultiPartParser, FormParser]
    
    @extend_schema(
        summary="Analyze File with OpenAI",
        description="Upload a PDF file and analyze it with OpenAI using a custom prompt. No database storage.",
        request={
            'multipart/form-data': {
                'type': 'object',
                'properties': {
                    'file': {
                        'type': 'string',
                        'format': 'binary',
                        'description': 'PDF file to upload and analyze'
                    },
                    'prompt': {
                        'type': 'string',
                        'description': 'Custom prompt/question to ask about the file',
                        'default': 'Analyze this document and provide a summary.'
                    }
                },
                'required': ['file']
            }
        },
        parameters=[
            OpenApiParameter(
                name="prompt",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Custom prompt/question about the file",
                required=False
            )
        ],
        responses={
            200: OpenApiResponse(
                description="File analyzed successfully",
                examples={
                    'application/json': {
                        'success': True,
                        'message': 'File analyzed successfully',
                        'file_id': 'file-abc123',
                        'prompt': 'What is the first dragon in the book?',
                        'response': 'The first dragon mentioned in the book is...',
                        'processing_time': 3.45
                    }
                }
            ),
            400: OpenApiResponse(description="Invalid file or validation error"),
            500: OpenApiResponse(description="OpenAI API error")
        }
    )
    def post(self, request):
        """
        Upload file to OpenAI and get analysis response.
        """
        import time
        start_time = time.time()
        
        try:
            # Step 1: Validate file upload
            if 'file' not in request.FILES:
                return Response({
                    'success': False,
                    'error': 'No file provided. Please upload a file.'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            uploaded_file = request.FILES['file']
            
            # Validate file type (PDF only)
            if not uploaded_file.name.lower().endswith('.pdf'):
                return Response({
                    'success': False,
                    'error': 'Only PDF files are supported.'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Validate file size (max 20MB for OpenAI)
            max_size = 20 * 1024 * 1024  # 20MB
            if uploaded_file.size > max_size:
                return Response({
                    'success': False,
                    'error': f'File too large. Maximum size is 20MB. Your file: {uploaded_file.size / (1024*1024):.2f}MB'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Get custom prompt from request
            prompt = request.data.get('prompt', 'Analyze this document and provide a detailed summary.')
            
            logger.info(f"Processing file: {uploaded_file.name} ({uploaded_file.size} bytes)")
            logger.info(f"Prompt: {prompt}")
            
            # Step 2: Initialize OpenAI client
            openai_api_key = os.getenv('PYRO_OPEN_AI_API')
            if not openai_api_key:
                return Response({
                    'success': False,
                    'error': 'OpenAI API key not configured on server. Please set PYRO_OPEN_AI_API environment variable.'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            client = OpenAI(api_key=openai_api_key)
            
            # Step 3: Save file temporarily and upload to OpenAI
            temp_file_path = None
            openai_file = None
            
            try:
                # Create temporary file
                with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                    temp_file_path = temp_file.name
                    
                    # Write uploaded file to temporary location
                    for chunk in uploaded_file.chunks():
                        temp_file.write(chunk)
                
                logger.info(f"Saved temporary file: {temp_file_path}")
                
                # Upload file to OpenAI
                with open(temp_file_path, 'rb') as file_obj:
                    openai_file = client.files.create(
                        file=file_obj,
                        purpose="user_data"
                    )
                
                logger.info(f"Uploaded to OpenAI with file ID: {openai_file.id}")
                
                # Step 4: Create analysis request
                try:
                    # First, let's try to retrieve and read the file content from OpenAI
                    # Since direct file reference in chat completions might not work as expected
                    
                    # Get file content from OpenAI
                    file_content_response = client.files.content(openai_file.id)
                    file_content = file_content_response.read()
                    
                    # If it's a PDF, we already extracted text earlier, so let's use that
                    # But let's also try to get content from OpenAI's file storage
                    try:
                        file_text = file_content.decode('utf-8')
                    except:
                        # If decoding fails, use our extracted PDF text
                        with open(temp_file_path, 'rb') as pdf_file:
                            import PyPDF2
                            pdf_reader = PyPDF2.PdfReader(pdf_file)
                            file_text = ""
                            for page in pdf_reader.pages:
                                file_text += page.extract_text() + "\n"
                    
                    # Now use chat completions with the actual file content
                    response = client.chat.completions.create(
                        model="gpt-3.5-turbo",  # Using GPT-3.5 Turbo - faster and cheaper
                        messages=[
                            {
                                "role": "system",
                                "content": "You are a helpful assistant that analyzes document content. If the user asks for JSON format, return valid JSON only without any markdown formatting or additional text."
                            },
                            {
                                "role": "user", 
                                "content": f"Please analyze the following document content and answer this question: {prompt}\n\nDocument content:\n{file_text[:8000]}"  # Limit to 8000 chars to stay within token limits
                            }
                        ],
                        max_tokens=2000,
                        temperature=0.3
                    )
                    
                    analysis_result = response.choices[0].message.content
                    # Try to parse as JSON if the response looks like JSON
                    analysis_result = parse_json_response(analysis_result)
                    
                except Exception as e:
                    logger.warning(f"File content retrieval failed, trying direct file reference: {str(e)}")
                    
                    try:
                        # Fallback: Try the assistants API which better supports file analysis
                        # Create a temporary assistant
                        assistant = client.beta.assistants.create(
                            name="Document Analyzer",
                            instructions="You are a helpful assistant that analyzes documents. Provide detailed and accurate responses based on the document content.",
                            model="gpt-3.5-turbo",
                            tools=[{"type": "retrieval"}]
                        )
                        
                        # Create a thread
                        thread = client.beta.threads.create()
                        
                        # Add message with file
                        message = client.beta.threads.messages.create(
                            thread_id=thread.id,
                            role="user",
                            content=prompt,
                            file_ids=[openai_file.id]
                        )
                        
                        # Run the assistant
                        run = client.beta.threads.runs.create(
                            thread_id=thread.id,
                            assistant_id=assistant.id
                        )
                        
                        # Wait for completion
                        import time
                        while run.status in ['queued', 'in_progress']:
                            time.sleep(1)
                            run = client.beta.threads.runs.retrieve(
                                thread_id=thread.id,
                                run_id=run.id
                            )
                        
                        # Get the response
                        messages = client.beta.threads.messages.list(thread_id=thread.id)
                        analysis_result = messages.data[0].content[0].text.value
                        # Try to parse as JSON if the response looks like JSON
                        analysis_result = parse_json_response(analysis_result)
                        
                        # Cleanup
                        client.beta.assistants.delete(assistant.id)
                        
                    except Exception as e2:
                        logger.error(f"Assistants API also failed: {str(e2)}")
                        # Final fallback - just extract PDF text and analyze
                        try:
                            with open(temp_file_path, 'rb') as pdf_file:
                                import PyPDF2
                                pdf_reader = PyPDF2.PdfReader(pdf_file)
                                extracted_text = ""
                                for page in pdf_reader.pages:
                                    extracted_text += page.extract_text() + "\n"
                            
                            if not extracted_text.strip():
                                raise ValueError("No text could be extracted from PDF")
                            
                            # Analyze extracted text
                            response = client.chat.completions.create(
                                model="gpt-3.5-turbo",
                                messages=[
                                    {
                                        "role": "system",
                                        "content": "You are a helpful assistant that analyzes document content. If the user asks for JSON format, return valid JSON only without any markdown formatting or additional text."
                                    },
                                    {
                                        "role": "user", 
                                        "content": f"Analyze this document content and answer: {prompt}\n\nContent:\n{extracted_text[:8000]}"
                                    }
                                ],
                                max_tokens=2000,
                                temperature=0.3
                            )
                            
                            analysis_result = response.choices[0].message.content
                            # Try to parse as JSON if the response looks like JSON
                            analysis_result = parse_json_response(analysis_result)
                            
                        except Exception as e3:
                            logger.error(f"All methods failed: {str(e3)}")
                            raise Exception(f"Could not analyze document: {str(e3)}")
                
                processing_time = time.time() - start_time
                
                # Step 5: Return response
                return Response({
                    'success': True,
                    'message': 'File analyzed successfully',
                    'file_id': openai_file.id,
                    'filename': uploaded_file.name,
                    'prompt': prompt,
                    'response': analysis_result,
                    'processing_time': round(processing_time, 2)
                }, status=status.HTTP_200_OK)
                
            finally:
                # Cleanup: Delete temporary file
                if temp_file_path and os.path.exists(temp_file_path):
                    try:
                        os.unlink(temp_file_path)
                        logger.info(f"Deleted temporary file: {temp_file_path}")
                    except Exception as e:
                        logger.warning(f"Could not delete temp file {temp_file_path}: {str(e)}")
                
                # Optionally delete file from OpenAI (uncomment if you want to clean up)
                # if openai_file:
                #     try:
                #         client.files.delete(openai_file.id)
                #         logger.info(f"Deleted OpenAI file: {openai_file.id}")
                #     except Exception as e:
                #         logger.warning(f"Could not delete OpenAI file: {str(e)}")
                
        except Exception as e:
            logger.error(f"Unexpected error in OpenAI file analysis: {str(e)}")
            return Response({
                'success': False,
                'error': f'Processing failed: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class OpenAITextAnalysisView(APIView):
    """
    Simple text-only analysis with OpenAI (no file upload).
    """
    permission_classes = [AllowAny]
    
    @extend_schema(
        summary="Analyze Text with OpenAI",
        description="Send text directly to OpenAI for analysis without file upload.",
        request={
            'application/json': {
                'type': 'object',
                'properties': {
                    'text': {
                        'type': 'string',
                        'description': 'Text content to analyze'
                    },
                    'prompt': {
                        'type': 'string',
                        'description': 'Analysis prompt/question',
                        'default': 'Analyze this text and provide insights.'
                    }
                },
                'required': ['text']
            }
        },
        responses={
            200: OpenApiResponse(description="Text analyzed successfully"),
            400: OpenApiResponse(description="Invalid request"),
            500: OpenApiResponse(description="OpenAI API error")
        }
    )
    def post(self, request):
        """
        Analyze text content with OpenAI.
        """
        import time
        start_time = time.time()
        
        try:
            text_content = request.data.get('text', '').strip()
            prompt = request.data.get('prompt', 'Analyze this text and provide insights.')
            
            if not text_content:
                return Response({
                    'success': False,
                    'error': 'No text content provided.'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Initialize OpenAI client
            openai_api_key = os.getenv('PYRO_OPEN_AI_API')
            if not openai_api_key:
                return Response({
                    'success': False,
                    'error': 'OpenAI API key not configured. Please set PYRO_OPEN_AI_API environment variable.'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            client = OpenAI(api_key=openai_api_key)
            
            # Create analysis request
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful assistant that analyzes text content. If the user asks for JSON format, return valid JSON only without any markdown formatting or additional text."
                    },
                    {
                        "role": "user",
                        "content": f"{prompt}\n\nText to analyze:\n{text_content}"
                    }
                ],
                max_tokens=2000,
                temperature=0.3
            )
            
            analysis_result = response.choices[0].message.content
            # Try to parse as JSON if the response looks like JSON
            analysis_result = parse_json_response(analysis_result)
            processing_time = time.time() - start_time
            
            return Response({
                'success': True,
                'message': 'Text analyzed successfully',
                'prompt': prompt,
                'response': analysis_result,
                'processing_time': round(processing_time, 2)
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Text analysis error: {str(e)}")
            return Response({
                'success': False,
                'error': f'Analysis failed: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
