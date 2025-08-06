import logging
import subprocess
import os
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated, AllowAny

logger = logging.getLogger(__name__)


class CopyScriptView(APIView):
    """
    Class-based view to run Python scripts via API call.
    Similar structure to analytics views.
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
        Execute a Python script with the provided arguments.
        
        Expected request body:
        {
            "script_path": "script_name.py",
            "script_args": ["--arg1", "value1", "--arg2", "value2"]
        }
        """
        try:
            # Get script path from request
            script_path = request.data.get('script_path')
            script_args = request.data.get('script_args', [])
            
            if not script_path:
                return Response(
                    {'error': 'script_path is required'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Check if script exists
            if not os.path.exists(script_path):
                # Try with scripts directory relative to current working directory
                scripts_path = os.path.join(os.getcwd(), 'scripts', os.path.basename(script_path))
                if os.path.exists(scripts_path):
                    script_path = scripts_path
                else:
                    return Response(
                        {'error': f'Script not found: {script_path}'},
                        status=status.HTTP_404_NOT_FOUND
                    )
            
            # Run the Python script
            logger.info(f"Running Python script: {script_path}")
            
            # Execute the script with proper environment for Unicode and performance
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            env['PYTHONUTF8'] = '1'
            env['BATCH_SIZE'] = '1'  # Process 1 ticket per batch (one by one)
            
            result = subprocess.run(
                ['python', script_path] + script_args,
                capture_output=True,
                text=True,
                encoding='utf-8',
                env=env,
                timeout=1800  # 30 minutes timeout
            )
            
            # Prepare response
            response_data = {
                'message': 'Script executed successfully',
                'script_path': script_path,
                'return_code': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
            if result.returncode == 0:
                logger.info(f"Script {script_path} executed successfully")
                return Response(response_data, status=status.HTTP_200_OK)
            else:
                logger.error(f"Script {script_path} failed with return code {result.returncode}")
                return Response(response_data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
                
        except subprocess.TimeoutExpired:
            logger.error(f"Script {script_path} timed out")
            return Response(
                {'error': 'Script execution timed out'},
                status=status.HTTP_408_REQUEST_TIMEOUT
            )
        except Exception as e:
            logger.error(f"Error running script: {str(e)}")
            return Response(
                {'error': 'Internal server error', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            ) 
