"""
Simple log formatter for API logging middleware with debugging details.
"""
import json
import logging


class SimpleAPILogFormatter(logging.Formatter):
    """
    Formatter that shows basic info for all requests, detailed info for errors/slow requests.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with debugging details when needed."""
        rid = getattr(record, 'request_id', '-')

        if hasattr(record, 'log_type'):
            if hasattr(record, 'log_data') and record.log_data:
                log_data = record.log_data
                method = log_data.get('method', 'N/A')
                path = log_data.get('path', 'N/A')
                status = log_data.get('status_code', 'N/A')
                duration = log_data.get('duration_ms')
                
                if duration:
                    base_line = f"[{rid}] {method:4} {path:40} → {status} ({duration:>6.0f}ms)"
                else:
                    base_line = f"[{rid}] {method:4} {path:40} → {status}"
                
                details = []
                
                if log_data.get('query_params'):
                    qp = log_data['query_params']
                    if isinstance(qp, dict) and qp:
                        details.append(f"Query: {json.dumps(qp)}")
                
                if log_data.get('payload'):
                    payload = log_data['payload']
                    payload_str = json.dumps(payload) if isinstance(payload, dict) else str(payload)
                    if len(payload_str) > 500:
                        payload_str = payload_str[:500] + "...[TRUNCATED]"
                    details.append(f"Payload: {payload_str}")
                
                response_body = log_data.get('response_body') or log_data.get('response', {}).get('body')
                if response_body:
                    body_str = json.dumps(response_body, indent=2) if isinstance(response_body, dict) else str(response_body)
                    if len(body_str) > 1000:
                        body_str = body_str[:1000] + "...[TRUNCATED]"
                    details.append(f"Response:\n{body_str}")
                
                if status >= 400 or log_data.get('user', {}).get('is_authenticated'):
                    if log_data.get('user', {}).get('is_authenticated'):
                        details.append(f"User: {log_data['user'].get('email', 'N/A')}")
                    if log_data.get('tenant', {}).get('tenant_slug'):
                        details.append(f"Tenant: {log_data['tenant']['tenant_slug']}")
                
                if details:
                    return base_line + "\n  " + "\n  ".join(details)
                
                return base_line
            
            if hasattr(record, 'exception_type'):
                method = getattr(record, 'method', 'N/A')
                path = getattr(record, 'path', 'N/A')
                exc_type = record.exception_type
                exc_msg = getattr(record, 'exception_message', '')[:200]
                return f"[{rid}] {method:4} {path:40} → EXCEPTION: {exc_type} - {exc_msg}"
        
        return super().format(record)
