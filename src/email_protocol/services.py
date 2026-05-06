import logging
import os
from typing import Tuple, List, Optional, Dict, Any, Union
from django.core.mail import EmailMessage, EmailMultiAlternatives
from django.conf import settings

logger = logging.getLogger(__name__)


def send_email(
    to_emails: Union[str, List[str]],
    subject: str,
    message: str = "",
    html_message: Optional[str] = None,
    cc: Optional[Union[str, List[str]]] = None,
    bcc: Optional[Union[str, List[str]]] = None,
    reply_to: Optional[Union[str, List[str]]] = None,
    from_email: Optional[str] = None,
    client_name: Optional[str] = None,
    fail_silently: bool = False
) -> Tuple[bool, str]:
    """
    Unified internal email service function - call directly from code
    
    This is a single, flexible function that handles all email sending scenarios.
    You can pass a single email string or a list of emails for any recipient field.
    
    Args:
        to_emails: Single email string or list of recipient email addresses (required)
        subject: Email subject (required)
        message: Plain text message body (optional if html_message provided)
        html_message: HTML message body (optional if message provided)
        cc: Single email string or list of CC email addresses (optional)
        bcc: Single email string or list of BCC email addresses (optional)
        reply_to: Single email string or list of reply-to email addresses (optional)
        from_email: Custom from email address (optional, uses DEFAULT_FROM_EMAIL if not provided)
        client_name: Client identifier for logging (optional)
        fail_silently: Whether to fail silently on errors (default: False)
        
    Returns:
        Tuple of (success: bool, message: str)
        
    Examples:
        # Simple email
        success, msg = send_email(
            to_emails="user@example.com",
            subject="Welcome",
            message="Welcome to our service"
        )
        
        # Multiple recipients with HTML
        success, msg = send_email(
            to_emails=["user1@example.com", "user2@example.com"],
            subject="Update",
            message="Plain text",
            html_message="<h1>HTML</h1>",
            cc="manager@example.com",
            client_name="ClientABC"
        )
    """
    try:
        # Normalize inputs - convert single strings to lists
        def normalize_email_input(email_input: Union[str, List[str], None]) -> List[str]:
            if email_input is None:
                return []
            if isinstance(email_input, str):
                return [email_input.strip().lower()] if email_input.strip() else []
            return [email.strip().lower() for email in email_input if email and email.strip()]
        
        to_emails_list = normalize_email_input(to_emails)
        cc_list = normalize_email_input(cc)
        bcc_list = normalize_email_input(bcc)
        reply_to_list = normalize_email_input(reply_to)
        
        # Validate inputs
        if not to_emails_list:
            return False, "to_emails must be provided and non-empty"
        
        if not subject or not subject.strip():
            return False, "Subject cannot be empty"
        
        if not message and not html_message:
            return False, "Either message or html_message must be provided"
        
        # Get from email
        final_from_email = from_email
        if not final_from_email:
            final_from_email = getattr(settings, 'DEFAULT_FROM_EMAIL', None)
        if not final_from_email:
            final_from_email = os.environ.get('DEFAULT_FROM_EMAIL', 'noreply@thepyro.ai')
        
        client_id = client_name or "internal"
        
        logger.info(f"[{client_id}] Sending email to: {to_emails_list}")
        logger.info(f"[{client_id}] Subject: {subject}")
        logger.info(f"[{client_id}] From: {final_from_email}")
        if cc_list:
            logger.info(f"[{client_id}] CC: {cc_list}")
        if bcc_list:
            logger.info(f"[{client_id}] BCC: {bcc_list}")
        
        # Create email message
        if html_message:
            # Use EmailMultiAlternatives for HTML emails
            email = EmailMultiAlternatives(
                subject=subject,
                body=message or html_message,  # Fallback to HTML if no plain text
                from_email=final_from_email,
                to=to_emails_list,
                cc=cc_list if cc_list else None,
                bcc=bcc_list if bcc_list else None,
                reply_to=reply_to_list if reply_to_list else None
            )
            if html_message:
                email.attach_alternative(html_message, "text/html")
        else:
            # Use EmailMessage for plain text
            email = EmailMessage(
                subject=subject,
                body=message,
                from_email=final_from_email,
                to=to_emails_list,
                cc=cc_list if cc_list else None,
                bcc=bcc_list if bcc_list else None,
                reply_to=reply_to_list if reply_to_list else None
            )
        
        # Send email
        email.send(fail_silently=fail_silently)
        
        total_recipients = len(to_emails_list) + len(cc_list) + len(bcc_list)
        logger.info(f"[{client_id}] Email sent successfully to {total_recipients} recipient(s)")
        return True, f"Email sent successfully to {total_recipients} recipient(s)"
        
    except Exception as e:
        error_msg = f"Failed to send email: {str(e)}"
        client_id = client_name or "internal"
        logger.error(f"[{client_id}] {error_msg}", exc_info=True)
        return False, error_msg

def send_bulk_emails(
    email_list: List[Dict[str, Any]],
    client_name: Optional[str] = None,
    fail_silently: bool = False
) -> Dict[str, Any]:
    """
    Send multiple emails in bulk - internal function
    
    Args:
        email_list: List of email dicts, each with:
            - to_emails: str or List[str] (required)
            - subject: str (required)
            - message: str (optional if html_message provided)
            - html_message: str (optional if message provided)
            - cc: str or List[str] (optional)
            - bcc: str or List[str] (optional)
            - reply_to: str or List[str] (optional)
            - from_email: str (optional)
        client_name: Client identifier for logging (optional)
        fail_silently: Whether to fail silently on errors (default: False)
        
    Returns:
        Dict with 'success_count', 'failure_count', 'total', 'results' list
        
    Example:
        results = send_bulk_emails([
            {
                "to_emails": "user1@example.com",
                "subject": "Email 1",
                "message": "Message 1"
            },
            {
                "to_emails": ["user2@example.com"],
                "subject": "Email 2",
                "html_message": "<h1>Email 2</h1>"
            }
        ], client_name="Campaign")
    """
    results = []
    success_count = 0
    failure_count = 0
    client_id = client_name or "bulk"
    
    logger.info(f"[{client_id}] Sending {len(email_list)} emails in bulk")
    
    for idx, email_data in enumerate(email_list):
        try:
            success, message = send_email(
                to_emails=email_data.get('to_emails', []),
                subject=email_data.get('subject', ''),
                message=email_data.get('message', ''),
                html_message=email_data.get('html_message'),
                cc=email_data.get('cc'),
                bcc=email_data.get('bcc'),
                reply_to=email_data.get('reply_to'),
                from_email=email_data.get('from_email'),
                client_name=client_id,
                fail_silently=fail_silently
            )
            
            if success:
                success_count += 1
            else:
                failure_count += 1
            
            # Normalize to_emails for results
            to_emails = email_data.get('to_emails', [])
            if isinstance(to_emails, str):
                to_emails = [to_emails]
            
            results.append({
                'index': idx,
                'success': success,
                'message': message,
                'recipients': to_emails
            })
            
        except Exception as e:
            failure_count += 1
            to_emails = email_data.get('to_emails', [])
            if isinstance(to_emails, str):
                to_emails = [to_emails]
            results.append({
                'index': idx,
                'success': False,
                'message': f"Exception: {str(e)}",
                'recipients': to_emails
            })
    
    logger.info(f"[{client_id}] Bulk send complete: {success_count} success, {failure_count} failed")
    
    return {
        'success_count': success_count,
        'failure_count': failure_count,
        'total': len(email_list),
        'results': results
    }

