# Email Protocol - Internal Service

This is an **internal email service** designed to be called directly from your code. 
No need for API endpoints - just import and use the functions!

## Quick Start

```python
from email_protocol.services import send_email, send_bulk_emails

# Simple email
success, message = send_email(
    to_emails="user@example.com",
    subject="Welcome!",
    message="Welcome to our service"
)

if success:
    print("Email sent!")
else:
    print(f"Error: {message}")
```

## Main Function: `send_email()`

This is the **one unified function** that handles all email scenarios.

### Function Signature

```python
def send_email(
    to_emails: Union[str, List[str]],      # Single email or list
    subject: str,                           # Required
    message: str = "",                      # Plain text (optional if html_message provided)
    html_message: Optional[str] = None,    # HTML content (optional if message provided)
    cc: Optional[Union[str, List[str]]] = None,
    bcc: Optional[Union[str, List[str]]] = None,
    reply_to: Optional[Union[str, List[str]]] = None,
    from_email: Optional[str] = None,       # Custom from address
    client_name: Optional[str] = None,      # For logging/tracking
    fail_silently: bool = False
) -> Tuple[bool, str]:
    """
    Returns: (success: bool, message: str)
    """
```

### Examples

#### 1. Simple Email (Single Recipient)
```python
from email_protocol.services import send_email

success, msg = send_email(
    to_emails="client@example.com",
    subject="Welcome!",
    message="Thank you for joining us!"
)
```

#### 2. Multiple Recipients
```python
success, msg = send_email(
    to_emails=["user1@example.com", "user2@example.com"],
    subject="Team Update",
    message="Here's the latest update for the team."
)
```

#### 3. HTML Email
```python
success, msg = send_email(
    to_emails="user@example.com",
    subject="Newsletter",
    message="Plain text version",
    html_message="<h1>Newsletter</h1><p>HTML version</p>"
)
```

#### 4. With CC and BCC
```python
success, msg = send_email(
    to_emails="client@example.com",
    subject="Invoice",
    message="Your invoice is attached.",
    cc="accounting@example.com",
    bcc="archive@example.com"
)
```

#### 5. Custom From Address
```python
success, msg = send_email(
    to_emails="user@example.com",
    subject="Support Response",
    message="We've received your support request.",
    from_email="support@mycompany.com",
    reply_to="support@mycompany.com"
)
```

#### 6. With Client Tracking
```python
success, msg = send_email(
    to_emails="user@example.com",
    subject="Welcome",
    message="Welcome!",
    client_name="ClientABC"  # For logging/tracking
)
```

## Bulk Emails: `send_bulk_emails()`

Send multiple different emails in one call.

```python
from email_protocol.services import send_bulk_emails

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

print(f"Sent {results['success_count']} emails successfully")
print(f"Failed: {results['failure_count']}")
```

## Usage in Your Code

### Example: In a View
```python
from email_protocol.services import send_email

def my_view(request):
    # ... your code ...
    
    # Send email
    success, message = send_email(
        to_emails=user.email,
        subject="Action Required",
        message="Please complete your profile.",
        html_message="<h1>Action Required</h1><p>Please complete your profile.</p>",
        client_name="ProfileCompletion"
    )
    
    if success:
        # Email sent successfully
        pass
    else:
        # Handle error
        logger.error(f"Failed to send email: {message}")
```

### Example: In a Service/Utility
```python
from email_protocol.services import send_email

def notify_user(user_email, notification_type):
    """Send notification email to user"""
    success, msg = send_email(
        to_emails=user_email,
        subject=f"New {notification_type}",
        message=f"You have a new {notification_type}.",
        client_name="NotificationService"
    )
    return success
```

## Features

✅ **Single email or multiple recipients** - Pass string or list  
✅ **Plain text and HTML** - Support both formats  
✅ **CC and BCC** - Carbon copy and blind carbon copy  
✅ **Custom from address** - Override default sender  
✅ **Reply-to address** - Set custom reply-to  
✅ **Client tracking** - Log emails by client name  
✅ **Flexible input** - Accept strings or lists for all email fields  
✅ **Error handling** - Returns success status and message  

## Return Values

### `send_email()` returns:
```python
(success: bool, message: str)
```

- `success = True`: Email sent successfully
- `success = False`: Email failed, check `message` for error details

### `send_bulk_emails()` returns:
```python
{
    'success_count': int,
    'failure_count': int,
    'total': int,
    'results': [
        {
            'index': int,
            'success': bool,
            'message': str,
            'recipients': List[str]
        },
        ...
    ]
}
```

## Notes

- All email addresses are automatically normalized (lowercased, trimmed)
- You can pass a single email string or a list of emails for any recipient field
- Either `message` or `html_message` must be provided (or both)
- The service uses your Django email configuration from settings.py
- All emails are logged with client_name for tracking

## Configuration

Make sure your `.env` file has:
```env
EMAIL_BACKEND=email_protocol.backends.CustomSMTPBackend
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_USE_TLS=True
EMAIL_HOST_USER=your-email@gmail.com
EMAIL_HOST_PASSWORD=your-app-password
DEFAULT_FROM_EMAIL=your-email@gmail.com
EMAIL_SSL_VERIFY=False  # For development
```

That's it! Just import and use. No endpoints needed for internal use.

