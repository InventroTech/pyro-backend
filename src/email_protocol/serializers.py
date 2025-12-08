from rest_framework import serializers


class SendEmailSerializer(serializers.Serializer):
    """
    Serializer for sending email to client (simple version - backward compatible)
    """
    email = serializers.EmailField(required=True, help_text="Client's email address")
    subject = serializers.CharField(required=True, max_length=200, help_text="Email subject")
    message = serializers.CharField(required=True, help_text="Email message body")
    html_message = serializers.CharField(required=False, allow_blank=True, help_text="Optional HTML message body")
    
    def validate_email(self, value):
        """Validate email format"""
        if not value or not value.strip():
            raise serializers.ValidationError("Email cannot be empty")
        return value.strip().lower()
    
    def validate_subject(self, value):
        """Validate subject"""
        if not value or not value.strip():
            raise serializers.ValidationError("Subject cannot be empty")
        return value.strip()
    
    def validate_message(self, value):
        """Validate message"""
        if not value or not value.strip():
            raise serializers.ValidationError("Message cannot be empty")
        return value.strip()


class SendAdvancedEmailSerializer(serializers.Serializer):
    """
    Advanced serializer for sending emails with full flexibility
    """
    to_emails = serializers.ListField(
        child=serializers.EmailField(),
        required=True,
        min_length=1,
        help_text="List of recipient email addresses"
    )
    subject = serializers.CharField(required=True, max_length=200, help_text="Email subject")
    message = serializers.CharField(required=False, allow_blank=True, help_text="Plain text message body")
    html_message = serializers.CharField(required=False, allow_blank=True, help_text="HTML message body")
    cc = serializers.ListField(
        child=serializers.EmailField(),
        required=False,
        allow_empty=True,
        help_text="Optional list of CC email addresses"
    )
    bcc = serializers.ListField(
        child=serializers.EmailField(),
        required=False,
        allow_empty=True,
        help_text="Optional list of BCC email addresses"
    )
    reply_to = serializers.ListField(
        child=serializers.EmailField(),
        required=False,
        allow_empty=True,
        help_text="Optional list of reply-to email addresses"
    )
    from_email = serializers.EmailField(required=False, help_text="Optional custom from email address")
    client_name = serializers.CharField(required=False, max_length=100, help_text="Optional client identifier for logging")
    
    def validate(self, data):
        """Validate that at least message or html_message is provided"""
        message = data.get('message', '').strip()
        html_message = data.get('html_message', '').strip()
        
        if not message and not html_message:
            raise serializers.ValidationError("Either 'message' or 'html_message' must be provided")
        
        return data


class BulkEmailItemSerializer(serializers.Serializer):
    """Serializer for individual email in bulk send"""
    to_emails = serializers.ListField(
        child=serializers.EmailField(),
        required=True,
        min_length=1
    )
    subject = serializers.CharField(required=True, max_length=200)
    message = serializers.CharField(required=False, allow_blank=True)
    html_message = serializers.CharField(required=False, allow_blank=True)
    cc = serializers.ListField(child=serializers.EmailField(), required=False, allow_empty=True)
    bcc = serializers.ListField(child=serializers.EmailField(), required=False, allow_empty=True)
    reply_to = serializers.ListField(child=serializers.EmailField(), required=False, allow_empty=True)
    from_email = serializers.EmailField(required=False)


class BulkSendEmailSerializer(serializers.Serializer):
    """Serializer for bulk email sending"""
    emails = serializers.ListField(
        child=BulkEmailItemSerializer(),
        required=True,
        min_length=1,
        help_text="List of email objects to send"
    )
    client_name = serializers.CharField(required=False, max_length=100, help_text="Optional client identifier")

