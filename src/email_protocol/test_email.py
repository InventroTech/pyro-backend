"""
Quick Email Service Test Script

Run this from Django shell:
    python manage.py shell < email_protocol/test_email.py

Or import and run:
    from email_protocol.test_email import run_tests
    run_tests()
"""

from email_protocol.services import send_email, send_bulk_emails


def run_tests():
    """Run email service tests"""
    
    # CHANGE THIS TO YOUR EMAIL ADDRESS
    TEST_EMAIL = "ritam@thepyro.ai"
    
    print("=" * 60)
    print("EMAIL SERVICE TESTING")
    print("=" * 60)
    
    # Test 1: Simple Email
    print("\n[TEST 1] Simple Email")
    print("-" * 60)
    success, message = send_email(
        to_emails=TEST_EMAIL,
        subject="Test 1: Simple Email",
        message="This is a simple test email.",
        client_name="Test1"
    )
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print(f"Message: {message}")
    
    # Test 2: HTML Email
    print("\n[TEST 2] HTML Email")
    print("-" * 60)
    success, message = send_email(
        to_emails=TEST_EMAIL,
        subject="Test 2: HTML Email",
        message="Plain text version",
        html_message="<h1>HTML Test</h1><p>This is an <b>HTML</b> email!</p>",
        client_name="Test2"
    )
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print(f"Message: {message}")
    
    # Test 3: Multiple Recipients
    print("\n[TEST 3] Multiple Recipients")
    print("-" * 60)
    success, message = send_email(
        to_emails=[TEST_EMAIL],  # Add more emails here if needed
        subject="Test 3: Multiple Recipients",
        message="This email is sent to multiple recipients.",
        client_name="Test3"
    )
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print(f"Message: {message}")
    
    # Test 4: With CC
    print("\n[TEST 4] Email with CC")
    print("-" * 60)
    success, message = send_email(
        to_emails=TEST_EMAIL,
        subject="Test 4: Email with CC",
        message="This email has a CC recipient.",
        cc=TEST_EMAIL,  # CC to same email for testing
        client_name="Test4"
    )
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print(f"Message: {message}")
    
    # Test 5: Custom From Email
    print("\n[TEST 5] Custom From Email")
    print("-" * 60)
    success, message = send_email(
        to_emails=TEST_EMAIL,
        subject="Test 5: Custom From",
        message="This email has a custom from address.",
        client_name="Test5"
    )
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print(f"Message: {message}")
    
    # Test 6: Bulk Emails
    print("\n[TEST 6] Bulk Emails")
    print("-" * 60)
    results = send_bulk_emails([
        {
            "to_emails": TEST_EMAIL,
            "subject": "Bulk Test 1",
            "message": "This is the first bulk email."
        },
        {
            "to_emails": TEST_EMAIL,
            "subject": "Bulk Test 2",
            "message": "This is the second bulk email."
        }
    ], client_name="BulkTest")
    
    print(f"Total: {results['total']}")
    print(f"Success: {results['success_count']}")
    print(f"Failed: {results['failure_count']}")
    
    print("\n" + "=" * 60)
    print("TESTING COMPLETE")
    print("=" * 60)
    print("\n📧 Check your email inbox for the test emails!")
    print("   (Check spam folder if not in inbox)")


if __name__ == "__main__":
    run_tests()

