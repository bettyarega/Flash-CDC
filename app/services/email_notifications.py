"""
Email notification service for listener errors.
"""
import os
import logging
from typing import Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import aiosmtplib

logger = logging.getLogger("email-notifications")

# Email configuration from environment variables
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", SMTP_USER)
NOTIFICATION_EMAIL = os.getenv("NOTIFICATION_EMAIL")  # Recipient email address

# Check if email is configured
EMAIL_ENABLED = bool(SMTP_HOST and SMTP_USER and SMTP_PASSWORD and NOTIFICATION_EMAIL)

# Log email configuration status on module load
if EMAIL_ENABLED:
    logger.info(f"Email notifications enabled: SMTP_HOST={SMTP_HOST}, SMTP_PORT={SMTP_PORT}, NOTIFICATION_EMAIL={NOTIFICATION_EMAIL}")
else:
    missing = []
    if not SMTP_HOST:
        missing.append("SMTP_HOST")
    if not SMTP_USER:
        missing.append("SMTP_USER")
    if not SMTP_PASSWORD:
        missing.append("SMTP_PASSWORD")
    if not NOTIFICATION_EMAIL:
        missing.append("NOTIFICATION_EMAIL")
    logger.warning(f"Email notifications disabled: missing {', '.join(missing)}")


async def send_listener_error_notification(
    client_id: int,
    client_name: str,
    error_message: str,
    topic_name: Optional[str] = None,
) -> bool:
    """
    Send an email notification when a listener stops due to an error.
    
    Args:
        client_id: The client ID
        client_name: The client name
        error_message: The error message
        topic_name: Optional topic name
        
    Returns:
        True if email was sent successfully, False otherwise
    """
    if not EMAIL_ENABLED:
        logger.warning(f"Email notifications are not configured - cannot send notification for client {client_id}")
        return False
    
    try:
        logger.info(f"Attempting to send error notification email for client {client_id} ({client_name}) to {NOTIFICATION_EMAIL}")
        
        subject = f"Listener Error: {client_name} (ID: {client_id})"
        
        body = f"""
A listener has stopped due to an error and is no longer receiving events.

Client Details:
- Client ID: {client_id}
- Client Name: {client_name}
{f"- Topic: {topic_name}" if topic_name else ""}

Error Message:
{error_message}

Please check the client configuration and restart the listener once the issue is resolved.

This is an automated notification from the Flash Admin system.
"""
        
        # Create message
        message = MIMEMultipart()
        message["From"] = SMTP_FROM_EMAIL
        message["To"] = NOTIFICATION_EMAIL
        message["Subject"] = subject
        
        message.attach(MIMEText(body, "plain"))
        
        # Port 465 typically uses SSL (implicit), port 587 uses TLS (STARTTLS)
        # aiosmtplib handles SSL automatically for port 465
        # For port 587, we use STARTTLS if SMTP_USE_TLS is true
        if SMTP_PORT == 465:
            # Port 465 uses SSL implicitly - aiosmtplib handles this automatically
            # Don't use start_tls for port 465
            await aiosmtplib.send(
                message,
                hostname=SMTP_HOST,
                port=SMTP_PORT,
                username=SMTP_USER,
                password=SMTP_PASSWORD,
                start_tls=False,  # Port 465 uses implicit SSL, not STARTTLS
            )
        else:
            # Port 587 or other ports use STARTTLS
            await aiosmtplib.send(
                message,
                hostname=SMTP_HOST,
                port=SMTP_PORT,
                username=SMTP_USER,
                password=SMTP_PASSWORD,
                start_tls=SMTP_USE_TLS,  # Use STARTTLS for port 587
            )
        
        logger.info(f"Successfully sent error notification email for client {client_id} ({client_name}) to {NOTIFICATION_EMAIL}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send error notification email for client {client_id}: {e}", exc_info=True)
        return False

