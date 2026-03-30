"""Email module for sending emails via Azure Communication Services.

This module provides utilities for sending emails through Azure Communication Services (ACS),
including models for email content, recipients, and attachments.
"""

from .acs_email_service import AcsEmailService
from .models import EmailContent, EmailRecipient, EmailRecipients, EmailAttachment
from .errors import EmailError

__all__ = ["AcsEmailService", "EmailContent", "EmailRecipient", "EmailRecipients", "EmailAttachment", "EmailError"]
