import logging
from typing import Any, List, MutableMapping

from azure.communication.email import EmailClient
from azure.core.polling._poller import LROPoller

from .models import EmailContent, EmailRecipients, EmailAttachment
from .errors import EmailError


class AcsEmailService:

    def __init__(self, acs_connection_string: str, from_email: str, email_sending_disabled: bool = False) -> None:
        self.acs_connection_string: str = acs_connection_string
        self.from_email: str = from_email
        self.email_client: EmailClient = EmailClient.from_connection_string(acs_connection_string)
        self.email_sending_disabled: bool = email_sending_disabled
        self.logger: logging.Logger = logging.getLogger(__name__)

    def send_email(
        self,
        content: EmailContent,
        recipients: EmailRecipients,
        attachments: List[EmailAttachment] = [],
    ) -> None:
        message: dict[str, Any] = {
            "content": content.to_dict(),
            "recipients": recipients.to_dict(),
        }

        if attachments:
            message["attachments"] = [x.to_dict() for x in attachments]

        if not self.email_sending_disabled:
            message["senderAddress"] = self.from_email

            poller: LROPoller[MutableMapping[str, Any]] = self.email_client.begin_send(message)
            result: MutableMapping[str, Any] = poller.result()

            if result["status"] != "Succeeded":
                raise EmailError(result["error"])

            self.logger.info(f"Email message sent successfully. Message:\n\n{message}")
        else:
            self.logger.warning("Email sending disabled.")
