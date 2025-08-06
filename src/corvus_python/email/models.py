from dataclasses import dataclass, field
from typing import List


@dataclass
class EmailContent:
    subject: str
    plain_text: str
    html: str

    def to_dict(self) -> dict[str, str]:
        return {
            "subject": self.subject,
            "plainText": self.plain_text,
            "html": self.html,
        }


@dataclass
class EmailRecipient:
    address: str
    display_name: str

    def to_dict(self) -> dict[str, str]:
        return {"address": self.address, "displayName": self.display_name}


@dataclass
class EmailRecipients:
    to: List[EmailRecipient]
    cc: List[EmailRecipient] = field(default_factory=list)
    bcc: List[EmailRecipient] = field(default_factory=list)

    def to_dict(self) -> dict[str, list[dict[str, str]]]:
        return {
            "to": [x.to_dict() for x in self.to],
            "cc": [x.to_dict() for x in self.cc],
            "bcc": [x.to_dict() for x in self.bcc],
        }


@dataclass
class EmailAttachment:
    name: str
    content_type: str
    content_in_base64: str

    def to_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "contentType": self.content_type,
            "contentInBase64": self.content_in_base64,
        }
