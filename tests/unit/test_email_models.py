from corvus_python.email.models import EmailContent, EmailRecipient, EmailRecipients, EmailAttachment


class TestEmailContent:
    def test_email_content_initialization(self):
        content = EmailContent(subject="Test Subject", plain_text="Plain text content", html="<p>HTML content</p>")

        assert content.subject == "Test Subject"
        assert content.plain_text == "Plain text content"
        assert content.html == "<p>HTML content</p>"

    def test_email_content_to_dict(self):
        content = EmailContent(subject="Test Subject", plain_text="Plain text content", html="<p>HTML content</p>")

        expected_dict = {"subject": "Test Subject", "plainText": "Plain text content", "html": "<p>HTML content</p>"}

        assert content.to_dict() == expected_dict


class TestEmailRecipient:
    def test_email_recipient_initialization(self):
        recipient = EmailRecipient(address="test@example.com", display_name="Test User")

        assert recipient.address == "test@example.com"
        assert recipient.display_name == "Test User"

    def test_email_recipient_to_dict(self):
        recipient = EmailRecipient(address="test@example.com", display_name="Test User")

        expected_dict = {"address": "test@example.com", "displayName": "Test User"}

        assert recipient.to_dict() == expected_dict


class TestEmailRecipients:
    def test_email_recipients_initialization_with_defaults(self):
        recipient_to = EmailRecipient("to@example.com", "To User")
        recipients = EmailRecipients(to=[recipient_to])

        assert len(recipients.to) == 1
        assert recipients.to[0] == recipient_to
        assert recipients.cc == []
        assert recipients.bcc == []

    def test_email_recipients_initialization_with_all_fields(self):
        recipient_to = EmailRecipient("to@example.com", "To User")
        recipient_cc = EmailRecipient("cc@example.com", "CC User")
        recipient_bcc = EmailRecipient("bcc@example.com", "BCC User")

        recipients = EmailRecipients(to=[recipient_to], cc=[recipient_cc], bcc=[recipient_bcc])

        assert len(recipients.to) == 1
        assert len(recipients.cc) == 1
        assert len(recipients.bcc) == 1
        assert recipients.to[0] == recipient_to
        assert recipients.cc[0] == recipient_cc
        assert recipients.bcc[0] == recipient_bcc

    def test_email_recipients_to_dict(self):
        recipient_to = EmailRecipient("to@example.com", "To User")
        recipient_cc = EmailRecipient("cc@example.com", "CC User")
        recipient_bcc = EmailRecipient("bcc@example.com", "BCC User")

        recipients = EmailRecipients(to=[recipient_to], cc=[recipient_cc], bcc=[recipient_bcc])

        expected_dict = {
            "to": [{"address": "to@example.com", "displayName": "To User"}],
            "cc": [{"address": "cc@example.com", "displayName": "CC User"}],
            "bcc": [{"address": "bcc@example.com", "displayName": "BCC User"}],
        }

        assert recipients.to_dict() == expected_dict

    def test_email_recipients_multiple_recipients_per_type(self):
        recipients = EmailRecipients(
            to=[EmailRecipient("to1@example.com", "To User 1"), EmailRecipient("to2@example.com", "To User 2")],
            cc=[EmailRecipient("cc1@example.com", "CC User 1"), EmailRecipient("cc2@example.com", "CC User 2")],
        )

        result_dict = recipients.to_dict()

        assert len(result_dict["to"]) == 2
        assert len(result_dict["cc"]) == 2
        assert len(result_dict["bcc"]) == 0

        assert result_dict["to"][0]["address"] == "to1@example.com"
        assert result_dict["to"][1]["address"] == "to2@example.com"
        assert result_dict["cc"][0]["address"] == "cc1@example.com"
        assert result_dict["cc"][1]["address"] == "cc2@example.com"


class TestEmailAttachment:
    def test_email_attachment_initialization(self):
        attachment = EmailAttachment(
            name="test_file.pdf", content_type="application/pdf", content_in_base64="base64encodedcontent"
        )

        assert attachment.name == "test_file.pdf"
        assert attachment.content_type == "application/pdf"
        assert attachment.content_in_base64 == "base64encodedcontent"

    def test_email_attachment_to_dict(self):
        attachment = EmailAttachment(
            name="test_file.pdf", content_type="application/pdf", content_in_base64="base64encodedcontent"
        )

        expected_dict = {
            "name": "test_file.pdf",
            "contentType": "application/pdf",
            "contentInBase64": "base64encodedcontent",
        }

        assert attachment.to_dict() == expected_dict
