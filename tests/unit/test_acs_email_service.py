import pytest
from unittest.mock import Mock, patch
from corvus_python.email.acs_email_service import AcsEmailService
from corvus_python.email.models import EmailContent, EmailRecipient, EmailRecipients, EmailAttachment
from corvus_python.email.errors import EmailError


class TestAcsEmailService:
    def test_acs_email_service_initialization(self):
        with patch("corvus_python.email.acs_email_service.EmailClient") as mock_email_client:
            mock_client_instance = Mock()
            mock_email_client.from_connection_string.return_value = mock_client_instance

            service = AcsEmailService(acs_connection_string="test_connection_string", from_email="sender@example.com")

            assert service.acs_connection_string == "test_connection_string"
            assert service.from_email == "sender@example.com"
            assert service.email_sending_disabled is False
            assert service.email_client == mock_client_instance
            assert service.logger is not None

            mock_email_client.from_connection_string.assert_called_once_with("test_connection_string")

    def test_acs_email_service_initialization_with_disabled_sending(self):
        with patch("corvus_python.email.acs_email_service.EmailClient") as mock_email_client:
            mock_client_instance = Mock()
            mock_email_client.from_connection_string.return_value = mock_client_instance

            service = AcsEmailService(
                acs_connection_string="test_connection_string",
                from_email="sender@example.com",
                email_sending_disabled=True,
            )

            assert service.email_sending_disabled is True

    def test_send_email_success(self):
        with patch("corvus_python.email.acs_email_service.EmailClient") as mock_email_client:
            # Setup mocks
            mock_client_instance = Mock()
            mock_email_client.from_connection_string.return_value = mock_client_instance

            mock_poller = Mock()
            mock_result = {"status": "Succeeded"}
            mock_poller.result.return_value = mock_result
            mock_client_instance.begin_send.return_value = mock_poller

            # Create service
            service = AcsEmailService(acs_connection_string="test_connection_string", from_email="sender@example.com")

            # Mock logger to capture log calls
            service.logger = Mock()

            # Create test data
            content = EmailContent(subject="Test Subject", plain_text="Plain text", html="<p>HTML content</p>")
            recipients = EmailRecipients(to=[EmailRecipient("recipient@example.com", "Recipient")])

            # Call send_email
            service.send_email(content, recipients)

            # Verify email client was called correctly
            mock_client_instance.begin_send.assert_called_once()
            call_args = mock_client_instance.begin_send.call_args[0][0]

            assert call_args["senderAddress"] == "sender@example.com"
            assert call_args["content"]["subject"] == "Test Subject"
            assert call_args["recipients"]["to"][0]["address"] == "recipient@example.com"

            # Verify success log
            service.logger.info.assert_called_once()
            assert "Email message sent successfully" in service.logger.info.call_args[0][0]

    def test_send_email_with_attachments(self):
        with patch("corvus_python.email.acs_email_service.EmailClient") as mock_email_client:
            # Setup mocks
            mock_client_instance = Mock()
            mock_email_client.from_connection_string.return_value = mock_client_instance

            mock_poller = Mock()
            mock_result = {"status": "Succeeded"}
            mock_poller.result.return_value = mock_result
            mock_client_instance.begin_send.return_value = mock_poller

            # Create service
            service = AcsEmailService(acs_connection_string="test_connection_string", from_email="sender@example.com")
            service.logger = Mock()

            # Create test data with attachments
            content = EmailContent(subject="Test Subject", plain_text="Plain text", html="<p>HTML content</p>")
            recipients = EmailRecipients(to=[EmailRecipient("recipient@example.com", "Recipient")])
            attachments = [
                EmailAttachment(name="test.pdf", content_type="application/pdf", content_in_base64="base64content")
            ]

            # Call send_email
            service.send_email(content, recipients, attachments)

            # Verify email client was called with attachments
            call_args = mock_client_instance.begin_send.call_args[0][0]
            assert "attachments" in call_args
            assert len(call_args["attachments"]) == 1
            assert call_args["attachments"][0]["name"] == "test.pdf"

    def test_send_email_disabled(self):
        with patch("corvus_python.email.acs_email_service.EmailClient") as mock_email_client:
            mock_client_instance = Mock()
            mock_email_client.from_connection_string.return_value = mock_client_instance

            # Create service with email sending disabled
            service = AcsEmailService(
                acs_connection_string="test_connection_string",
                from_email="sender@example.com",
                email_sending_disabled=True,
            )
            service.logger = Mock()

            # Create test data
            content = EmailContent(subject="Test Subject", plain_text="Plain text", html="<p>HTML content</p>")
            recipients = EmailRecipients(to=[EmailRecipient("recipient@example.com", "Recipient")])

            # Call send_email
            service.send_email(content, recipients)

            # Verify email client was NOT called
            mock_client_instance.begin_send.assert_not_called()

            # Verify warning log
            service.logger.warning.assert_called_once_with("Email sending disabled.")

    def test_send_email_failure_raises_error(self):
        with patch("corvus_python.email.acs_email_service.EmailClient") as mock_email_client:
            # Setup mocks for failure scenario
            mock_client_instance = Mock()
            mock_email_client.from_connection_string.return_value = mock_client_instance

            mock_poller = Mock()
            mock_result = {"status": "Failed", "error": "Authentication failed"}
            mock_poller.result.return_value = mock_result
            mock_client_instance.begin_send.return_value = mock_poller

            # Create service
            service = AcsEmailService(acs_connection_string="test_connection_string", from_email="sender@example.com")

            # Create test data
            content = EmailContent(subject="Test Subject", plain_text="Plain text", html="<p>HTML content</p>")
            recipients = EmailRecipients(to=[EmailRecipient("recipient@example.com", "Recipient")])

            # Verify EmailError is raised
            with pytest.raises(EmailError) as exc_info:
                service.send_email(content, recipients)

            assert "Authentication failed" in str(exc_info.value)

    def test_send_email_without_attachments_no_attachments_key(self):
        with patch("corvus_python.email.acs_email_service.EmailClient") as mock_email_client:
            # Setup mocks
            mock_client_instance = Mock()
            mock_email_client.from_connection_string.return_value = mock_client_instance

            mock_poller = Mock()
            mock_result = {"status": "Succeeded"}
            mock_poller.result.return_value = mock_result
            mock_client_instance.begin_send.return_value = mock_poller

            # Create service
            service = AcsEmailService(acs_connection_string="test_connection_string", from_email="sender@example.com")
            service.logger = Mock()

            # Create test data without attachments
            content = EmailContent(subject="Test Subject", plain_text="Plain text", html="<p>HTML content</p>")
            recipients = EmailRecipients(to=[EmailRecipient("recipient@example.com", "Recipient")])

            # Call send_email without attachments
            service.send_email(content, recipients)

            # Verify email client was called without attachments key
            call_args = mock_client_instance.begin_send.call_args[0][0]
            assert "attachments" not in call_args

    def test_send_email_with_empty_attachments_list(self):
        with patch("corvus_python.email.acs_email_service.EmailClient") as mock_email_client:
            # Setup mocks
            mock_client_instance = Mock()
            mock_email_client.from_connection_string.return_value = mock_client_instance

            mock_poller = Mock()
            mock_result = {"status": "Succeeded"}
            mock_poller.result.return_value = mock_result
            mock_client_instance.begin_send.return_value = mock_poller

            # Create service
            service = AcsEmailService(acs_connection_string="test_connection_string", from_email="sender@example.com")
            service.logger = Mock()

            # Create test data with empty attachments list
            content = EmailContent(subject="Test Subject", plain_text="Plain text", html="<p>HTML content</p>")
            recipients = EmailRecipients(to=[EmailRecipient("recipient@example.com", "Recipient")])

            # Call send_email with empty attachments list
            service.send_email(content, recipients, [])

            # Verify email client was called without attachments key
            call_args = mock_client_instance.begin_send.call_args[0][0]
            assert "attachments" not in call_args
