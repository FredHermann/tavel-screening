import pytest
import json
from unittest.mock import patch, MagicMock
import boto3
import os
import sys

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Import the Lambda functions
from appointment_request_processor import lambda_handler as request_handler
from appointment_confirmation_processor import lambda_handler as confirmation_handler
from appointment_reminder_processor import lambda_handler as reminder_handler

class TestIntegration:
    """Integration tests using mocked AWS services."""
    
    @patch('appointment_request_processor.dynamodb')
    @patch('appointment_confirmation_processor.dynamodb')
    @patch('appointment_reminder_processor.dynamodb')
    @patch('appointment_request_processor.sqs')
    @patch('appointment_confirmation_processor.sqs')
    @patch('appointment_reminder_processor.sqs')
    @patch('appointment_request_processor.datetime')
    @patch('appointment_confirmation_processor.datetime')
    @patch('appointment_reminder_processor.datetime')
    @patch('appointment_request_processor.uuid.uuid4')
    def test_end_to_end_appointment_flow(self, mock_uuid, mock_request_datetime, mock_reminder_datetime, 
                                       mock_confirmation_datetime, mock_reminder_sqs, mock_confirmation_sqs, 
                                       mock_request_sqs, mock_reminder_dynamodb, 
                                       mock_confirmation_dynamodb, mock_request_dynamodb):
        """Test the complete flow from appointment request to confirmation."""
        # Set up test environment
        os.environ['APPOINTMENTS_TABLE'] = 'test-appointments'
        os.environ['PATIENTS_TABLE'] = 'test-patients'
        os.environ['CONFIRMATION_QUEUE_URL'] = 'https://sqs.us-east-1.amazonaws.com/123456789012/test-confirmation-queue'
        os.environ['REMINDER_QUEUE_URL'] = 'https://sqs.us-east-1.amazonaws.com/123456789012/test-reminder-queue'
        
        # Mock datetime to return a fixed date (2024-01-01)
        from datetime import datetime
        mock_request_datetime.now.return_value = datetime(2024, 1, 1)
        mock_request_datetime.utcnow.return_value = datetime(2024, 1, 1)
        mock_request_datetime.strptime.side_effect = lambda date_str, format_str: datetime.strptime(date_str, format_str)
        
        mock_confirmation_datetime.now.return_value = datetime(2024, 1, 1)
        mock_confirmation_datetime.utcnow.return_value = datetime(2024, 1, 1)
        mock_confirmation_datetime.strptime.side_effect = lambda date_str, format_str: datetime.strptime(date_str, format_str)
        
        mock_reminder_datetime.now.return_value = datetime(2024, 1, 1)
        mock_reminder_datetime.utcnow.return_value = datetime(2024, 1, 1)
        mock_reminder_datetime.strptime.side_effect = lambda date_str, format_str: datetime.strptime(date_str, format_str)
        
        # Mock uuid to return a fixed ID
        mock_uuid.return_value = 'appointment-123'
        
        # Mock DynamoDB tables
        mock_appointments_table = MagicMock()
        mock_patients_table = MagicMock()
        
        # Set up the mock tables for all Lambda functions
        mock_request_dynamodb.Table.return_value = mock_appointments_table
        mock_confirmation_dynamodb.Table.return_value = mock_appointments_table
        mock_reminder_dynamodb.Table.return_value = mock_appointments_table
        
        # Mock patient table for request processor
        mock_request_patients_table = MagicMock()
        mock_request_dynamodb.Table.side_effect = lambda name: mock_patients_table if 'patients' in name else mock_appointments_table
        
        # Mock SQS clients
        mock_request_sqs.send_message.return_value = {'MessageId': 'msg-123'}
        mock_confirmation_sqs.send_message.return_value = {'MessageId': 'msg-reminder-123'}
        
        # Mock patient exists
        mock_patients_table.get_item.return_value = {
            'Item': {
                'patientId': 'patient-123',
                'firstName': 'John',
                'lastName': 'Doe',
                'email': 'john.doe@example.com'
            }
        }
        
        # Mock no appointment conflicts
        mock_appointments_table.query.return_value = {'Items': []}
        
        # Mock appointment creation
        mock_appointments_table.put_item.return_value = None
        
        # Test 1: Process appointment request
        appointment_request_event = {
            'Records': [
                {
                    'messageId': 'msg-123',
                    'body': json.dumps({
                        'patientId': 'patient-123',
                        'appointmentDate': '2024-01-02',
                        'startTime': '10:00',
                        'endTime': '11:00',
                        'notes': 'Regular checkup'
                    })
                }
            ]
        }
        
        result = request_handler(appointment_request_event, {})
        assert result['successful'] == 1
        assert result['failed'] == 0
        
        # Verify DynamoDB was called
        mock_appointments_table.put_item.assert_called_once()
        
        # Verify SQS was called
        mock_request_sqs.send_message.assert_called_once()
        
        # Test 2: Process confirmation message
        confirmation_message = {
            'appointmentId': 'appointment-123',
            'patientId': 'patient-123',
            'action': 'CONFIRM',
            'timestamp': '2024-01-01T00:00:00Z'
        }
        
        confirmation_event = {
            'Records': [
                {
                    'messageId': 'msg-confirm-123',
                    'body': json.dumps(confirmation_message)
                }
            ]
        }
        
        # Mock appointment retrieval for confirmation
        mock_appointments_table.get_item.return_value = {
            'Item': {
                'appointmentId': 'appointment-123',
                'patientId': 'patient-123',
                'status': 'REQUESTED',
                'appointmentDate': '2024-01-02',
                'startTime': '10:00',
                'endTime': '11:00'
            }
        }
        
        result = confirmation_handler(confirmation_event, {})
        assert result['successful'] == 1
        assert result['failed'] == 0
        
        # Verify appointment status was updated
        mock_appointments_table.update_item.assert_called()
        
        # Verify reminder message was sent to SQS
        mock_confirmation_sqs.send_message.assert_called_once()
        
        # Test 3: Process reminder message
        reminder_message = {
            'appointmentId': 'appointment-123',
            'patientId': 'patient-123',
            'reminderTime': '2025-01-15T10:00:00',
            'appointmentDate': '2025-01-15',
            'startTime': '10:00',
            'endTime': '11:00',
            'patientName': 'John Doe',
            'patientEmail': 'john.doe@example.com'
        }
        
        reminder_event = {
            'Records': [
                {
                    'messageId': 'msg-reminder-123',
                    'body': json.dumps(reminder_message)
                }
            ]
        }
        
        # Mock appointment retrieval for reminder
        mock_appointments_table.get_item.return_value = {
            'Item': {
                'appointmentId': 'appointment-123',
                'patientId': 'patient-123',
                'status': 'CONFIRMED',
                'appointmentDate': '2024-01-02',
                'startTime': '10:00',
                'endTime': '11:00'
            }
        }
        
        result = reminder_handler(reminder_event, {})
        assert result['successful'] == 1
        assert result['failed'] == 0
        
        # Verify reminder was processed (check for reminderSent flag update)
        mock_appointments_table.update_item.assert_called()
    
    @patch('appointment_request_processor.dynamodb')
    @patch('appointment_request_processor.datetime')
    def test_patient_not_found_scenario(self, mock_datetime, mock_dynamodb):
        """Test appointment request fails when patient doesn't exist."""
        # Set up test environment
        os.environ['APPOINTMENTS_TABLE'] = 'test-appointments'
        os.environ['PATIENTS_TABLE'] = 'test-patients'
        os.environ['CONFIRMATION_QUEUE_URL'] = 'https://sqs.us-east-1.amazonaws.com/123456789012/test-confirmation-queue'
        
        # Mock datetime to return a fixed date (2024-01-01)
        from datetime import datetime
        mock_datetime.now.return_value = datetime(2024, 1, 1)
        mock_datetime.strptime.side_effect = lambda date_str, format_str: datetime.strptime(date_str, format_str)
        
        # Mock DynamoDB tables
        mock_appointments_table = MagicMock()
        mock_patients_table = MagicMock()
        
        # Set up the mock tables
        mock_dynamodb.Table.side_effect = lambda name: mock_patients_table if 'patients' in name else mock_appointments_table
        
        # Mock patient not found
        mock_patients_table.get_item.return_value = {}
        
        # Try to create appointment for non-existent patient
        appointment_request_event = {
            'Records': [
                {
                    'messageId': 'msg-123',
                    'body': json.dumps({
                        'patientId': 'non-existent-patient',
                        'appointmentDate': '2025-01-15',
                        'startTime': '10:00',
                        'endTime': '11:00'
                    })
                }
            ]
        }
        
        result = request_handler(appointment_request_event, {})
        assert result['successful'] == 0
        assert result['failed'] == 1
        assert len(result['errors']) > 0
        assert any('not found' in error.lower() for error in result['errors'])
        
        # Verify no appointment was created
        mock_appointments_table.put_item.assert_not_called()
    
    @patch('appointment_request_processor.dynamodb')
    @patch('appointment_request_processor.datetime')
    def test_appointment_conflict_scenario(self, mock_datetime, mock_dynamodb):
        """Test appointment request fails when there's a time conflict."""
        # Set up test environment
        os.environ['APPOINTMENTS_TABLE'] = 'test-appointments'
        os.environ['PATIENTS_TABLE'] = 'test-patients'
        os.environ['CONFIRMATION_QUEUE_URL'] = 'https://sqs.us-east-1.amazonaws.com/123456789012/test-confirmation-queue'
        
        # Mock datetime to return a fixed date (2024-01-01)
        from datetime import datetime
        mock_datetime.now.return_value = datetime(2024, 1, 1)
        mock_datetime.strptime.side_effect = lambda date_str, format_str: datetime.strptime(date_str, format_str)
        
        # Mock DynamoDB tables
        mock_appointments_table = MagicMock()
        mock_patients_table = MagicMock()
        
        # Set up the mock tables
        mock_dynamodb.Table.side_effect = lambda name: mock_patients_table if 'patients' in name else mock_appointments_table
        
        # Mock patient exists
        mock_patients_table.get_item.return_value = {
            'Item': {
                'patientId': 'patient-123',
                'firstName': 'John',
                'lastName': 'Doe',
                'email': 'john.doe@example.com'
            }
        }
        
        # Mock existing appointment that conflicts
        mock_appointments_table.query.return_value = {
            'Items': [{
                'startTime': '10:00',
                'endTime': '11:00'
            }]
        }
        
        # Try to create conflicting appointment
        appointment_request_event = {
            'Records': [
                {
                    'messageId': 'msg-123',
                    'body': json.dumps({
                        'patientId': 'patient-123',
                        'appointmentDate': '2025-01-15',
                        'startTime': '10:30',  # Overlaps with 10:00-11:00
                        'endTime': '11:30'
                    })
                }
            ]
        }
        
        result = request_handler(appointment_request_event, {})
        assert result['successful'] == 0
        assert result['failed'] == 1
        assert len(result['errors']) > 0
        assert any('conflict' in error.lower() for error in result['errors'])
        
        # Verify no new appointment was created
        mock_appointments_table.put_item.assert_not_called()
