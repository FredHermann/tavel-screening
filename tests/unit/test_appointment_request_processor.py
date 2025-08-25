import pytest
import json
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Import the functions to test
from appointment_request_processor import (
    validate_appointment_request,
    check_patient_exists,
    check_appointment_conflicts,
    create_appointment,
    send_confirmation_message,
    lambda_handler
)

class TestValidateAppointmentRequest:
    """Test cases for appointment request validation."""
    
    def test_valid_appointment_request(self, sample_appointment_request):
        """Test that a valid appointment request passes validation."""
        errors = validate_appointment_request(sample_appointment_request)
        assert len(errors) == 0
    
    def test_missing_required_fields(self):
        """Test validation fails when required fields are missing."""
        invalid_request = {
            'patientId': 'patient-123',
            'appointmentDate': '2024-01-02'
            # Missing startTime and endTime
        }
        errors = validate_appointment_request(invalid_request)
        assert len(errors) > 0
        assert any('required' in error.lower() for error in errors)
    
    def test_past_appointment_date(self):
        """Test validation fails for past appointment dates."""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        invalid_request = {
            'patientId': 'patient-123',
            'appointmentDate': yesterday,
            'startTime': '10:00',
            'endTime': '11:00'
        }
        errors = validate_appointment_request(invalid_request)
        assert len(errors) > 0
        assert any('past' in error.lower() for error in errors)
    
    def test_invalid_time_format(self):
        """Test validation fails for invalid time formats."""
        invalid_request = {
            'patientId': 'patient-123',
            'appointmentDate': '2024-01-02',
            'startTime': '25:00',  # Invalid hour
            'endTime': '11:00'
        }
        errors = validate_appointment_request(invalid_request)
        assert len(errors) > 0
        assert any('schema validation' in error.lower() for error in errors)
    
    def test_start_time_after_end_time(self):
        """Test validation fails when start time is after end time."""
        invalid_request = {
            'patientId': 'patient-123',
            'appointmentDate': '2024-01-02',
            'startTime': '11:00',
            'endTime': '10:00'  # End before start
        }
        errors = validate_appointment_request(invalid_request)
        assert len(errors) > 0
        assert any('before' in error.lower() for error in errors)
    
    def test_outside_business_hours(self):
        """Test validation fails for appointments outside business hours."""
        invalid_request = {
            'patientId': 'patient-123',
            'appointmentDate': '2024-01-02',
            'startTime': '07:00',  # Before 8 AM
            'endTime': '08:00'
        }
        errors = validate_appointment_request(invalid_request)
        assert len(errors) > 0
        assert any('business hours' in error.lower() for error in errors)
        
        invalid_request = {
            'patientId': 'patient-123',
            'appointmentDate': '2024-01-02',
            'startTime': '18:00',
            'endTime': '19:00'  # After 6 PM
        }
        errors = validate_appointment_request(invalid_request)
        assert len(errors) > 0
        assert any('business hours' in error.lower() for error in errors)

class TestCheckPatientExists:
    """Test cases for patient existence checking."""
    
    @patch('appointment_request_processor.dynamodb')
    def test_patient_exists(self, mock_dynamodb, sample_patient):
        """Test that existing patient returns True."""
        # Mock the DynamoDB table and response
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.get_item.return_value = {'Item': sample_patient}
        
        result = check_patient_exists('patient-123')
        assert result is True
        mock_table.get_item.assert_called_once_with(Key={'patientId': 'patient-123'})
    
    @patch('appointment_request_processor.dynamodb')
    def test_patient_not_exists(self, mock_dynamodb):
        """Test that non-existing patient returns False."""
        # Mock the DynamoDB table and response
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.get_item.return_value = {}
        
        result = check_patient_exists('patient-999')
        assert result is False
    
    @patch('appointment_request_processor.dynamodb')
    def test_dynamodb_error(self, mock_dynamodb):
        """Test that DynamoDB errors are handled gracefully."""
        from botocore.exceptions import ClientError
        
        # Mock the DynamoDB table and error
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.get_item.side_effect = ClientError(
            {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Table not found'}},
            'GetItem'
        )
        
        result = check_patient_exists('patient-123')
        assert result is False

class TestCheckAppointmentConflicts:
    """Test cases for appointment conflict checking."""
    
    @patch('appointment_request_processor.dynamodb')
    def test_no_conflicts(self, mock_dynamodb):
        """Test that no conflicts are found when none exist."""
        # Mock the DynamoDB table and response
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.query.return_value = {'Items': []}
        
        conflicts = check_appointment_conflicts('patient-123', '2024-01-02', '10:00', '11:00')
        assert len(conflicts) == 0
    
    @patch('appointment_request_processor.dynamodb')
    def test_time_conflict(self, mock_dynamodb):
        """Test that time conflicts are detected."""
        # Mock existing appointment that overlaps
        existing_appointment = {
            'startTime': '10:30',
            'endTime': '11:30'
        }
        
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.query.return_value = {'Items': [existing_appointment]}
        
        conflicts = check_appointment_conflicts('patient-123', '2024-01-02', '10:00', '11:00')
        assert len(conflicts) > 0
        assert any('conflict' in conflict.lower() for conflict in conflicts)
    
    @patch('appointment_request_processor.dynamodb')
    def test_dynamodb_error(self, mock_dynamodb):
        """Test that DynamoDB errors are handled gracefully."""
        from botocore.exceptions import ClientError
        
        # Mock the DynamoDB table and error
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.query.side_effect = ClientError(
            {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Table not found'}},
            'Query'
        )
        
        conflicts = check_appointment_conflicts('patient-123', '2024-01-02', '10:00', '11:00')
        assert len(conflicts) > 0
        assert any('error' in conflict.lower() for conflict in conflicts)

class TestCreateAppointment:
    """Test cases for appointment creation."""
    
    @patch('appointment_request_processor.dynamodb')
    @patch('appointment_request_processor.uuid.uuid4')
    def test_create_appointment_success(self, mock_uuid, mock_dynamodb, sample_appointment_request):
        """Test successful appointment creation."""
        # Mock dependencies
        mock_uuid.return_value = 'appointment-123'
        
        # Mock the DynamoDB table
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        
        result = create_appointment(sample_appointment_request)
        
        # Verify the result
        assert result['appointmentId'] == 'appointment-123'
        assert result['patientId'] == sample_appointment_request['patientId']
        assert result['status'] == 'REQUESTED'
        assert 'createdAt' in result
        assert 'updatedAt' in result
        
        # Verify DynamoDB was called
        mock_table.put_item.assert_called_once()
    
    @patch('appointment_request_processor.dynamodb')
    def test_create_appointment_dynamodb_error(self, mock_dynamodb, sample_appointment_request):
        """Test that DynamoDB errors are properly handled."""
        from botocore.exceptions import ClientError
        
        # Mock the DynamoDB table and error
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.put_item.side_effect = ClientError(
            {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Table not found'}},
            'PutItem'
        )
        
        with pytest.raises(ClientError):
            create_appointment(sample_appointment_request)

class TestSendConfirmationMessage:
    """Test cases for sending confirmation messages."""
    
    @patch('appointment_request_processor.sqs')
    def test_send_confirmation_success(self, mock_sqs, sample_appointment):
        """Test successful confirmation message sending."""
        # Mock the SQS client
        mock_sqs.send_message.return_value = {'MessageId': 'msg-123'}
        
        send_confirmation_message(sample_appointment)
        
        # Verify SQS was called
        mock_sqs.send_message.assert_called_once()
        call_args = mock_sqs.send_message.call_args
        assert 'QueueUrl' in call_args[1]
        assert 'MessageBody' in call_args[1]
        
        # Verify message content
        message_body = json.loads(call_args[1]['MessageBody'])
        assert message_body['appointmentId'] == sample_appointment['appointmentId']
        assert message_body['action'] == 'CONFIRM'
    
    @patch('appointment_request_processor.sqs')
    def test_send_confirmation_sqs_error(self, mock_sqs, sample_appointment):
        """Test that SQS errors don't fail the operation."""
        from botocore.exceptions import ClientError
        
        # Mock SQS error
        mock_sqs.send_message.side_effect = ClientError(
            {'Error': {'Code': 'InvalidParameterValue', 'Message': 'Invalid queue URL'}},
            'SendMessage'
        )
        
        # Should not raise an exception
        send_confirmation_message(sample_appointment)

class TestLambdaHandler:
    """Test cases for the main Lambda handler."""
    
    @patch('appointment_request_processor.check_patient_exists')
    @patch('appointment_request_processor.check_appointment_conflicts')
    @patch('appointment_request_processor.create_appointment')
    @patch('appointment_request_processor.send_confirmation_message')
    def test_successful_processing(self, mock_send_conf, mock_create, mock_check_conflicts, 
                                 mock_check_patient, sample_sqs_event, sample_appointment):
        """Test successful processing of appointment requests."""
        # Mock all the dependencies
        mock_check_patient.return_value = True
        mock_check_conflicts.return_value = []
        mock_create.return_value = sample_appointment
        
        result = lambda_handler(sample_sqs_event, {})
        
        # Verify the result
        assert result['successful'] == 1
        assert result['failed'] == 0
        assert len(result['errors']) == 0
        
        # Verify all functions were called
        mock_check_patient.assert_called_once()
        mock_check_conflicts.assert_called_once()
        mock_create.assert_called_once()
        mock_send_conf.assert_called_once()
    
    def test_validation_failure(self, sample_sqs_event):
        """Test processing fails when validation fails."""
        # Modify the event to have invalid data
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        sample_sqs_event['Records'][0]['body'] = json.dumps({
            'patientId': '',  # Invalid: empty patient ID
            'appointmentDate': tomorrow,
            'startTime': '10:00',
            'endTime': '11:00'
        })
        
        result = lambda_handler(sample_sqs_event, {})
        
        # Verify the result
        assert result['successful'] == 0
        assert result['failed'] == 1
        assert len(result['errors']) > 0
    
    def test_patient_not_found(self, sample_sqs_event):
        """Test processing fails when patient doesn't exist."""
        with patch('appointment_request_processor.check_patient_exists') as mock_check_patient:
            mock_check_patient.return_value = False
            
            result = lambda_handler(sample_sqs_event, {})
            
            # Verify the result
            assert result['successful'] == 0
            assert result['failed'] == 1
            assert len(result['errors']) > 0
    
    def test_appointment_conflicts(self, sample_sqs_event):
        """Test processing fails when appointment conflicts exist."""
        with patch('appointment_request_processor.check_patient_exists') as mock_check_patient, \
             patch('appointment_request_processor.check_appointment_conflicts') as mock_check_conflicts:
            
            mock_check_patient.return_value = True
            mock_check_conflicts.return_value = ['Time conflict with existing appointment']
            
            result = lambda_handler(sample_sqs_event, {})
            
            # Verify the result
            assert result['successful'] == 0
            assert result['failed'] == 1
            assert len(result['errors']) > 0
    
    def test_multiple_records(self):
        """Test processing multiple SQS records."""
        # Create event with multiple records
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        day_after_tomorrow = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d')
        multi_record_event = {
            'Records': [
                {
                    'messageId': 'msg-1',
                    'body': json.dumps({
                        'patientId': 'patient-1',
                        'appointmentDate': tomorrow,
                        'startTime': '10:00',
                        'endTime': '11:00'
                    })
                },
                {
                    'messageId': 'msg-2',
                    'body': json.dumps({
                        'patientId': 'patient-2',
                        'appointmentDate': day_after_tomorrow,
                        'startTime': '14:00',
                        'endTime': '15:00'
                    })
                }
            ]
        }
        
        with patch('appointment_request_processor.check_patient_exists') as mock_check_patient, \
             patch('appointment_request_processor.check_appointment_conflicts') as mock_check_conflicts, \
             patch('appointment_request_processor.create_appointment') as mock_create, \
             patch('appointment_request_processor.send_confirmation_message') as mock_send_conf:
            
            mock_check_patient.return_value = True
            mock_check_conflicts.return_value = []
            mock_create.return_value = {'appointmentId': 'test-id'}
            
            result = lambda_handler(multi_record_event, {})
            
            # Verify the result
            assert result['successful'] == 2
            assert result['failed'] == 0
            assert len(result['errors']) == 0
    
    def test_exception_handling(self, sample_sqs_event):
        """Test that exceptions are properly handled and logged."""
        with patch('appointment_request_processor.check_patient_exists') as mock_check_patient:
            # Make the function raise an exception
            mock_check_patient.side_effect = Exception("Unexpected error")
            
            result = lambda_handler(sample_sqs_event, {})
            
            # Verify the result
            assert result['successful'] == 0
            assert result['failed'] == 1
            assert len(result['errors']) > 0
            assert any('unexpected error' in error.lower() for error in result['errors'])
