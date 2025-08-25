import pytest
import boto3
import os
import sys
from moto.core.decorator import mock_aws
from datetime import datetime, timedelta
import json

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Set environment variables for testing
os.environ['APPOINTMENTS_TABLE'] = 'test-appointments'
os.environ['PATIENTS_TABLE'] = 'test-patients'
os.environ['CONFIRMATION_QUEUE_URL'] = 'https://sqs.us-east-1.amazonaws.com/123456789012/test-confirmation-queue'
os.environ['REMINDER_QUEUE_URL'] = 'https://sqs.us-east-1.amazonaws.com/123456789012/test-reminder-queue'

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture(scope='function')
def dynamodb_mock(aws_credentials):
    """Mock DynamoDB for testing."""
    with mock_aws(['dynamodb']):
        yield boto3.resource('dynamodb', region_name='us-east-1')

@pytest.fixture(scope='function')
def sqs_mock(aws_credentials):
    """Mock SQS for testing."""
    with mock_aws(['sqs']):
        yield boto3.client('sqs', region_name='us-east-1')

@pytest.fixture(scope='function')
def setup_tables(dynamodb_mock):
    """Set up test DynamoDB tables."""
    # Create appointments table
    appointments_table = dynamodb_mock.create_table(
        TableName='test-appointments',
        KeySchema=[
            {'AttributeName': 'appointmentId', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'appointmentId', 'AttributeType': 'S'},
            {'AttributeName': 'patientId', 'AttributeType': 'S'},
            {'AttributeName': 'appointmentDate', 'AttributeType': 'S'},
            {'AttributeName': 'status', 'AttributeType': 'S'}
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'PatientAppointmentsIndex',
                'KeySchema': [
                    {'AttributeName': 'patientId', 'KeyType': 'HASH'},
                    {'AttributeName': 'appointmentDate', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'}
            },
            {
                'IndexName': 'StatusDateIndex',
                'KeySchema': [
                    {'AttributeName': 'status', 'KeyType': 'HASH'},
                    {'AttributeName': 'appointmentDate', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'}
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create patients table
    patients_table = dynamodb_mock.create_table(
        TableName='test-patients',
        KeySchema=[
            {'AttributeName': 'patientId', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'patientId', 'AttributeType': 'S'},
            {'AttributeName': 'email', 'AttributeType': 'S'}
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'EmailIndex',
                'KeySchema': [
                    {'AttributeName': 'email', 'KeyType': 'HASH'}
                ],
                'Projection': {'ProjectionType': 'ALL'}
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Wait for tables to be created
    appointments_table.meta.client.get_waiter('table_exists').wait(TableName='test-appointments')
    patients_table.meta.client.get_waiter('table_exists').wait(TableName='test-patients')
    
    return appointments_table, patients_table

@pytest.fixture(scope='function')
def setup_queues(sqs_mock):
    """Set up test SQS queues."""
    # Create confirmation queue
    confirmation_queue = sqs_mock.create_queue(
        QueueName='test-confirmation-queue'
    )
    
    # Create reminder queue
    reminder_queue = sqs_mock.create_queue(
        QueueName='test-reminder-queue'
    )
    
    return confirmation_queue, reminder_queue

@pytest.fixture
def sample_patient():
    """Sample patient data for testing."""
    return {
        'patientId': 'patient-123',
        'firstName': 'John',
        'lastName': 'Doe',
        'email': 'john.doe@example.com',
        'phone': '+1-555-123-4567',
        'dateOfBirth': '1990-01-01',
        'createdAt': '2024-01-01T00:00:00Z',
        'updatedAt': '2024-01-01T00:00:00Z'
    }

@pytest.fixture
def sample_appointment():
    """Sample appointment data for testing."""
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    return {
        'appointmentId': 'appointment-123',
        'patientId': 'patient-123',
        'appointmentDate': tomorrow,
        'startTime': '10:00',
        'endTime': '11:00',
        'status': 'REQUESTED',
        'notes': 'Regular checkup',
        'createdAt': '2024-01-01T00:00:00Z',
        'updatedAt': '2024-01-01T00:00:00Z',
        'ttl': int((datetime.now() + timedelta(days=31)).timestamp())
    }

@pytest.fixture
def sample_appointment_request():
    """Sample appointment request data for testing."""
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    return {
        'patientId': 'patient-123',
        'appointmentDate': tomorrow,
        'startTime': '10:00',
        'endTime': '11:00',
        'notes': 'Regular checkup'
    }

@pytest.fixture
def sample_confirmation_message():
    """Sample confirmation message data for testing."""
    return {
        'appointmentId': 'appointment-123',
        'patientId': 'patient-123',
        'action': 'CONFIRM',
        'timestamp': '2024-01-01T00:00:00Z'
    }

@pytest.fixture
def sample_reminder_message():
    """Sample reminder message data for testing."""
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    return {
        'appointmentId': 'appointment-123',
        'patientId': 'patient-123',
        'reminderTime': (datetime.now() + timedelta(hours=12)).isoformat(),
        'appointmentDate': tomorrow,
        'startTime': '10:00',
        'endTime': '11:00',
        'patientName': 'John Doe',
        'patientEmail': 'john.doe@example.com',
        'patientPhone': '+1-555-123-4567',
        'timestamp': '2024-01-01T00:00:00Z'
    }

@pytest.fixture
def sample_sqs_event():
    """Sample SQS event for testing."""
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    return {
        'Records': [
            {
                'messageId': 'msg-123',
                'receiptHandle': 'receipt-123',
                'body': json.dumps({
                    'patientId': 'patient-123',
                    'appointmentDate': tomorrow,
                    'startTime': '10:00',
                    'endTime': '11:00',
                    'notes': 'Regular checkup'
                }),
                'attributes': {
                    'ApproximateReceiveCount': '1',
                    'SentTimestamp': '1640995200000',
                    'SenderId': 'AIDACKCEVSQ6C2EXAMPLE',
                    'ApproximateFirstReceiveTimestamp': '1640995200000'
                },
                'messageAttributes': {},
                'md5OfBody': 'md5-hash',
                'eventSource': 'aws:sqs',
                'eventSourceARN': 'arn:aws:sqs:us-east-1:123456789012:test-queue',
                'awsRegion': 'us-east-1'
            }
        ]
    }

@pytest.fixture
def sample_http_event():
    """Sample HTTP event for testing the query processor."""
    return {
        'httpMethod': 'GET',
        'path': '/appointments/search',
        'queryStringParameters': {
            'patientId': 'patient-123',
            'status': 'CONFIRMED'
        },
        'headers': {
            'Content-Type': 'application/json'
        }
    }

@pytest.fixture
def invalid_appointment_request():
    """Invalid appointment request data for testing validation."""
    return {
        'patientId': '',  # Empty patient ID
        'appointmentDate': '2023-01-01',  # Past date
        'startTime': '25:00',  # Invalid time
        'endTime': '09:00'  # End time before start time
    }

@pytest.fixture
def conflicting_appointment_request():
    """Appointment request that would conflict with existing appointment."""
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    return {
        'patientId': 'patient-123',
        'appointmentDate': tomorrow,
        'startTime': '10:30',  # Overlaps with 10:00-11:00
        'endTime': '11:30',
        'notes': 'Conflicting appointment'
    }
