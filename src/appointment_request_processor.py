import json
import logging
import os
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError
from jsonschema import validate, ValidationError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

# Environment variables
APPOINTMENTS_TABLE = os.environ['APPOINTMENTS_TABLE']
PATIENTS_TABLE = os.environ['PATIENTS_TABLE']
CONFIRMATION_QUEUE_URL = os.environ['CONFIRMATION_QUEUE_URL']

# JSON Schema for appointment request validation
APPOINTMENT_REQUEST_SCHEMA = {
    "type": "object",
    "properties": {
        "patientId": {"type": "string", "minLength": 1},
        "appointmentDate": {"type": "string", "format": "date"},
        "startTime": {"type": "string", "pattern": "^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$"},
        "endTime": {"type": "string", "pattern": "^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$"},
        "notes": {"type": "string", "maxLength": 500}
    },
    "required": ["patientId", "appointmentDate", "startTime", "endTime"],
    "additionalProperties": False
}

def validate_appointment_request(request: Dict[str, Any]) -> List[str]:
    """
    Validate appointment request data and return list of validation errors.
    
    Args:
        request: The appointment request data
        
    Returns:
        List of validation error messages
    """
    errors = []
    
    try:
        validate(instance=request, schema=APPOINTMENT_REQUEST_SCHEMA)
    except ValidationError as e:
        errors.append(f"Schema validation error: {e.message}")
    
    # Additional business logic validation
    if not errors:
        try:
            appointment_date = datetime.strptime(request['appointmentDate'], '%Y-%m-%d').date()
            start_time = datetime.strptime(request['startTime'], '%H:%M').time()
            end_time = datetime.strptime(request['endTime'], '%H:%M').time()
            
            # Check if appointment date is in the future
            if appointment_date < datetime.now().date():
                errors.append("Appointment date cannot be in the past")
            
            # Check if start time is before end time
            if start_time >= end_time:
                errors.append("Start time must be before end time")
            
            # Check if appointment is during business hours (8 AM - 6 PM)
            if start_time < datetime.strptime('08:00', '%H:%M').time() or \
               end_time > datetime.strptime('18:00', '%H:%M').time():
                errors.append("Appointments must be during business hours (8 AM - 6 PM)")
                
        except ValueError as e:
            errors.append(f"Date/time parsing error: {str(e)}")
    
    return errors

def check_patient_exists(patient_id: str) -> bool:
    """
    Check if a patient exists in the patients table.
    
    Args:
        patient_id: The patient ID to check
        
    Returns:
        True if patient exists, False otherwise
    """
    try:
        table = dynamodb.Table(PATIENTS_TABLE)
        response = table.get_item(Key={'patientId': patient_id})
        return 'Item' in response
    except ClientError as e:
        logger.error(f"Error checking patient existence: {e}")
        return False

def check_appointment_conflicts(patient_id: str, appointment_date: str, 
                              start_time: str, end_time: str) -> List[str]:
    """
    Check for appointment conflicts for the same patient on the same date.
    
    Args:
        patient_id: The patient ID
        appointment_date: The appointment date
        start_time: The start time
        end_time: The end time
        
    Returns:
        List of conflict error messages
    """
    conflicts = []
    
    try:
        table = dynamodb.Table(APPOINTMENTS_TABLE)
        
        # Query for existing appointments on the same date for this patient
        response = table.query(
            IndexName='PatientAppointmentsIndex',
            KeyConditionExpression='patientId = :pid AND appointmentDate = :date',
            FilterExpression='#status <> :cancelled',
            ExpressionAttributeNames={
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':pid': patient_id,
                ':date': appointment_date,
                ':cancelled': 'CANCELLED'
            }
        )
        
        # Check for time conflicts
        new_start = datetime.strptime(start_time, '%H:%M').time()
        new_end = datetime.strptime(end_time, '%H:%M').time()
        
        for item in response.get('Items', []):
            existing_start = datetime.strptime(item['startTime'], '%H:%M').time()
            existing_end = datetime.strptime(item['endTime'], '%H:%M').time()
            
            # Check if there's any overlap
            if (new_start < existing_end and new_end > existing_start):
                conflicts.append(f"Time conflict with existing appointment: {item['startTime']} - {item['endTime']}")
                
    except ClientError as e:
        logger.error(f"Error checking appointment conflicts: {e}")
        conflicts.append("Error checking for conflicts")
    
    return conflicts

def create_appointment(request: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new appointment in DynamoDB.
    
    Args:
        request: The validated appointment request
        
    Returns:
        The created appointment data
    """
    appointment_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    
    # Calculate TTL (30 days from appointment date)
    appointment_date = datetime.strptime(request['appointmentDate'], '%Y-%m-%d')
    ttl = int((appointment_date + timedelta(days=30)).timestamp())
    
    appointment = {
        'appointmentId': appointment_id,
        'patientId': request['patientId'],
        'appointmentDate': request['appointmentDate'],
        'startTime': request['startTime'],
        'endTime': request['endTime'],
        'status': 'REQUESTED',
        'notes': request.get('notes', ''),
        'createdAt': now,
        'updatedAt': now,
        'ttl': ttl
    }
    
    try:
        table = dynamodb.Table(APPOINTMENTS_TABLE)
        table.put_item(Item=appointment)
        logger.info(f"Created appointment {appointment_id} for patient {request['patientId']}")
        return appointment
    except ClientError as e:
        logger.error(f"Error creating appointment: {e}")
        raise

def send_confirmation_message(appointment: Dict[str, Any]) -> None:
    """
    Send appointment confirmation message to SQS queue.
    
    Args:
        appointment: The created appointment data
    """
    try:
        message_body = {
            'appointmentId': appointment['appointmentId'],
            'patientId': appointment['patientId'],
            'action': 'CONFIRM',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        sqs.send_message(
            QueueUrl=CONFIRMATION_QUEUE_URL,
            MessageBody=json.dumps(message_body),
            MessageAttributes={
                'MessageType': {
                    'StringValue': 'APPOINTMENT_CONFIRMATION',
                    'DataType': 'String'
                }
            }
        )
        
        logger.info(f"Sent confirmation message for appointment {appointment['appointmentId']}")
        
    except ClientError as e:
        logger.error(f"Error sending confirmation message: {e}")
        # Don't fail the entire operation if SQS fails
        pass

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for processing appointment requests from SQS.
    
    Args:
        event: SQS event containing appointment requests
        context: Lambda context
        
    Returns:
        Processing results
    """
    logger.info(f"Processing {len(event['Records'])} appointment request(s)")
    
    results = {
        'successful': 0,
        'failed': 0,
        'errors': []
    }
    
    for record in event['Records']:
        try:
            # Parse SQS message
            message_body = json.loads(record['body'])
            request_id = record.get('messageId', 'unknown')
            
            logger.info(f"Processing request {request_id}: {message_body}")
            
            # Validate request
            validation_errors = validate_appointment_request(message_body)
            if validation_errors:
                error_msg = f"Validation failed for request {request_id}: {'; '.join(validation_errors)}"
                logger.warning(error_msg)
                results['errors'].append(error_msg)
                results['failed'] += 1
                continue
            
            # Check if patient exists
            if not check_patient_exists(message_body['patientId']):
                error_msg = f"Patient {message_body['patientId']} not found for request {request_id}"
                logger.warning(error_msg)
                results['errors'].append(error_msg)
                results['failed'] += 1
                continue
            
            # Check for appointment conflicts
            conflicts = check_appointment_conflicts(
                message_body['patientId'],
                message_body['appointmentDate'],
                message_body['startTime'],
                message_body['endTime']
            )
            
            if conflicts:
                error_msg = f"Conflicts found for request {request_id}: {'; '.join(conflicts)}"
                logger.warning(error_msg)
                results['errors'].append(error_msg)
                results['failed'] += 1
                continue
            
            # Create appointment
            appointment = create_appointment(message_body)
            
            # Send confirmation message
            send_confirmation_message(appointment)
            
            results['successful'] += 1
            logger.info(f"Successfully processed request {request_id}")
            
        except Exception as e:
            error_msg = f"Unexpected error processing request {record.get('messageId', 'unknown')}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            results['errors'].append(error_msg)
            results['failed'] += 1
    
    logger.info(f"Processing complete. Successful: {results['successful']}, Failed: {results['failed']}")
    return results
