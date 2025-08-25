import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

# Environment variables
APPOINTMENTS_TABLE = os.environ['APPOINTMENTS_TABLE']
PATIENTS_TABLE = os.environ['PATIENTS_TABLE']
REMINDER_QUEUE_URL = os.environ['REMINDER_QUEUE_URL']

def get_appointment(appointment_id: str) -> Dict[str, Any]:
    """
    Retrieve appointment details from DynamoDB.
    
    Args:
        appointment_id: The appointment ID to retrieve
        
    Returns:
        The appointment data or None if not found
    """
    try:
        table = dynamodb.Table(APPOINTMENTS_TABLE)
        response = table.get_item(Key={'appointmentId': appointment_id})
        return response.get('Item')
    except ClientError as e:
        logger.error(f"Error retrieving appointment {appointment_id}: {e}")
        return None

def get_patient(patient_id: str) -> Dict[str, Any]:
    """
    Retrieve patient details from DynamoDB.
    
    Args:
        patient_id: The patient ID to retrieve
        
    Returns:
        The patient data or None if not found
    """
    try:
        table = dynamodb.Table(PATIENTS_TABLE)
        response = table.get_item(Key={'patientId': patient_id})
        return response.get('Item')
    except ClientError as e:
        logger.error(f"Error retrieving patient {patient_id}: {e}")
        return None

def update_appointment_status(appointment_id: str, status: str, notes: str = None) -> bool:
    """
    Update the status of an appointment in DynamoDB.
    
    Args:
        appointment_id: The appointment ID to update
        status: The new status
        notes: Optional notes about the status change
        
    Returns:
        True if successful, False otherwise
    """
    try:
        table = dynamodb.Table(APPOINTMENTS_TABLE)
        
        update_expression = "SET #status = :status, updatedAt = :updatedAt"
        expression_attribute_names = {
            '#status': 'status'
        }
        expression_attribute_values = {
            ':status': status,
            ':updatedAt': datetime.utcnow().isoformat()
        }
        
        if notes:
            update_expression += ", notes = :notes"
            expression_attribute_values[':notes'] = notes
        
        table.update_item(
            Key={'appointmentId': appointment_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ConditionExpression='attribute_exists(appointmentId)'
        )
        
        logger.info(f"Updated appointment {appointment_id} status to {status}")
        return True
        
    except ClientError as e:
        logger.error(f"Error updating appointment {appointment_id} status: {e}")
        return False

def send_confirmation_notification(appointment: Dict[str, Any], patient: Dict[str, Any]) -> None:
    """
    Send confirmation notification to the patient.
    In a real implementation, this would integrate with email/SMS services.
    
    Args:
        appointment: The appointment data
        patient: The patient data
    """
    try:
        # This is a placeholder for actual notification logic
        # In production, you would integrate with SES, SNS, or other notification services
        
        notification_data = {
            'type': 'APPOINTMENT_CONFIRMATION',
            'patientId': patient['patientId'],
            'patientName': f"{patient.get('firstName', '')} {patient.get('lastName', '')}",
            'patientEmail': patient.get('email'),
            'patientPhone': patient.get('phone'),
            'appointmentId': appointment['appointmentId'],
            'appointmentDate': appointment['appointmentDate'],
            'startTime': appointment['startTime'],
            'endTime': appointment['endTime'],
            'timestamp': datetime.utcnow().isoformat()
        }
        
        logger.info(f"Confirmation notification prepared for patient {patient['patientId']}: {notification_data}")
        
        # TODO: Implement actual notification sending
        # Example: Send email via SES, SMS via SNS, etc.
        
    except Exception as e:
        logger.error(f"Error sending confirmation notification: {e}")
        # Don't fail the entire operation if notification fails

def schedule_reminder(appointment: Dict[str, Any], patient: Dict[str, Any]) -> None:
    """
    Schedule a reminder for the appointment by sending a message to the reminder queue.
    
    Args:
        appointment: The appointment data
        patient: The patient data
    """
    try:
        # Calculate reminder time (24 hours before appointment)
        appointment_datetime = datetime.strptime(
            f"{appointment['appointmentDate']} {appointment['startTime']}", 
            '%Y-%m-%d %H:%M'
        )
        reminder_time = appointment_datetime - timedelta(hours=24)
        
        # Only schedule reminder if it's in the future
        if reminder_time > datetime.now():
            reminder_message = {
                'appointmentId': appointment['appointmentId'],
                'patientId': patient['patientId'],
                'reminderTime': reminder_time.isoformat(),
                'appointmentDate': appointment['appointmentDate'],
                'startTime': appointment['startTime'],
                'endTime': appointment['endTime'],
                'patientName': f"{patient.get('firstName', '')} {patient.get('lastName', '')}",
                'patientEmail': patient.get('email'),
                'patientPhone': patient.get('phone'),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            sqs.send_message(
                QueueUrl=REMINDER_QUEUE_URL,
                MessageBody=json.dumps(reminder_message),
                MessageAttributes={
                    'MessageType': {
                        'StringValue': 'APPOINTMENT_REMINDER',
                        'DataType': 'String'
                    },
                    'ReminderTime': {
                        'StringValue': reminder_time.isoformat(),
                        'DataType': 'String'
                    }
                }
            )
            
            logger.info(f"Scheduled reminder for appointment {appointment['appointmentId']} at {reminder_time}")
        else:
            logger.info(f"Appointment {appointment['appointmentId']} is too soon to schedule reminder")
            
    except Exception as e:
        logger.error(f"Error scheduling reminder for appointment {appointment['appointmentId']}: {e}")
        # Don't fail the entire operation if reminder scheduling fails

def process_confirmation_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single confirmation message.
    
    Args:
        message: The confirmation message data
        
    Returns:
        Processing result
    """
    appointment_id = message.get('appointmentId')
    patient_id = message.get('patientId')
    action = message.get('action')
    
    if not appointment_id or not patient_id:
        return {
            'success': False,
            'error': 'Missing required fields: appointmentId or patientId'
        }
    
    # Get appointment details
    appointment = get_appointment(appointment_id)
    if not appointment:
        return {
            'success': False,
            'error': f'Appointment {appointment_id} not found'
        }
    
    # Get patient details
    patient = get_patient(patient_id)
    if not patient:
        return {
            'success': False,
            'error': f'Patient {patient_id} not found'
        }
    
    # Verify appointment belongs to patient
    if appointment['patientId'] != patient_id:
        return {
            'success': False,
            'error': f'Appointment {appointment_id} does not belong to patient {patient_id}'
        }
    
    # Process based on action
    if action == 'CONFIRM':
        # Update appointment status to confirmed
        if not update_appointment_status(appointment_id, 'CONFIRMED', 'Appointment confirmed'):
            return {
                'success': False,
                'error': f'Failed to update appointment {appointment_id} status'
            }
        
        # Send confirmation notification
        send_confirmation_notification(appointment, patient)
        
        # Schedule reminder
        schedule_reminder(appointment, patient)
        
        return {
            'success': True,
            'appointmentId': appointment_id,
            'status': 'CONFIRMED'
        }
    
    elif action == 'CANCEL':
        # Update appointment status to cancelled
        if not update_appointment_status(appointment_id, 'CANCELLED', 'Appointment cancelled'):
            return {
                'success': False,
                'error': f'Failed to update appointment {appointment_id} status'
            }
        
        return {
            'success': True,
            'appointmentId': appointment_id,
            'status': 'CANCELLED'
        }
    
    else:
        return {
            'success': False,
            'error': f'Unknown action: {action}'
        }

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for processing appointment confirmation messages from SQS.
    
    Args:
        event: SQS event containing confirmation messages
        context: Lambda context
        
    Returns:
        Processing results
    """
    logger.info(f"Processing {len(event['Records'])} confirmation message(s)")
    
    results = {
        'successful': 0,
        'failed': 0,
        'errors': []
    }
    
    for record in event['Records']:
        try:
            # Parse SQS message
            message_body = json.loads(record['body'])
            message_id = record.get('messageId', 'unknown')
            
            logger.info(f"Processing confirmation message {message_id}: {message_body}")
            
            # Process the confirmation message
            result = process_confirmation_message(message_body)
            
            if result['success']:
                results['successful'] += 1
                logger.info(f"Successfully processed confirmation message {message_id}")
            else:
                results['failed'] += 1
                error_msg = f"Failed to process confirmation message {message_id}: {result['error']}"
                logger.warning(error_msg)
                results['errors'].append(error_msg)
                
        except Exception as e:
            error_msg = f"Unexpected error processing confirmation message {record.get('messageId', 'unknown')}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            results['errors'].append(error_msg)
            results['failed'] += 1
    
    logger.info(f"Processing complete. Successful: {results['successful']}, Failed: {results['failed']}")
    return results
