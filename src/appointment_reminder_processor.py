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

def should_send_reminder(appointment: Dict[str, Any]) -> bool:
    """
    Determine if a reminder should be sent for the appointment.
    
    Args:
        appointment: The appointment data
        
    Returns:
        True if reminder should be sent, False otherwise
    """
    # Check if appointment is still confirmed
    if appointment.get('status') != 'CONFIRMED':
        logger.info(f"Appointment {appointment['appointmentId']} is not confirmed, skipping reminder")
        return False
    
    # Check if appointment is in the future
    try:
        appointment_datetime = datetime.strptime(
            f"{appointment['appointmentDate']} {appointment['startTime']}", 
            '%Y-%m-%d %H:%M'
        )
        
        if appointment_datetime <= datetime.now():
            logger.info(f"Appointment {appointment['appointmentId']} is in the past, skipping reminder")
            return False
            
    except ValueError as e:
        logger.error(f"Error parsing appointment datetime for {appointment['appointmentId']}: {e}")
        return False
    
    return True

def send_reminder_notification(appointment: Dict[str, Any], patient: Dict[str, Any]) -> bool:
    """
    Send reminder notification to the patient.
    In a real implementation, this would integrate with email/SMS services.
    
    Args:
        appointment: The appointment data
        patient: The patient data
        
    Returns:
        True if notification was sent successfully, False otherwise
    """
    try:
        # This is a placeholder for actual notification logic
        # In production, you would integrate with SES, SNS, or other notification services
        
        notification_data = {
            'type': 'APPOINTMENT_REMINDER',
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
        
        logger.info(f"Reminder notification prepared for patient {patient['patientId']}: {notification_data}")
        
        # TODO: Implement actual notification sending
        # Example: Send email via SES, SMS via SNS, etc.
        
        # For now, we'll simulate a successful notification
        return True
        
    except Exception as e:
        logger.error(f"Error sending reminder notification: {e}")
        return False

def update_reminder_sent_flag(appointment_id: str) -> bool:
    """
    Update the appointment to mark that a reminder has been sent.
    
    Args:
        appointment_id: The appointment ID to update
        
    Returns:
        True if successful, False otherwise
    """
    try:
        table = dynamodb.Table(APPOINTMENTS_TABLE)
        
        # Add a reminderSent field to track that reminder was sent
        table.update_item(
            Key={'appointmentId': appointment_id},
            UpdateExpression="SET reminderSent = :reminderSent, updatedAt = :updatedAt",
            ExpressionAttributeValues={
                ':reminderSent': True,
                ':updatedAt': datetime.utcnow().isoformat()
            },
            ConditionExpression='attribute_exists(appointmentId)'
        )
        
        logger.info(f"Updated appointment {appointment_id} with reminder sent flag")
        return True
        
    except ClientError as e:
        logger.error(f"Error updating reminder sent flag for appointment {appointment_id}: {e}")
        return False

def process_reminder_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single reminder message.
    
    Args:
        message: The reminder message data
        
    Returns:
        Processing result
    """
    appointment_id = message.get('appointmentId')
    patient_id = message.get('patientId')
    
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
    
    # Check if reminder should be sent
    if not should_send_reminder(appointment):
        return {
            'success': True,
            'appointmentId': appointment_id,
            'status': 'REMINDER_SKIPPED',
            'reason': 'Appointment not eligible for reminder'
        }
    
    # Send reminder notification
    if not send_reminder_notification(appointment, patient):
        return {
            'success': False,
            'error': f'Failed to send reminder notification for appointment {appointment_id}'
        }
    
    # Update reminder sent flag
    if not update_reminder_sent_flag(appointment_id):
        logger.warning(f"Failed to update reminder sent flag for appointment {appointment_id}")
        # Don't fail the entire operation if this update fails
    
    return {
        'success': True,
        'appointmentId': appointment_id,
        'status': 'REMINDER_SENT'
    }

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for processing appointment reminder messages from SQS.
    
    Args:
        event: SQS event containing reminder messages
        context: Lambda context
        
    Returns:
        Processing results
    """
    logger.info(f"Processing {len(event['Records'])} reminder message(s)")
    
    results = {
        'successful': 0,
        'failed': 0,
        'skipped': 0,
        'errors': []
    }
    
    for record in event['Records']:
        try:
            # Parse SQS message
            message_body = json.loads(record['body'])
            message_id = record.get('messageId', 'unknown')
            
            logger.info(f"Processing reminder message {message_id}: {message_body}")
            
            # Process the reminder message
            result = process_reminder_message(message_body)
            
            if result['success']:
                if result.get('status') == 'REMINDER_SKIPPED':
                    results['skipped'] += 1
                    logger.info(f"Skipped reminder for appointment {result['appointmentId']}: {result.get('reason')}")
                else:
                    results['successful'] += 1
                    logger.info(f"Successfully processed reminder message {message_id}")
            else:
                results['failed'] += 1
                error_msg = f"Failed to process reminder message {message_id}: {result['error']}"
                logger.warning(error_msg)
                results['errors'].append(error_msg)
                
        except Exception as e:
            error_msg = f"Unexpected error processing reminder message {record.get('messageId', 'unknown')}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            results['errors'].append(error_msg)
            results['failed'] += 1
    
    logger.info(f"Processing complete. Successful: {results['successful']}, Failed: {results['failed']}, Skipped: {results['skipped']}")
    return results
