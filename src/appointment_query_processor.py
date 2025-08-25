import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

# Environment variables
APPOINTMENTS_TABLE = os.environ['APPOINTMENTS_TABLE']
PATIENTS_TABLE = os.environ['PATIENTS_TABLE']

def get_appointment_by_id(appointment_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve a specific appointment by ID.
    
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

def get_patient_by_id(patient_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve a specific patient by ID.
    
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

def get_patient_by_email(email: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve a patient by email address.
    
    Args:
        email: The email address to search for
        
    Returns:
        The patient data or None if not found
    """
    try:
        table = dynamodb.Table(PATIENTS_TABLE)
        response = table.query(
            IndexName='EmailIndex',
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={':email': email}
        )
        
        items = response.get('Items', [])
        return items[0] if items else None
        
    except ClientError as e:
        logger.error(f"Error retrieving patient by email {email}: {e}")
        return None

def get_appointments_by_patient(patient_id: str, status: Optional[str] = None, 
                               start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Retrieve appointments for a specific patient with optional filtering.
    
    Args:
        patient_id: The patient ID
        status: Optional status filter
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        
    Returns:
        List of appointment data
    """
    try:
        table = dynamodb.Table(APPOINTMENTS_TABLE)
        
        # Build query parameters
        key_condition_expression = 'patientId = :pid'
        expression_attribute_values = {':pid': patient_id}
        filter_expression = None
        
        # Add date range filter if provided
        if start_date and end_date:
            key_condition_expression += ' AND appointmentDate BETWEEN :start_date AND :end_date'
            expression_attribute_values[':start_date'] = start_date
            expression_attribute_values[':end_date'] = end_date
        elif start_date:
            key_condition_expression += ' AND appointmentDate >= :start_date'
            expression_attribute_values[':start_date'] = start_date
        elif end_date:
            key_condition_expression += ' AND appointmentDate <= :end_date'
            expression_attribute_values[':end_date'] = end_date
        
        # Add status filter if provided
        if status:
            filter_expression = '#status = :status'
            expression_attribute_values[':status'] = status
        
        # Execute query
        query_params = {
            'IndexName': 'PatientAppointmentsIndex',
            'KeyConditionExpression': key_condition_expression,
            'ExpressionAttributeValues': expression_attribute_values
        }
        
        if filter_expression:
            query_params['FilterExpression'] = filter_expression
            query_params['ExpressionAttributeNames'] = {'#status': 'status'}
        
        response = table.query(**query_params)
        
        return response.get('Items', [])
        
    except ClientError as e:
        logger.error(f"Error retrieving appointments for patient {patient_id}: {e}")
        return []

def get_appointments_by_status(status: str, start_date: Optional[str] = None, 
                             end_date: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Retrieve appointments by status with optional date filtering.
    
    Args:
        status: The appointment status to filter by
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        
    Returns:
        List of appointment data
    """
    try:
        table = dynamodb.Table(APPOINTMENTS_TABLE)
        
        # Build query parameters
        key_condition_expression = '#status = :status'
        expression_attribute_values = {':status': status}
        
        # Add date range filter if provided
        if start_date and end_date:
            key_condition_expression += ' AND appointmentDate BETWEEN :start_date AND :end_date'
            expression_attribute_values[':start_date'] = start_date
            expression_attribute_values[':end_date'] = end_date
        elif start_date:
            key_condition_expression += ' AND appointmentDate >= :start_date'
            expression_attribute_values[':start_date'] = start_date
        elif end_date:
            key_condition_expression += ' AND appointmentDate <= :end_date'
            expression_attribute_values[':end_date'] = end_date
        
        response = table.query(
            IndexName='StatusDateIndex',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames={'#status': 'status'}
        )
        
        return response.get('Items', [])
        
    except ClientError as e:
        logger.error(f"Error retrieving appointments by status {status}: {e}")
        return []

def get_appointments_by_date_range(start_date: str, end_date: str, 
                                 status: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Retrieve appointments within a date range with optional status filtering.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        status: Optional status filter
        
    Returns:
        List of appointment data
    """
    try:
        table = dynamodb.Table(APPOINTMENTS_TABLE)
        
        # Use scan with filter for date range queries
        filter_expression = 'appointmentDate BETWEEN :start_date AND :end_date'
        expression_attribute_values = {
            ':start_date': start_date,
            ':end_date': end_date
        }
        
        if status:
            filter_expression += ' AND #status = :status'
            expression_attribute_values[':status'] = status
        
        scan_params = {
            'FilterExpression': filter_expression,
            'ExpressionAttributeValues': expression_attribute_values
        }
        
        if status:
            scan_params['ExpressionAttributeNames'] = {'#status': 'status'}
        
        response = table.scan(**scan_params)
        
        return response.get('Items', [])
        
    except ClientError as e:
        logger.error(f"Error retrieving appointments by date range {start_date} to {end_date}: {e}")
        return []

def search_appointments(query_params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Search appointments based on various criteria.
    
    Args:
        query_params: Dictionary containing search parameters
        
    Returns:
        Search results with metadata
    """
    try:
        # Extract search parameters
        appointment_id = query_params.get('appointmentId')
        patient_id = query_params.get('patientId')
        patient_email = query_params.get('patientEmail')
        status = query_params.get('status')
        start_date = query_params.get('startDate')
        end_date = query_params.get('endDate')
        limit = min(int(query_params.get('limit', 100)), 1000)  # Cap at 1000
        
        results = []
        
        # Search by appointment ID (most specific)
        if appointment_id:
            appointment = get_appointment_by_id(appointment_id)
            if appointment:
                results.append(appointment)
                return {
                    'count': 1,
                    'results': results,
                    'searchType': 'by_appointment_id'
                }
        
        # Search by patient ID
        if patient_id:
            results = get_appointments_by_patient(patient_id, status, start_date, end_date)
            return {
                'count': len(results),
                'results': results[:limit],
                'searchType': 'by_patient_id'
            }
        
        # Search by patient email
        if patient_email:
            patient = get_patient_by_email(patient_email)
            if patient:
                results = get_appointments_by_patient(patient['patientId'], status, start_date, end_date)
                return {
                    'count': len(results),
                    'results': results[:limit],
                    'searchType': 'by_patient_email'
                }
        
        # Search by status and date range
        if status and (start_date or end_date):
            results = get_appointments_by_status(status, start_date, end_date)
            return {
                'count': len(results),
                'results': results[:limit],
                'searchType': 'by_status_and_date'
            }
        
        # Search by date range only
        if start_date and end_date:
            results = get_appointments_by_date_range(start_date, end_date, status)
            return {
                'count': len(results),
                'results': results[:limit],
                'searchType': 'by_date_range'
            }
        
        # Search by status only
        if status:
            results = get_appointments_by_status(status)
            return {
                'count': len(results),
                'results': results[:limit],
                'searchType': 'by_status'
            }
        
        # No valid search criteria
        return {
            'count': 0,
            'results': [],
            'searchType': 'none',
            'error': 'No valid search criteria provided'
        }
        
    except Exception as e:
        logger.error(f"Error in search_appointments: {e}")
        return {
            'count': 0,
            'results': [],
            'searchType': 'error',
            'error': str(e)
        }

def get_appointment_statistics(start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Get appointment statistics for a date range.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        Statistics about appointments in the date range
    """
    try:
        appointments = get_appointments_by_date_range(start_date, end_date)
        
        # Calculate statistics
        total_appointments = len(appointments)
        status_counts = {}
        patient_count = set()
        
        for appointment in appointments:
            status = appointment.get('status', 'UNKNOWN')
            status_counts[status] = status_counts.get(status, 0) + 1
            patient_count.add(appointment.get('patientId'))
        
        return {
            'dateRange': {
                'startDate': start_date,
                'endDate': end_date
            },
            'totalAppointments': total_appointments,
            'uniquePatients': len(patient_count),
            'statusBreakdown': status_counts,
            'averageAppointmentsPerDay': total_appointments / max(1, (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1)
        }
        
    except Exception as e:
        logger.error(f"Error getting appointment statistics: {e}")
        return {
            'error': str(e)
        }

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for appointment queries and searches.
    
    Args:
        event: Event containing query parameters
        context: Lambda context
        
    Returns:
        Query results
    """
    try:
        # Parse the event
        http_method = event.get('httpMethod', 'GET')
        path = event.get('path', '')
        query_string_parameters = event.get('queryStringParameters', {}) or {}
        
        logger.info(f"Processing {http_method} request to {path}")
        
        # Route based on path
        if path == '/appointments/search':
            # Search appointments
            results = search_appointments(query_string_parameters)
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(results)
            }
            
        elif path == '/appointments/statistics':
            # Get appointment statistics
            start_date = query_string_parameters.get('startDate')
            end_date = query_string_parameters.get('endDate')
            
            if not start_date or not end_date:
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': 'startDate and endDate are required for statistics'
                    })
                }
            
            stats = get_appointment_statistics(start_date, end_date)
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(stats)
            }
            
        elif path.startswith('/appointments/'):
            # Get specific appointment
            appointment_id = path.split('/')[-1]
            appointment = get_appointment_by_id(appointment_id)
            
            if not appointment:
                return {
                    'statusCode': 404,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': f'Appointment {appointment_id} not found'
                    })
                }
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(appointment)
            }
            
        elif path.startswith('/patients/'):
            # Get specific patient
            patient_id = path.split('/')[-1]
            patient = get_patient_by_id(patient_id)
            
            if not patient:
                return {
                    'statusCode': 404,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': f'Patient {patient_id} not found'
                    })
                }
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(patient)
            }
            
        else:
            # Unknown path
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Endpoint not found'
                })
            }
            
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Internal server error'
            })
        }
