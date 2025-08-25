# Appointment Scheduling Microservice

## Overview

This microservice handles appointment scheduling operations through a distributed architecture using AWS Lambda functions, SQS queues, and DynamoDB. The service processes appointment requests asynchronously, manages confirmations, sends reminders, and provides query capabilities.

## Architecture

### Components

1. **DynamoDB Tables**
   - `AppointmentsTable`: Stores appointment information with indexes for patient lookups and status-based queries
   - `PatientsTable`: Stores patient information with email-based lookups

2. **SQS Queues**
   - `AppointmentRequestQueue`: Receives new appointment requests
   - `AppointmentConfirmationQueue`: Processes appointment confirmations
   - `AppointmentReminderQueue`: Handles appointment reminders
   - `AppointmentRequestDLQ`: Dead letter queue for failed requests

3. **Lambda Functions**
   - `AppointmentRequestProcessor`: Consumes from SQS, validates requests, and creates appointments
   - `AppointmentConfirmationProcessor`: Processes confirmations and schedules reminders
   - `AppointmentReminderProcessor`: Sends reminder notifications
   - `AppointmentQueryProcessor`: Handles appointment queries and searches

### Data Flow

```
Appointment Request → SQS Queue → Request Processor → DynamoDB
                                           ↓
                                    Confirmation Queue → Confirmation Processor → Reminder Queue → Reminder Processor
```

## Features

- **Asynchronous Processing**: All operations are processed asynchronously through SQS queues
- **Fault Tolerance**: Dead letter queues and retry mechanisms for failed operations
- **Scalability**: Auto-scaling Lambda functions based on queue depth
- **Data Consistency**: Optimistic locking and conditional updates for data integrity
- **Audit Trail**: Comprehensive logging and monitoring

## Data Models

### Appointment
```json
{
  "appointmentId": "string",
  "patientId": "string",
  "appointmentDate": "ISO8601 string",
  "startTime": "HH:MM",
  "endTime": "HH:MM",
  "status": "REQUESTED|CONFIRMED|CANCELLED|COMPLETED",
  "notes": "string",
  "createdAt": "ISO8601 string",
  "updatedAt": "ISO8601 string",
  "ttl": "number"
}
```

### Patient
```json
{
  "patientId": "string",
  "firstName": "string",
  "lastName": "string",
  "email": "string",
  "phone": "string",
  "dateOfBirth": "ISO8601 string",
  "createdAt": "ISO8601 string",
  "updatedAt": "ISO8601 string"
}
```

## Local Development

### Prerequisites
- Python 3.9+
- AWS SAM CLI
- Docker (for local Lambda testing)

### Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure AWS credentials:
   ```bash
   aws configure
   ```

3. Build the SAM application:
   ```bash
   sam build
   ```

4. Deploy to AWS:
   ```bash
   sam deploy --guided
   ```

## Testing

### Running Tests

The application includes comprehensive unit tests and integration tests. To run the tests:

1. **Unit Tests**:
   ```bash
   python -m pytest tests/unit/ -v
   ```

2. **Integration Tests**:
   ```bash
   python -m pytest tests/integration/ -v
   ```

3. **All Tests with Coverage**:
   ```bash
   python -m pytest tests/ --cov=src --cov-report=html
   ```

4. **Local Lambda Testing**:
   ```bash
   sam local invoke AppointmentRequestProcessor --event events/appointment-request.json
   ```

### Test Structure

```
tests/
├── unit/                    # Unit tests for individual functions
│   ├── test_appointment_request_processor.py
│   ├── test_appointment_confirmation_processor.py
│   ├── test_appointment_reminder_processor.py
│   └── test_appointment_query_processor.py
├── integration/             # Integration tests
│   ├── test_dynamodb_operations.py
│   └── test_sqs_operations.py
├── fixtures/                # Test data and fixtures
│   ├── sample_appointments.json
│   └── sample_patients.json
└── conftest.py             # Pytest configuration and fixtures
```

### Test Data

The tests use realistic sample data that covers various scenarios:
- Valid appointment requests
- Invalid data formats
- Edge cases (past dates, overlapping times)
- Patient data variations

## Monitoring and Observability

### CloudWatch Metrics
- Lambda function invocations and errors
- SQS queue depth and message processing
- DynamoDB read/write capacity and throttling

### Logging
- Structured JSON logging for all operations
- Correlation IDs for request tracing
- Error details with stack traces

### Alarms
- Lambda function error rates
- SQS queue depth thresholds
- DynamoDB throttling events

## Security

- IAM roles with least privilege access
- DynamoDB encryption at rest
- SQS encryption in transit
- Environment variable encryption

## Performance

- Lambda function timeout: 30 seconds
- Memory allocation: 256 MB (configurable)
- SQS batch processing: up to 10 messages
- DynamoDB on-demand billing for cost optimization

## Deployment

### SAM Template
The `template.yaml` file defines all AWS resources:
- DynamoDB tables with proper indexes
- SQS queues with dead letter queues
- Lambda functions with appropriate IAM roles
- CloudWatch alarms and monitoring

### Environment Variables
All configuration is externalized through environment variables:
- Table names
- Queue URLs
- Log levels
- Feature flags

## Troubleshooting

### Common Issues

1. **Lambda Timeout**: Increase timeout or optimize database queries
2. **SQS Message Processing**: Check visibility timeout and batch size
3. **DynamoDB Throttling**: Monitor capacity and adjust as needed
4. **Permission Errors**: Verify IAM roles and policies

### Debug Mode
Enable debug logging by setting the `LOG_LEVEL` environment variable to `DEBUG`.

## Contributing

1. Follow the existing code style and patterns
2. Add tests for new functionality
3. Update documentation for API changes
4. Ensure all tests pass before submitting

## License

This project is licensed under the Apache 2.0 License.
