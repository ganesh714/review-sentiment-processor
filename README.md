# Product Review Event Processor

A robust, containerized backend service that processes product reviews using an Event-Driven Architecture.

## Features
- **Event-Driven**: Consumes reviews from RabbitMQ.
- **Sentiment Analysis**: Analyzes review comments (Positive/Negative/Neutral).
- **Data Persistence**: Stores enriched reviews in PostgreSQL.
- **Reliability**: Implements Idempotency, Retries, and Dead Letter Queues (DLQ).
- **Observability**: Structured JSON logging.
- **Containerized**: Fully Dockerized setup.

## Architecture
1.  **Producer**: Sends `ProductReviewSubmitted` event.
2.  **Consumer**:
    - Checks for duplicates (Idempotency).
    - Analyzes sentiment.
    - Saves to DB.
    - Publishes `ReviewProcessed` event.
3.  **Failure Handling**: Malformed or failed messages are sent to a DLQ.

## Prerequisites
- Docker & Docker Compose
- Python 3.9+ (for local development/testing)

## Setup & Running

1.  **Start Services**:
    ```bash
    docker-compose up --build -d
    ```
    This starts RabbitMQ, PostgreSQL, and the Processor App.

2.  **Verify Status**:
    ```bash
    docker-compose ps
    ```

3.  **Check Logs**:
    ```bash
    docker-compose logs -f app
    ```

## Testing

### Manual Testing
You can use the included `test_publisher.py` script to simulate events.

1.  **Install local dependencies**:
    ```bash
    pip install pika
    ```

2.  **Send a Valid Review**:
    ```bash
    python test_publisher.py success
    ```
    *Check logs to see "Saved review..." and "Published ReviewProcessed event..."*

3.  **Send a Malformed Message (Test DLQ)**:
    ```bash
    python test_publisher.py malformed
    ```
    *Check logs to see "Failed to decode JSON. Rejecting (to DLQ)."*

### Unit Tests
You can run the unit tests without any external dependencies (RabbitMQ/DB not required):
```bash
python tests/test_consumer.py
```

To run standard discovery (requires dependencies installed):
```bash
python -m unittest discover tests
```
## Configuration
- **RabbitMQ**: `localhost:5672` (Mgmt: 15672)
- **Postgres**: `localhost:5432`
- **Environment Variables**: Defined in `docker-compose.yml`.
