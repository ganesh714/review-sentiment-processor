import sys
import os
import time
import json
import logging
import pika
from sqlalchemy.exc import IntegrityError

# Add the current directory to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database import init_db, get_db_session, ProcessedReview
from src.sentiment import analyze_sentiment
from src.publisher import EventPublisher

# Configure Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
    datefmt='%Y-%m-%dT%H:%M:%SZ'
)
logger = logging.getLogger(__name__)

# Configuration
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')
QUEUE_NAME = 'product_reviews'
DLQ_NAME = 'product_reviews_dlq'
DLX_NAME = 'product_reviews_dlx'

def connect():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    
    connection = None
    while connection is None:
        try:
            connection = pika.BlockingConnection(parameters)
            logger.info(f"Connected to RabbitMQ at {RABBITMQ_HOST}")
        except pika.exceptions.AMQPConnectionError:
            logger.warning("RabbitMQ not ready, retrying in 5 seconds...")
            time.sleep(5)
    return connection

def setup_queues(channel):
    """
    Declares the main queue, DLX, and DLQ.
    Configures the main queue to forward dead-lettered messages to DLX.
    """
    # 1. Declare Dead Letter Exchange (DLX)
    channel.exchange_declare(exchange=DLX_NAME, exchange_type='direct', durable=True)

    # 2. Declare Dead Letter Queue (DLQ)
    channel.queue_declare(queue=DLQ_NAME, durable=True)

    # 3. Bind DLQ to DLX
    channel.queue_bind(exchange=DLX_NAME, queue=DLQ_NAME, routing_key='dead_letter')

    # 4. Declare Main Queue with DLX arguments
    arguments = {
        'x-dead-letter-exchange': DLX_NAME,
        'x-dead-letter-routing-key': 'dead_letter'
    }
    channel.queue_declare(queue=QUEUE_NAME, durable=True, arguments=arguments)
    logger.info("Queues and DLQ configured successfully.")

def process_message(ch, method, properties, body, publisher):
    """Callback function to process messages."""
    review_id = "unknown"
    try:
        data = json.loads(body)
        review_id = data.get('reviewId')
        
        logger.info(f"Received review: {review_id}")

        # 1. Idempotency Check & DB Session
        session = next(get_db_session())
        existing_review = session.query(ProcessedReview).filter_by(review_id=review_id).first()
        
        if existing_review:
            logger.info(f"Review {review_id} already processed. Skipping.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # 2. Sentiment Analysis
        sentiment = analyze_sentiment(data.get('comment', ''))
        logger.info(f"Sentiment for {review_id}: {sentiment}")

        # 3. Save to Database
        new_review = ProcessedReview(
            review_id=review_id,
            product_id=data.get('productId'),
            user_id=data.get('userId'),
            rating=data.get('rating'),
            comment=data.get('comment'),
            sentiment=sentiment
        )
        session.add(new_review)
        session.commit()
        logger.info(f"Saved review {review_id} to DB.")

        # 4. Publish Event
        processed_event = {
            "reviewId": review_id,
            "sentiment": sentiment,
            "processedTimestamp": datetime.utcnow().isoformat()
        }
        publisher.publish(processed_event)

        # 5. Acknowledge
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON. Rejecting (to DLQ). Body: {body[:50]}...")
        # Reject without requeue triggers DLQ
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
    except IntegrityError:
        logger.error(f"Integrity Error for {review_id}. Concurrent write? Requeuing.")
        session.rollback()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    except Exception as e:
        logger.error(f"Error processing message {review_id}: {e}")
        # Retry logic: basic_nack with requeue=True will loop forever if persistent error. 
        # For production, we might want a retry count header check, but for now we'll requeue once or DLQ?
        # Let's send to DLQ for generic unknown errors to avoid infinite loops for this task.
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False) 
    finally:
        if 'session' in locals():
            session.close()

from datetime import datetime

def main():
    logger.info("Starting Review Processor...")
    
    while True:
        try:
            init_db()
            logger.info("Database initialized.")
            break
        except Exception as e:
            logger.warning(f"Database not ready: {e}. Retrying...")
            time.sleep(5)

    connection = connect()
    channel = connection.channel()

    # Setup Queues & DLQ
    setup_queues(channel)
    
    # Initialize Publisher
    publisher = EventPublisher(channel)

    channel.basic_qos(prefetch_count=1)
    
    # Use partial to pass publisher to callback
    from functools import partial
    on_message_callback = partial(process_message, publisher=publisher)
    
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message_callback)

    logger.info('Waiting for messages.')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
