import sys
import os

# Add the current directory to sys.path to ensure local imports work
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pika
import time
import json
from sqlalchemy.exc import IntegrityError
from src.database import init_db, get_db_session, ProcessedReview
from src.sentiment import analyze_sentiment

# Configuration
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')
QUEUE_NAME = 'product_reviews'

def connect():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    
    connection = None
    while connection is None:
        try:
            connection = pika.BlockingConnection(parameters)
            print(f"Connected to RabbitMQ at {RABBITMQ_HOST}")
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ not ready, retrying in 5 seconds...")
            time.sleep(5)
    return connection

def process_message(ch, method, properties, body):
    """Callback function to process messages."""
    try:
        data = json.loads(body)
        review_id = data.get('reviewId')
        
        print(f" [x] Received review: {review_id}")

        # 1. Idempotency Check & DB Session
        session = next(get_db_session())
        existing_review = session.query(ProcessedReview).filter_by(review_id=review_id).first()
        
        if existing_review:
            print(f" [!] Review {review_id} already processed. Skipping.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # 2. Sentiment Analysis
        sentiment = analyze_sentiment(data.get('comment', ''))
        print(f" [x] Sentiment for {review_id}: {sentiment}")

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
        print(f" [x] Saved review {review_id} to DB.")

        # 4. Acknowledge
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        print(" [!] Failed to decode JSON. Moving to DLQ (TODO).")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False) # Drop or DLQ
    except IntegrityError:
        print(f" [!] Integrity Error for {review_id} (Concurrent write?). Rolling back.")
        session.rollback()
        # Retry logic could be handled here or by Nack with requeue=True
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True) 
    except Exception as e:
        print(f" [!] Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True) # Retry
    finally:
        if 'session' in locals():
            session.close()

def main():
    print("Starting Review Processor...")
    
    # Initialize DB (create tables)
    # retry loop for DB connection availability
    while True:
        try:
            init_db()
            print("Database initialized.")
            break
        except Exception as e:
            print(f"Database not ready: {e}. Retrying...")
            time.sleep(5)

    connection = connect()
    channel = connection.channel()

    # Declare the queue (idempotent)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_message)

    print(' [*] Waiting for messages.')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
