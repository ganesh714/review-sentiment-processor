import pika
import os
import json
import uuid
import datetime

# Configuration
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')
QUEUE_NAME = 'product_reviews'

def connect():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    return pika.BlockingConnection(parameters)

def main():
    connection = connect()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # Mock Data
    review_id = f"rv_{uuid.uuid4().hex[:8]}"
    message = {
        "reviewId": review_id,
        "productId": "prod_123",
        "userId": "user_456",
        "rating": 5,
        "comment": "This product is amazing! I love it.",
        "timestamp": datetime.datetime.utcnow().isoformat()
    }

    channel.basic_publish(
        exchange='',
        routing_key=QUEUE_NAME,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    
    print(f" [x] Sent review {review_id}")
    connection.close()

if __name__ == '__main__':
    main()
