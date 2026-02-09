import pika
import os
import json
import uuid
import datetime
import sys

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
    mode = sys.argv[1] if len(sys.argv) > 1 else "success"
    
    connection = connect()
    channel = connection.channel()
    # Match Queue Arguments from Consumer
    arguments = {
        'x-dead-letter-exchange': 'product_reviews_dlx',
        'x-dead-letter-routing-key': 'dead_letter'
    }
    channel.queue_declare(queue=QUEUE_NAME, durable=True, arguments=arguments)

    review_id = f"rv_{uuid.uuid4().hex[:8]}"
    
    if mode == "malformed":
        # Send invalid JSON
        body = "This is not json"
        print(f" [x] Sending malformed message: {body}")
    else:
        # Send valid JSON
        message = {
            "reviewId": review_id,
            "productId": "prod_123",
            "userId": "user_456",
            "rating": 5,
            "comment": "This product is amazing! I love it.",
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        body = json.dumps(message)
        print(f" [x] Sent review {review_id}")

    channel.basic_publish(
        exchange='',
        routing_key=QUEUE_NAME,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    
    connection.close()

if __name__ == '__main__':
    main()
