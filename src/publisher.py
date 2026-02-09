import pika
import json
import os
import logging

logger = logging.getLogger(__name__)

class EventPublisher:
    def __init__(self, channel=None):
        self.channel = channel
        self.output_queue = 'review_processed'
        if self.channel:
            self.channel.queue_declare(queue=self.output_queue, durable=True)

    def publish(self, review_data):
        """
        Publishes the processed review data to the output queue.
        """
        try:
            if not self.channel:
                logger.error("Publisher channel is not initialized.")
                return

            self.channel.basic_publish(
                exchange='',
                routing_key=self.output_queue,
                body=json.dumps(review_data),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Persistent message
                )
            )
            logger.info(f"Published ReviewProcessed event for {review_data.get('reviewId')}")
        except Exception as e:
            logger.error(f"Failed to publish event for {review_data.get('reviewId')}: {e}")
