import json
import logging
import pika

logger = logging.getLogger(__name__)

class EventPublisher:
    def __init__(self, channel, exchange_name='review_events', routing_key='review.processed'):
        """
        Initializes the EventPublisher.
        
        Args:
            channel: The RabbitMQ channel to use for publishing.
            exchange_name (str): The name of the exchange to publish to.
            routing_key (str): The routing key for the messages.
        """
        self.channel = channel
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        
        # Declare the exchange to ensure it exists
        try:
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic', durable=True)
            logger.info(f"Exchange '{self.exchange_name}' declared successfully.")
        except Exception as e:
            logger.error(f"Failed to declare exchange '{self.exchange_name}': {e}")
            raise

    def publish(self, event_data):
        """
        Publishes an event to the configured exchange.

        Args:
            event_data (dict): The dictionary containing the event data.
        """
        try:
            message_body = json.dumps(event_data)
            
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.routing_key,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )
            logger.info(f"Published event to '{self.exchange_name}' with key '{self.routing_key}': {event_data.get('reviewId', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            # Depending on requirements, we might want to re-raise this to trigger a retry in the consumer
            raise
