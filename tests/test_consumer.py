import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import json

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# --- AGGRESSIVE MOCKING START ---
# We must mock modules BEFORE they are imported by the code under test

# Mock pika
mock_pika = MagicMock()
sys.modules['pika'] = mock_pika
sys.modules['pika.exceptions'] = MagicMock()

# Mock sqlalchemy
mock_sqlalchemy = MagicMock()
sys.modules['sqlalchemy'] = mock_sqlalchemy
sys.modules['sqlalchemy.exc'] = MagicMock()
sys.modules['sqlalchemy.orm'] = MagicMock()

# Mock textblob
sys.modules['textblob'] = MagicMock()

# Mock src modules that have dependencies we want to avoid
mock_database = MagicMock()
sys.modules['src.database'] = mock_database

# We can let src.sentiment and src.publisher be imported if we mocked pika and textblob correctly,
# OR we can mock them too to be safe and isolate consumer completely.
# Let's mock them to be safe and strictly unit test consumer.py
mock_sentiment = MagicMock()
sys.modules['src.sentiment'] = mock_sentiment

mock_publisher = MagicMock()
sys.modules['src.publisher'] = mock_publisher

# --- AGGRESSIVE MOCKING END ---

# Now import the module under test
import src.consumer

class TestConsumerLogicV2(unittest.TestCase):
    
    def setUp(self):
        self.mock_ch = MagicMock()
        self.mock_method = MagicMock()
        self.mock_properties = MagicMock()
        self.mock_publisher = MagicMock()
        
    def test_process_valid_message(self):
        # Trigger the callback
        # process_message(ch, method, properties, body, publisher)
        
        # Setup specific mocks for this test
        # We need to mock what src.consumer uses: 
        #   src.database.get_db_session
        #   src.sentiment.analyze_sentiment
        #   src.publisher.EventPublisher (actually passed in, so we use self.mock_publisher)
        
        mock_session = MagicMock()
        # get_db_session is a generator
        mock_database.get_db_session.return_value = iter([mock_session])
        
        # Analyze sentiment returns string
        mock_sentiment.analyze_sentiment.return_value = 'POSITIVE'
        
        # Database query returns None (no existing review)
        mock_session.query.return_value.filter_by.return_value.first.return_value = None
        
        body = json.dumps({
            "reviewId": "rv_v2_1",
            "productId": "prod_1",
            "userId": "user_1",
            "rating": 5,
            "comment": "Nice!",
            "timestamp": "2023-01-01"
        }).encode('utf-8')
        
        # Call the function
        src.consumer.process_message(self.mock_ch, self.mock_method, self.mock_properties, body, self.mock_publisher)
        
        # Verifications
        mock_sentiment.analyze_sentiment.assert_called_with("Nice!")
        
        mock_session.add.assert_called()
        args, _ = mock_session.add.call_args
        # The object added should be an instance of ProcessedReview (which is a mock)
        # We can check its attributes if the mock constructor stored them, but MagicMock defaults don't simplify that.
        # usually verify that it was called.
        
        self.mock_publisher.publish.assert_called()
        pub_args = self.mock_publisher.publish.call_args[0][0]
        self.assertEqual(pub_args['reviewId'], "rv_v2_1")
        self.assertEqual(pub_args['sentiment'], "POSITIVE")
        
        self.mock_ch.basic_ack.assert_called()

    def test_process_duplicate(self):
        mock_session = MagicMock()
        mock_database.get_db_session.return_value = iter([mock_session])
        
        # Database query returns Something
        mock_session.query.return_value.filter_by.return_value.first.return_value = MagicMock()
        
        body = json.dumps({
            "reviewId": "rv_dup",
            "productId": "prod_1",
            "userId": "user_1",
            "rating": 5,
            "comment": "Duplicate review"
        }).encode('utf-8')
        
        src.consumer.process_message(self.mock_ch, self.mock_method, self.mock_properties, body, self.mock_publisher)
        
        mock_session.add.assert_not_called()
        self.mock_publisher.publish.assert_not_called()
        self.mock_ch.basic_ack.assert_called()

    def test_malformed_json(self):
        body = b"invalid json"
        src.consumer.process_message(self.mock_ch, self.mock_method, self.mock_properties, body, self.mock_publisher)
        
        # Should reject
        # Should reject
        self.mock_ch.basic_reject.assert_called_with(delivery_tag=self.mock_method.delivery_tag, requeue=False)

    def test_missing_fields(self):
        body = json.dumps({
            "reviewId": "rv_missing",
            # Missing productId, userId, rating
            "comment": "Incomplete payload"
        }).encode('utf-8')
        
        src.consumer.process_message(self.mock_ch, self.mock_method, self.mock_properties, body, self.mock_publisher)
        
        # Should reject to DLQ
        self.mock_ch.basic_reject.assert_called_with(delivery_tag=self.mock_method.delivery_tag, requeue=False)

    def test_missing_critical_fields(self):
        # Test missing reviewId
        body_no_id = json.dumps({
            "productId": "prod_1",
            "userId": "user_1",
            "rating": 5,
            "comment": "No ID here"
        }).encode('utf-8')
        
        src.consumer.process_message(self.mock_ch, self.mock_method, self.mock_properties, body_no_id, self.mock_publisher)
        self.mock_ch.basic_reject.assert_called_with(delivery_tag=self.mock_method.delivery_tag, requeue=False)
        
        # Reset mocks
        self.mock_ch.reset_mock()
        
        # Test missing comment
        body_no_comment = json.dumps({
            "reviewId": "rv_no_comment",
            "productId": "prod_1",
            "userId": "user_1",
            "rating": 5
        }).encode('utf-8')
        
        src.consumer.process_message(self.mock_ch, self.mock_method, self.mock_properties, body_no_comment, self.mock_publisher)
        self.mock_ch.basic_reject.assert_called_with(delivery_tag=self.mock_method.delivery_tag, requeue=False)

    def test_integrity_error(self):
        """Test that IntegrityError triggers an Ack (treating as duplicate)."""
        mock_session = MagicMock()
        mock_database.get_db_session.return_value = iter([mock_session])
        
        # Database query returns None (so it tries to insert)
        mock_session.query.return_value.filter_by.return_value.first.return_value = None
        
        # Simulate IntegrityError on commit
        mock_session.commit.side_effect = src.sqlalchemy.exc.IntegrityError(None, None, None)
        
        body = json.dumps({
            "reviewId": "rv_integrity_fail",
            "productId": "prod_1",
            "userId": "user_1",
            "rating": 5,
            "comment": "Integrity Error Test"
        }).encode('utf-8')
        
        src.consumer.process_message(self.mock_ch, self.mock_method, self.mock_properties, body, self.mock_publisher)
        
        # Should Ack (treated as duplicate)
        self.mock_ch.basic_ack.assert_called_with(delivery_tag=self.mock_method.delivery_tag)
        # Should NOT Nack/Requeue
        self.mock_ch.basic_nack.assert_not_called()


if __name__ == '__main__':
    unittest.main()
