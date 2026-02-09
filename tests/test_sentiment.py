import unittest
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.sentiment import analyze_sentiment

class TestSentimentAnalysis(unittest.TestCase):

    def test_positive_sentiment(self):
        text = "This product is amazing and I love it!"
        result = analyze_sentiment(text)
        self.assertEqual(result, 'POSITIVE')

    def test_negative_sentiment(self):
        text = "This is terrible. Worst purchase ever."
        result = analyze_sentiment(text)
        self.assertEqual(result, 'NEGATIVE')

    def test_neutral_sentiment(self):
        text = "It is a product."
        result = analyze_sentiment(text)
        self.assertEqual(result, 'NEUTRAL')

    def test_empty_text(self):
        result = analyze_sentiment("")
        self.assertEqual(result, 'NEUTRAL')

if __name__ == '__main__':
    unittest.main()
