from textblob import TextBlob

def analyze_sentiment(text: str) -> str:
    """
    Analyzes the sentiment of a given text using TextBlob.
    Returns: 'POSITIVE', 'NEGATIVE', or 'NEUTRAL'
    """
    if not text:
        return 'NEUTRAL'
        
    analysis = TextBlob(text)
    polarity = analysis.sentiment.polarity

    if polarity > 0.1:
        return 'POSITIVE'
    elif polarity < -0.1:
        return 'NEGATIVE'
    else:
        return 'NEUTRAL'
