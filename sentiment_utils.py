import re

positive_words = ['soft', 'cute', 'adorable', 'love', 'great', 'quality', 'well-made', 'luxurious', 'amazing', 'helpful', 'perfect', 'joyful', 'durable']
negative_words = ['expensive', 'overpriced', 'fake', 'imitation', 'delay', 'dissappointed', 'poor', 'issue', 'cancelled', 'bad', 'waste', 'floppy', 'light']

def clean_text(text):
    clean_text = re.sub('<.*?>', ' ', text)
    clean_text = re.sub('\\s+', ' ', clean_text).strip()
    return clean_text

def analyze_sentiment(text):
    if not text:
        return 'neutral'
    lower_text = clean_text(text).lower()
    pos_count = sum(lower_text.count(word) for word in positive_words)
    neg_count = sum(lower_text.count(word) for word in negative_words)
    if pos_count > neg_count:
        return 'positive'
    elif neg_count > pos_count:
        return 'negative'
    else:
        return 'neutral'
