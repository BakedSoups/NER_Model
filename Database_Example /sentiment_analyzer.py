import re

class SentimentAnalyzer:
    def __init__(self):
        self.positive_indicators = {
            'strong': ['excellent', 'amazing', 'fantastic', 'outstanding', 'brilliant', 'perfect', 'love', 'thrilled', 'impressed', 'successful'],
            'medium': ['good', 'great', 'effective', 'improved', 'helpful', 'valuable', 'confident', 'promising', 'positive'],
            'weak': ['better', 'nice', 'fine', 'okay', 'decent']
        }
        
        self.negative_indicators = {
            'strong': ['terrible', 'awful', 'horrible', 'disgusting', 'outrageous', 'devastating', 'disaster', 'failed', 'useless'],
            'medium': ['bad', 'poor', 'disappointing', 'problematic', 'concerning', 'frustrated', 'difficult', 'struggling'],
            'weak': ['issues', 'problems', 'concerns', 'challenges', 'confusion']
        }
        
        self.transition_words = ['however', 'but', 'although', 'despite', 'unfortunately', 'fortunately', 'nevertheless', 'yet', 'still', 'though', 'while']
    
    def calculate_sentiment_scores(self, text):
        """Calculate positive and negative sentiment scores"""
        text_lower = text.lower()
        
        pos_score = 0
        neg_score = 0
        
        # Count sentiment indicators with weights
        for strength, words in self.positive_indicators.items():
            multiplier = {'strong': 3, 'medium': 2, 'weak': 1}[strength]
            for word in words:
                pos_score += text_lower.count(word) * multiplier
        
        for strength, words in self.negative_indicators.items():
            multiplier = {'strong': 3, 'medium': 2, 'weak': 1}[strength]
            for word in words:
                neg_score += text_lower.count(word) * multiplier
        
        return pos_score, neg_score
    
    def analyze_sentence_sentiment(self, sentence):
        """Analyze sentiment of a single sentence"""
        pos_score, neg_score = self.calculate_sentiment_scores(sentence)
        has_transition = any(trans in sentence.lower() for trans in self.transition_words)
        
        # Determine sentiment
        if pos_score > 0 and neg_score == 0:
            sentiment = "Positive"
            confidence = min(pos_score * 2, 10)
        elif neg_score > 0 and pos_score == 0:
            sentiment = "Negative" 
            confidence = min(neg_score * 2, 10)
        elif pos_score > neg_score * 1.5:
            sentiment = "Positive"
            confidence = min((pos_score - neg_score), 8)
        elif neg_score > pos_score * 1.5:
            sentiment = "Negative"
            confidence = min((neg_score - pos_score), 8)
        else:
            return None  # Unclear sentiment
        
        # Bonus for transitions (sentiment complexity)
        if has_transition:
            confidence += 2
        
        return {
            'sentiment': sentiment,
            'confidence': confidence,
            'pos_score': pos_score,
            'neg_score': neg_score,
            'has_transition': has_transition
        }