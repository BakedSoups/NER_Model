import json
from database_queries import DatabaseQueries
from sentiment_analyzer import SentimentAnalyzer
from entity_processor import EntityProcessor

class SentimentExtractor:
    def __init__(self, db_path="sentiment_research.duckdb"):
        self.db_queries = DatabaseQueries(db_path)
        self.sentiment_analyzer = SentimentAnalyzer()
        self.entity_processor = None
        self.target_entity = None
    
    def set_target_entity(self, entity):
        """Set the target entity for extraction"""
        self.target_entity = entity
        self.entity_processor = EntityProcessor(entity)
    
    def process_article(self, article):
        """Extract sentiment examples from one article"""
        if not self.entity_processor:
            return []
        
        candidates = self.entity_processor.extract_entity_sentences(article['content'])
        examples = []
        
        for sentence in candidates:
            sentiment_result = self.sentiment_analyzer.analyze_sentence_sentiment(sentence)
            
            if sentiment_result and sentiment_result['confidence'] >= 3:
                asp_text = self.entity_processor.create_asp_format(sentence)
                
                example = {
                    "text": asp_text,
                    "expected_sentiments": [sentiment_result['sentiment']],
                    "category": 'long_text_limitation' if sentiment_result['has_transition'] else f'{self.target_entity.lower()}_sentiment',
                    "confidence_score": sentiment_result['confidence'],
                    "notes": f"Entity: {self.target_entity}. Pos:{sentiment_result['pos_score']} Neg:{sentiment_result['neg_score']}. Transition:{sentiment_result['has_transition']}",
                    "source_article": {
                        "title": article['title'],
                        "url": article['url'],
                        "source": article['source']
                    }
                }
                examples.append(example)
        
        return examples
    
    def get_top_examples_by_source(self, source_name, top_n=3):
        """Get the most confident examples from a specific source"""
        print(f"\n=== Processing {source_name} Articles ===")
        
        articles = self.db_queries.get_articles_mentioning_entity(source_name, self.target_entity, limit=50)
        print(f"Found {len(articles)} articles from {source_name} mentioning '{self.target_entity}'")
        
        if not articles:
            return []
        
        all_examples = []
        
        for article in articles:
            examples = self.process_article(article)
            all_examples.extend(examples)
            if examples:
                print(f"  → {len(examples)} examples from: {article['title'][:50]}...")
        
        # Sort by confidence and take top N
        top_examples = sorted(all_examples, key=lambda x: x['confidence_score'], reverse=True)[:top_n]
        print(f"Selected top {len(top_examples)} most confident examples from {source_name}")
        
        return top_examples
    
    def extract_top_examples(self, entity):
        """Main method - get top examples for specified entity"""
        self.set_target_entity(entity)
        print(f"=== EXTRACTING TOP SENTIMENT EXAMPLES FOR '{entity.upper()}' ===")
        
        # Get top examples from each source
        cbs_examples = self.get_top_examples_by_source("CBS", top_n=3)
        fox_examples = self.get_top_examples_by_source("Fox", top_n=3)
        cnn_examples = self.get_top_examples_by_source("CNN", top_n=3)
        
        # Combine results
        all_examples = {
            "target_entity": entity,
            "cbs_examples": cbs_examples,
            "fox_examples": fox_examples,
            "cnn_examples": cnn_examples,
            "total_examples": len(cbs_examples) + len(fox_examples) + len(cnn_examples)
        }
        
        # Save to file
        filename = f"sentiment_examples_{entity.lower().replace(' ', '_')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_examples, f, indent=2, ensure_ascii=False)
        
        print(f"\n=== RESULTS FOR '{entity.upper()}' ===")
        print(f"CBS News: {len(cbs_examples)} examples")
        print(f"Fox News: {len(fox_examples)} examples")
        print(f"CNN: {len(cnn_examples)} examples")
        print(f"Total: {all_examples['total_examples']} examples")
        print(f"Saved to: {filename}")
        
        return all_examples
    
    def preview_examples(self, examples_data):
        """Show preview of extracted examples"""
        print(f"\n=== PREVIEW OF TOP EXAMPLES ===")
        
        sources = [("CBS News", examples_data.get('cbs_examples', [])), 
                  ("Fox News", examples_data.get('fox_examples', [])),
                  ("CNN", examples_data.get('cnn_examples', []))]
        
        for source, examples in sources:
            print(f"\n--- {source} ---")
            
            if not examples:
                print("   No examples found")
                continue
                
            for i, example in enumerate(examples, 1):
                print(f"\n{i}. Confidence: {example['confidence_score']}")
                print(f"   Text: {example['text']}")
                print(f"   Sentiment: {example['expected_sentiments'][0]}")
                print(f"   Category: {example['category']}")
                print(f"   Source: {example['source_article']['title'][:60]}...")
    
    def close(self):
        """Close database connections"""
        self.db_queries.close()