import re

class EntityProcessor:
    def __init__(self, target_entity):
        self.target_entity = target_entity
    
    def extract_entity_sentences(self, content, min_length=100, max_length=400):
        """Extract sentences mentioning the target entity"""
        if not self.target_entity:
            return []
            
        sentences = re.split(r'[.!?]+', content)
        candidates = []
        entity_lower = self.target_entity.lower()
        
        for sentence in sentences:
            sentence = sentence.strip()
            
            # Filter by length and entity presence
            if (len(sentence) < min_length or 
                len(sentence) > max_length or 
                entity_lower not in sentence.lower()):
                continue
            
            candidates.append(sentence)
        
        return candidates
    
    def create_asp_format(self, sentence):
        """Wrap the target entity in ASP tags"""
        if not self.target_entity:
            return sentence
            
        entity_variations = self._get_entity_variations()
        
        # Find and wrap the first occurrence
        for variation in entity_variations:
            pattern = re.compile(r'\b' + re.escape(variation) + r'\b', re.IGNORECASE)
            if pattern.search(sentence):
                asp_sentence = pattern.sub(f'[ASP]{variation.title()}[ASP]', sentence, count=1)
                return asp_sentence
        
        # Fallback
        pattern = re.compile(r'\b' + re.escape(self.target_entity) + r'\b', re.IGNORECASE)
        asp_sentence = pattern.sub(f'[ASP]{self.target_entity.title()}[ASP]', sentence, count=1)
        return asp_sentence
    
    def _get_entity_variations(self):
        """Get common variations for the entity"""
        entity = self.target_entity.lower()
        
        if entity in ['trump', 'donald trump']:
            return ['trump', 'donald trump', 'president trump', 'mr. trump']
        elif entity in ['pelosi', 'nancy pelosi']:
            return ['pelosi', 'nancy pelosi', 'speaker pelosi', 'ms. pelosi']
        elif entity == 'ukraine':
            return ['ukraine', 'ukrainian']
        else:
            return [self.target_entity]