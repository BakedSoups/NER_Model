from sentiment_extractor import SentimentExtractor

def main():
    # Get entity from user input
    entity = input("Enter the entity to search for (e.g., 'Trump', 'Nancy Pelosi', 'Ukraine'): ").strip()
    
    if not entity:
        print("No entity provided. Exiting.")
        return
    
    print(f"Searching for sentiment examples about: {entity}")
    
    # Initialize extractor
    extractor = SentimentExtractor()
    
    try:
        # Extract top examples for the specified entity
        results = extractor.extract_top_examples(entity)
        
        # Preview results
        extractor.preview_examples(results)
        
    finally:
        extractor.close()

if __name__ == "__main__":
    main()