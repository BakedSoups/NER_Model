import duckdb

class DatabaseQueries:
    def __init__(self, db_path="sentiment_research.duckdb"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
    
    def get_articles_mentioning_entity(self, source_name, entity, limit=50):
        """Get articles from source that mention the entity"""
        entity_lower = entity.lower()

        query = """
        SELECT 
            sa.id,
            sa.title,
            sa.content,
            sa.word_count,
            sa.url,
            ns.name as source
        FROM scraped_articles sa
        JOIN news_sources ns ON sa.source_id = ns.id
        WHERE ns.name ILIKE ?
        AND sa.word_count BETWEEN 200 AND 1500
        AND sa.content IS NOT NULL
        AND LENGTH(sa.content) > 300
        AND (LOWER(sa.content) LIKE ? OR LOWER(sa.title) LIKE ?)
        ORDER BY sa.word_count DESC
        LIMIT ?
        """

        pattern = f"%{source_name}%"
        results = self.conn.execute(query, [pattern, f"%{entity_lower}%", f"%{entity_lower}%", limit]).fetchall()

        articles = [
            {
                'id': int(row[0]),
                'title': row[1],
                'content': row[2],
                'word_count': row[3],
                'url': row[4],
                'source': row[5]
            }
            for row in results
        ]

        return articles

    
    def get_database_summary(self):
        """Get summary of all sources and articles"""
        sources = self.conn.execute("SELECT * FROM news_sources").fetchall()
        
        counts = self.conn.execute("""
            SELECT ns.name, COUNT(*) as count, AVG(sa.word_count) as avg_words
            FROM scraped_articles sa
            JOIN news_sources ns ON sa.source_id = ns.id
            GROUP BY ns.name
            ORDER BY count DESC
        """).fetchall()
        
        return sources, counts
    
    def search_entity_across_sources(self, entity):
        """Search for entity mentions across all sources"""
        entity_lower = entity.lower()
        
        articles_with_entity = self.conn.execute("""
            SELECT sa.title, sa.word_count, ns.name, sa.content
            FROM scraped_articles sa
            JOIN news_sources ns ON sa.source_id = ns.id
            WHERE LOWER(sa.content) LIKE ?
            OR LOWER(sa.title) LIKE ?
            ORDER BY ns.name, sa.word_count DESC
        """, [f"%{entity_lower}%", f"%{entity_lower}%"]).fetchall()
        
        return articles_with_entity
    
    def close(self):
        """Close database connection"""
        self.conn.close()