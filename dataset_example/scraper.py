import requests
from bs4 import BeautifulSoup
import time
import random
import duckdb
import sqlite3
import json
import os
from urllib.parse import urljoin, urlparse
from datetime import datetime
import re
import shutil
from pathlib import Path

class EnhancedNewsArticleScraper:
    def __init__(self, db_path="sentiment_research.duckdb", backup_dir="backups"):
        self.db_path = db_path
        self.backup_dir = backup_dir
        self.batch_size = 50  # Save every 50 articles
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.setup_directories()
        self.setup_database()
        
    def setup_directories(self):
        """Create backup directory if it doesn't exist"""
        Path(self.backup_dir).mkdir(exist_ok=True)
        
    def setup_database(self):
        """Initialize DuckDB with required tables"""
        conn = duckdb.connect(self.db_path)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS news_sources (
                id INTEGER NOT NULL,
                name VARCHAR(255) NOT NULL,
                domain VARCHAR(255) UNIQUE,
                credibility_score INTEGER,
                source_type VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS article_id_seq START 1
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scraped_articles (
                id INTEGER DEFAULT nextval('article_id_seq'),
                source_id INTEGER,
                url TEXT UNIQUE,
                title TEXT,
                content TEXT,
                publish_date TIMESTAMP,
                scraped_at TIMESTAMP DEFAULT NOW(),
                word_count INTEGER
            )
        """)
        
        # Expanded list of 10 news sources
        sources = [
            (1, 'CNN', 'cnn.com', 7, 'news'),
            (2, 'CBS News', 'cbsnews.com', 8, 'news'),
            (3, 'Fox News', 'foxnews.com', 6, 'news'),
            (4, 'Reuters', 'reuters.com', 9, 'news'),
            (5, 'Associated Press', 'apnews.com', 9, 'news'),
            (6, 'BBC News', 'bbc.com', 8, 'news'),
            (7, 'The Guardian', 'theguardian.com', 8, 'news'),
            (8, 'NPR', 'npr.org', 8, 'news'),
            (9, 'ABC News', 'abcnews.go.com', 7, 'news'),
            (10, 'NBC News', 'nbcnews.com', 7, 'news')
        ]
        
        for source in sources:
            conn.execute("""
                INSERT OR IGNORE INTO news_sources 
                (id, name, domain, credibility_score, source_type) 
                VALUES (?, ?, ?, ?, ?)
            """, source)
        
        conn.close()
    
    def create_sqlite_backup(self, suffix=""):
        """Create SQLite3 backup of current data"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"news_backup_{timestamp}{suffix}.sqlite"
        backup_path = os.path.join(self.backup_dir, backup_filename)
        
        try:
            # Connect to DuckDB and SQLite
            duck_conn = duckdb.connect(self.db_path)
            sqlite_conn = sqlite3.connect(backup_path)
            
            # Create tables in SQLite
            sqlite_conn.execute("""
                CREATE TABLE IF NOT EXISTS news_sources (
                    id INTEGER NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    domain VARCHAR(255) UNIQUE,
                    credibility_score INTEGER,
                    source_type VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            sqlite_conn.execute("""
                CREATE TABLE IF NOT EXISTS scraped_articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER,
                    url TEXT UNIQUE,
                    title TEXT,
                    content TEXT,
                    publish_date TIMESTAMP,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    word_count INTEGER
                )
            """)
            
            # Copy data from DuckDB to SQLite
            sources = duck_conn.execute("SELECT * FROM news_sources").fetchall()
            for source in sources:
                sqlite_conn.execute("""
                    INSERT OR IGNORE INTO news_sources 
                    (id, name, domain, credibility_score, source_type, created_at) 
                    VALUES (?, ?, ?, ?, ?, ?)
                """, source)
            
            articles = duck_conn.execute("SELECT * FROM scraped_articles").fetchall()
            for article in articles:
                sqlite_conn.execute("""
                    INSERT OR IGNORE INTO scraped_articles 
                    (id, source_id, url, title, content, publish_date, scraped_at, word_count) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, article)
            
            sqlite_conn.commit()
            sqlite_conn.close()
            duck_conn.close()
            
            print(f"✓ SQLite backup created: {backup_path}")
            return backup_path
            
        except Exception as e:
            print(f"Error creating SQLite backup: {e}")
            return None
    
    def get_article_links(self, base_url, source_name, max_links=1000):
        """Enhanced link extraction for more sources"""
        try:
            response = self.session.get(base_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            links = set()
            
            # Try RSS first
            rss_links = soup.find_all('link', {'type': 'application/rss+xml'})
            if rss_links and len(rss_links) > 0:
                try:
                    rss_url = urljoin(base_url, rss_links[0].get('href'))
                    rss_response = self.session.get(rss_url, timeout=10)
                    rss_soup = BeautifulSoup(rss_response.content, 'xml')
                    
                    for item in rss_soup.find_all('item')[:max_links]:
                        link_elem = item.find('link')
                        if link_elem:
                            article_url = link_elem.get_text().strip()
                            domain = urlparse(base_url).netloc
                            if domain.replace('www.', '') in article_url:
                                links.add(article_url)
                    
                    if links:
                        print(f"Found {len(links)} articles via RSS for {source_name}")
                        return list(links)[:max_links]
                        
                except Exception as e:
                    print(f"RSS parsing failed for {source_name}, falling back to HTML parsing")
            
            # HTML parsing with expanded patterns
            links.update(self._extract_links_by_site(soup, base_url, source_name))
            
            print(f"Found {len(links)} potential article links for {source_name}")
            return list(links)[:max_links]
            
        except Exception as e:
            print(f"Error getting links from {source_name}: {e}")
            return []
    
    def _extract_links_by_site(self, soup, base_url, source_name):
        """Extract article links using expanded site-specific patterns"""
        links = set()
        domain = urlparse(base_url).netloc
        
        site_patterns = {
            'cnn.com': {
                'path_contains': ['/2024/', '/2025/', '/politics/', '/us/', '/world/', '/business/'],
                'exclude_patterns': ['video', 'live', 'gallery', 'photos'],
                'min_path_depth': 2
            },
            'cbsnews.com': {
                'path_starts': ['/news/', '/politics/', '/world/', '/health/', '/sports/', '/entertainment/', '/moneywatch/'],
                'min_path_depth': 2,
                'exclude_patterns': ['video', 'live', 'playlist', 'gallery', 'photo']
            },
            'foxnews.com': {
                'required_sections': ['/politics/', '/us/', '/world/', '/opinion/', '/category/'],
                'min_path_depth': 3,
                'exclude_patterns': ['video', 'live']
            },
            'reuters.com': {
                'path_contains': ['/world/', '/politics/', '/business/', '/technology/', '/markets/'],
                'exclude_patterns': ['video', 'graphics', 'picture'],
                'min_path_depth': 2
            },
            'apnews.com': {
                'path_starts': ['/article/', '/politics/', '/business/', '/technology/', '/health/'],
                'min_path_depth': 2,
                'exclude_patterns': ['video', 'photo', 'gallery']
            },
            'bbc.com': {
                'path_contains': ['/news/', '/world/', '/politics/', '/business/', '/technology/'],
                'exclude_patterns': ['video', 'live', 'iplayer', 'sounds'],
                'min_path_depth': 2
            },
            'theguardian.com': {
                'path_contains': ['/world/', '/politics/', '/business/', '/technology/', '/us-news/', '/uk-news/'],
                'exclude_patterns': ['video', 'live', 'gallery', 'audio'],
                'min_path_depth': 2
            },
            'npr.org': {
                'path_contains': ['/2024/', '/2025/', '/politics/', '/world/', '/business/', '/technology/'],
                'exclude_patterns': ['audio', 'podcasts', 'music'],
                'min_path_depth': 2
            },
            'abcnews.go.com': {
                'path_starts': ['/Politics/', '/US/', '/International/', '/Business/', '/Technology/'],
                'exclude_patterns': ['video', 'live', 'photo'],
                'min_path_depth': 2
            },
            'nbcnews.com': {
                'path_contains': ['/politics/', '/news/', '/world/', '/business/', '/tech/'],
                'exclude_patterns': ['video', 'live', 'slideshow'],
                'min_path_depth': 2
            }
        }
        
        pattern = None
        for site_domain, config in site_patterns.items():
            if site_domain in domain:
                pattern = config
                break
        
        if not pattern:
            # Generic pattern for unknown sites
            pattern = {
                'path_contains': ['/2024/', '/2025/', '/news/', '/article/'],
                'exclude_patterns': ['video', 'live', 'gallery', 'photo'],
                'min_path_depth': 1
            }
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            if self._matches_site_pattern(href, pattern):
                full_url = self._normalize_url(href, base_url, domain)
                if full_url:
                    links.add(full_url)
        
        return links
    
    def _matches_site_pattern(self, href, pattern):
        """Check if href matches the site-specific pattern"""
        href_lower = href.lower()
        
        if 'exclude_patterns' in pattern:
            if any(exclude in href_lower for exclude in pattern['exclude_patterns']):
                return False
        
        if 'path_starts' in pattern:
            if not any(href.startswith(start) for start in pattern['path_starts']):
                return False
        
        if 'path_contains' in pattern:
            if not any(required in href for required in pattern['path_contains']):
                return False
        
        if 'required_sections' in pattern:
            if not any(section in href for section in pattern['required_sections']):
                return False
        
        if 'min_path_depth' in pattern:
            if href.count('/') < pattern['min_path_depth']:
                return False
        
        return True
    
    def _normalize_url(self, href, base_url, domain):
        if href.startswith('/'):
            href = urljoin(base_url, href)
        
        if any(d in href for d in [domain.replace('www.', ''), domain]):
            return href
        
        return None
    
    def scrape_article_content(self, url, source_name):
        """Enhanced content extraction for all 10 sources"""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            title = ""
            content = ""
            publish_date = None
            
            # Extract title
            title_selectors = ['h1', '.headline', '.entry-title', '.article-title', 'title']
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title = title_elem.get_text().strip()
                    break
            
            # Site-specific content extraction
            if 'cnn.com' in url:
                content_div = soup.find('div', class_='article__content') or soup.find('div', {'data-module': 'ArticleBody'})
                if content_div:
                    paragraphs = content_div.find_all('p')
                    content = ' '.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
                    
            elif 'cbsnews.com' in url:
                content_div = soup.find('section', class_='content__body') or soup.find('div', class_='content__body')
                if content_div:
                    paragraphs = content_div.find_all('p')
                    content = ' '.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
                    
            elif 'foxnews.com' in url:
                content_div = soup.find('div', class_='article-body') or soup.find('div', class_='content')
                if content_div:
                    paragraphs = content_div.find_all('p')
                    content = ' '.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
                    
            elif 'reuters.com' in url:
                content_div = soup.find('div', {'data-module': 'ArticleBody'}) or soup.find('div', class_='StandardArticleBody_body')
                if content_div:
                    paragraphs = content_div.find_all('p')
                    content = ' '.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
                    
            elif 'apnews.com' in url:
                content_div = soup.find('div', class_='RichTextStoryBody') or soup.find('div', {'data-key': 'article'})
                if content_div:
                    paragraphs = content_div.find_all('p')
                    content = ' '.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
                    
            elif 'bbc.com' in url:
                content_div = soup.find('div', {'data-component': 'text-block'}) or soup.find('div', class_='story-body')
                if content_div:
                    paragraphs = content_div.find_all('p')
                    content = ' '.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
                    
            elif 'theguardian.com' in url:
                content_div = soup.find('div', class_='content__article-body') or soup.find('div', {'data-component': 'standfirst'})
                if content_div:
                    paragraphs = content_div.find_all('p')
                    content = ' '.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
                    
            elif 'npr.org' in url:
                content_div = soup.find('div', class_='storytext') or soup.find('div', id='storytext')
                if content_div:
                    paragraphs = content_div.find_all('p')
                    content = ' '.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
                    
            elif 'abcnews.go.com' in url:
                content_div = soup.find('div', class_='Article__Content') or soup.find('div', {'data-module': 'ArticleBody'})
                if content_div:
                    paragraphs = content_div.find_all('p')
                    content = ' '.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
                    
            elif 'nbcnews.com' in url:
                content_div = soup.find('div', class_='ArticleBody') or soup.find('div', {'data-module': 'ArticleBody'})
                if content_div:
                    paragraphs = content_div.find_all('p')
                    content = ' '.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
            
            # Fallback content extraction
            if not content:
                paragraphs = soup.find_all('p')
                content = ' '.join([p.get_text().strip() for p in paragraphs[:15] if p.get_text().strip()])
            
            content = re.sub(r'\s+', ' ', content).strip()
            word_count = len(content.split())
            
            return {
                'title': title,
                'content': content,
                'word_count': word_count,
                'publish_date': publish_date
            }
            
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None
    
    def scrape_news_site(self, base_url, source_name, target_articles=500):
        """Scrape articles from a news site with batch saving"""
        print(f"\n=== Scraping {source_name} (Target: {target_articles} articles) ===")
        
        # Get more links to account for failures
        article_links = self.get_article_links(base_url, source_name, target_articles * 3)
        
        # Try additional section URLs if not enough links
        if len(article_links) < target_articles:
            print(f"Only found {len(article_links)} {source_name} links, trying additional sections...")
            additional_links = self._get_additional_links(base_url, source_name)
            article_links.extend(additional_links)
            article_links = list(set(article_links))  # Remove duplicates
            print(f"Total {source_name} links after additional search: {len(article_links)}")
        
        if not article_links:
            print(f"No article links found for {source_name}")
            return 0
        
        scraped_count = 0
        batch_count = 0
        conn = duckdb.connect(self.db_path)
        
        source_id = conn.execute(
            "SELECT id FROM news_sources WHERE name = ?", 
            [source_name]
        ).fetchone()[0]
        
        for i, url in enumerate(article_links):
            if scraped_count >= target_articles:
                break
                
            # Check if already scraped
            existing = conn.execute(
                "SELECT COUNT(*) FROM scraped_articles WHERE url = ?", 
                [url]
            ).fetchone()[0]
            
            if existing > 0:
                continue
            
            print(f"Scraping ({scraped_count + 1}/{target_articles}): {url}")
            
            article_data = self.scrape_article_content(url, source_name)
            
            if article_data and article_data['content'] and article_data['word_count'] > 100:
                try:
                    conn.execute("""
                        INSERT INTO scraped_articles 
                        (source_id, url, title, content, word_count, publish_date) 
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, [
                        source_id, url, article_data['title'], 
                        article_data['content'], article_data['word_count'],
                        article_data['publish_date']
                    ])
                    scraped_count += 1
                    batch_count += 1
                    print(f"✓ Saved: {article_data['title'][:60]}...")
                    
                    # Save batch and create backup
                    if batch_count >= self.batch_size:
                        conn.commit()
                        print(f"  → Batch saved ({batch_count} articles)")
                        batch_count = 0
                        
                        # Create backup every 5 batches (250 articles)
                        if scraped_count % (self.batch_size * 5) == 0:
                            self.create_sqlite_backup(f"_{source_name}_{scraped_count}")
                            
                except Exception as e:
                    print(f"Error saving article: {e}")
            else:
                print(f"Skipped: insufficient content or failed to extract")
            
            # Random delay between requests
            time.sleep(random.uniform(1, 3))
        
        # Final commit for remaining articles
        if batch_count > 0:
            conn.commit()
            print(f"  → Final batch saved ({batch_count} articles)")
        
        conn.close()
        print(f"Successfully scraped {scraped_count} articles from {source_name}")
        return scraped_count
    
    def _get_additional_links(self, base_url, source_name):
        """Get additional links from section pages"""
        additional_links = []
        domain = urlparse(base_url).netloc
        
        # Define section URLs for each source
        section_urls = {
            'CNN': ['/politics', '/us', '/world', '/business', '/tech'],
            'CBS News': ['/news/', '/politics/', '/world/', '/health/', '/moneywatch/'],
            'Fox News': ['/politics', '/us', '/world', '/opinion'],
            'Reuters': ['/world/', '/politics/', '/business/', '/technology/'],
            'Associated Press': ['/politics/', '/business/', '/technology/', '/health/'],
            'BBC News': ['/news/world', '/news/politics', '/news/business', '/news/technology'],
            'The Guardian': ['/world', '/politics', '/business', '/technology', '/us-news'],
            'NPR': ['/politics/', '/world/', '/business/', '/technology/'],
            'ABC News': ['/Politics', '/US', '/International', '/Business'],
            'NBC News': ['/politics', '/news', '/world', '/business', '/tech']
        }
        
        sections = section_urls.get(source_name, [])
        
        for section in sections[:3]:  # Limit to 3 sections to avoid too many requests
            try:
                section_url = urljoin(base_url, section)
                print(f"  Checking section: {section_url}")
                
                response = self.session.get(section_url, timeout=10)
                soup = BeautifulSoup(response.content, 'html.parser')
                
                for link in soup.find_all('a', href=True)[:50]:  # Limit links per section
                    href = link['href']
                    if href.startswith('/'):
                        href = urljoin(base_url, href)
                    
                    if domain.replace('www.', '') in href and href not in additional_links:
                        additional_links.append(href)
                
                time.sleep(1)  # Delay between section requests
                
            except Exception as e:
                print(f"  Error checking section {section}: {e}")
        
        return additional_links
    
    def run_scraping_session(self):
        """Scrape articles from all 10 news sites"""
        sites = [
            ('https://www.cnn.com', 'CNN'),
            ('https://www.cbsnews.com', 'CBS News'),
            ('https://www.foxnews.com', 'Fox News'),
            ('https://www.reuters.com', 'Reuters'),
            ('https://apnews.com', 'Associated Press'),
            ('https://www.bbc.com/news', 'BBC News'),
            ('https://www.theguardian.com', 'The Guardian'),
            ('https://www.npr.org', 'NPR'),
            ('https://abcnews.go.com', 'ABC News'),
            ('https://www.nbcnews.com', 'NBC News')
        ]
        
        total_scraped = 0
        
        # Create initial backup
        print("Creating initial backup...")
        self.create_sqlite_backup("_initial")
        
        for base_url, source_name in sites:
            print(f"\n{'='*50}")
            print(f"Starting {source_name} - Target: 500 articles")
            print(f"{'='*50}")
            
            scraped = self.scrape_news_site(base_url, source_name, target_articles=500)
            total_scraped += scraped
            
            # Create backup after each source
            self.create_sqlite_backup(f"_after_{source_name.replace(' ', '_')}")
            
            # Brief pause between sources
            time.sleep(random.uniform(10, 20))
        
        print(f"\n{'='*60}")
        print(f"SCRAPING COMPLETE")
        print(f"{'='*60}")
        print(f"Total articles scraped: {total_scraped}")
        print(f"Target was: 5,000 articles")
        print(f"Success rate: {(total_scraped/5000)*100:.1f}%")
        
        # Create final comprehensive backup
        final_backup = self.create_sqlite_backup("_FINAL")
        print(f"Final backup created: {final_backup}")
        
        self.show_summary()
    
    def show_summary(self):
        """Display comprehensive summary of scraped data"""
        conn = duckdb.connect(self.db_path)
        
        summary = conn.execute("""
            SELECT 
                ns.name,
                COUNT(*) as article_count,
                AVG(sa.word_count) as avg_word_count,
                MIN(sa.word_count) as min_words,
                MAX(sa.word_count) as max_words,
                SUM(sa.word_count) as total_words
            FROM scraped_articles sa
            JOIN news_sources ns ON sa.source_id = ns.id
            GROUP BY ns.name
            ORDER BY article_count DESC
        """).fetchall()
        
        print("\n" + "="*80)
        print("COMPREHENSIVE SCRAPING SUMMARY")
        print("="*80)
        
        total_articles = 0
        total_words = 0
        
        for row in summary:
            name, count, avg_words, min_words, max_words, sum_words = row
            total_articles += count
            total_words += sum_words
            
            print(f"{name:15} | {count:4d} articles | "
                  f"Avg: {avg_words:5.0f} words | "
                  f"Range: {min_words:4d}-{max_words:5d} | "
                  f"Total: {sum_words:7,.0f} words")
        
        print("-" * 80)
        print(f"{'TOTAL':15} | {total_articles:4d} articles | "
              f"Avg: {total_words/total_articles if total_articles > 0 else 0:5.0f} words | "
              f"Grand Total: {total_words:7,.0f} words")
        
        print(f"\nDatabase file: {self.db_path}")
        print(f"Backups directory: {self.backup_dir}/")
        
        # List backup files
        backup_files = list(Path(self.backup_dir).glob("*.sqlite"))
        if backup_files:
            print(f"\nBackup files created: {len(backup_files)}")
            for backup in sorted(backup_files):
                size_mb = backup.stat().st_size / (1024 * 1024)
                print(f"  {backup.name} ({size_mb:.1f} MB)")
        
        conn.close()

if __name__ == "__main__":
    print("Enhanced News Article Scraper")
    print("Target: 5,000 articles from 10 news outlets")
    print("Features: Batch saving, SQLite backups, comprehensive error handling")
    print("-" * 60)
    
    scraper = EnhancedNewsArticleScraper()
    scraper.run_scraping_session()