import requests
from bs4 import BeautifulSoup
import time
import random
import duckdb
import json
from urllib.parse import urljoin, urlparse
from datetime import datetime
import re

class NewsArticleScraper:
    def __init__(self, db_path="sentiment_research.duckdb"):
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.setup_database()
        
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
        
        sources = [
            (1, 'CNN', 'cnn.com', 7, 'news'),
            (2, 'CBS News', 'cbsnews.com', 8, 'news'),
            (3, 'Fox News', 'foxnews.com', 6, 'news')
        ]
        
        for source in sources:
            conn.execute("""
                INSERT OR IGNORE INTO news_sources 
                (id, name, domain, credibility_score, source_type) 
                VALUES (?, ?, ?, ?, ?)
            """, source)
        
        conn.close()
    
    def get_article_links(self, base_url, source_name, max_links=35):
        try:
            response = self.session.get(base_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            links = set()
            
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
                            if base_url.split('//')[1] in article_url:
                                links.add(article_url)
                    
                    if links:
                        print(f"Found {len(links)} articles via RSS for {source_name}")
                        return list(links)[:max_links]
                        
                except Exception as e:
                    print(f"RSS parsing failed for {source_name}, falling back to HTML parsing")
            
            links.update(self._extract_links_by_site(soup, base_url, source_name))
            
            print(f"Found {len(links)} potential article links for {source_name}")
            return list(links)[:max_links]
            
        except Exception as e:
            print(f"Error getting links from {source_name}: {e}")
            return []
    
    def _extract_links_by_site(self, soup, base_url, source_name):
        """Extract article links using site-specific patterns"""
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
                'min_slug_length': 3,
                'exclude_patterns': ['video', 'live', 'playlist', 'gallery', 'photo']
            },
            'foxnews.com': {
                'required_sections': ['/politics/', '/us/', '/world/', '/opinion/', '/category/'],
                'min_path_depth': 3,
                'exclude_trailing_slash': True,
                'exclude_patterns': []
            }
        }
        
        pattern = None
        for site_domain, config in site_patterns.items():
            if site_domain in domain:
                pattern = config
                break
        
        if not pattern:
            return links
        
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
            if isinstance(pattern['path_starts'], list):
                if not any(href.startswith(start) for start in pattern['path_starts']):
                    return False
            else:
                if not href.startswith(pattern['path_starts']):
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
        
        if pattern.get('exclude_trailing_slash') and href.endswith('/'):
            return False
        
        if 'min_slug_length' in pattern:
            path_parts = href.split('/')
            if len(path_parts) >= 2:
                slug = path_parts[-2] if path_parts[-1] == '' else path_parts[-1]
                if len(slug) < pattern['min_slug_length']:
                    return False
        
        return True
    
    def _normalize_url(self, href, base_url, domain):
        if href.startswith('/'):
            href = urljoin(base_url, href)
        
        if domain in href:
            return href
        
        return None
    
    def scrape_article_content(self, url, source_name):
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            title = ""
            content = ""
            publish_date = None
            
            title_selectors = ['h1', '.headline', '.entry-title', 'title']
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title = title_elem.get_text().strip()
                    break
            
            # Extract content based on site ( I have to force this since each value is different)
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
            
            if not content:
                paragraphs = soup.find_all('p')
                content = ' '.join([p.get_text().strip() for p in paragraphs[:10] if p.get_text().strip()])
            
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
    
    def scrape_news_site(self, base_url, source_name, target_articles=35):
        """Scrape articles from a news site"""
        print(f"\n=== Scraping {source_name} ===")
        
        article_links = self.get_article_links(base_url, source_name, target_articles * 2)
        
        if ('CNN' in source_name or 'CBS' in source_name) and len(article_links) < 20:
            print(f"Only found {len(article_links)} {source_name} links, trying additional sources...")
            
            if 'CNN' in source_name:
                additional_urls = [
                    'https://www.cnn.com/politics',
                    'https://www.cnn.com/us',
                    'https://www.cnn.com/world',
                    'https://www.cnn.com/business'
                ]
                base_domain = 'cnn.com'
            else:  
                additional_urls = [
                    'https://www.cbsnews.com/latest/',
                    'https://www.cbsnews.com/news/',
                    'https://www.cbsnews.com/politics/',
                    'https://www.cbsnews.com/world/'
                ]
                base_domain = 'cbsnews.com'
            
            for additional_url in additional_urls:
                try:
                    print(f"Checking {additional_url}...")
                    response = self.session.get(additional_url, timeout=10)
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        
                        if 'CNN' in source_name:
                            if (('/2024/' in href or '/2025/' in href or 
                                 any(section in href for section in ['/politics/', '/us/', '/world/', '/business/'])) and
                                href.count('/') >= 3):
                                if href.startswith('/'):
                                    href = urljoin(f'https://www.{base_domain}', href)
                                if base_domain in href and href not in article_links:
                                    article_links.append(href)
                        else: 
                            if (href.startswith('/news/') or href.startswith('/politics/') or 
                                href.startswith('/world/') or href.startswith('/moneywatch/')) and href.count('/') >= 2:
                                if href.startswith('/'):
                                    href = urljoin(f'https://www.{base_domain}', href)
                                if base_domain in href and href not in article_links:
                                    article_links.append(href)
                    
                    time.sleep(1)  
                except Exception as e:
                    print(f"Error checking {additional_url}: {e}")
            
            print(f"Total {source_name} links after additional search: {len(article_links)}")
        
        if not article_links:
            print(f"No article links found for {source_name}")
            return 0
        
        scraped_count = 0
        conn = duckdb.connect(self.db_path)
        
        source_id = conn.execute(
            "SELECT id FROM news_sources WHERE name = ?", 
            [source_name]
        ).fetchone()[0]
        
        for i, url in enumerate(article_links):
            if scraped_count >= target_articles:
                break
                
            existing = conn.execute(
                "SELECT COUNT(*) FROM scraped_articles WHERE url = ?", 
                [url]
            ).fetchone()[0]
            
            if existing > 0:
                print(f"Skipping already scraped: {url}")
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
                    print(f"✓ Saved: {article_data['title'][:60]}...")
                except Exception as e:
                    print(f"Error saving article: {e}")
            else:
                print(f"Skipped: insufficient content or failed to extract")
            
            time.sleep(random.uniform(1, 3))
        
        conn.close()
        print(f"Successfully scraped {scraped_count} articles from {source_name}")
        return scraped_count
    
    def run_scraping_session(self):
        """Scrape articles from all news sites"""
        sites = [
            ('https://www.cnn.com', 'CNN'),
            ('https://www.cbsnews.com', 'CBS News'),
            ('https://www.foxnews.com', 'Fox News')
        ]
        
        total_scraped = 0
        
        for base_url, source_name in sites:
            scraped = self.scrape_news_site(base_url, source_name, target_articles=35)
            total_scraped += scraped
            
            time.sleep(random.uniform(5, 10))
        
        print(f"\n=== SCRAPING COMPLETE ===")
        print(f"Total articles scraped: {total_scraped}")
        
        self.show_summary()
    
    def show_summary(self):
        """Display summary of scraped data"""
        conn = duckdb.connect(self.db_path)
        
        summary = conn.execute("""
            SELECT 
                ns.name,
                COUNT(*) as article_count,
                AVG(sa.word_count) as avg_word_count,
                MIN(sa.word_count) as min_words,
                MAX(sa.word_count) as max_words
            FROM scraped_articles sa
            JOIN news_sources ns ON sa.source_id = ns.id
            GROUP BY ns.name
            ORDER BY article_count DESC
        """).fetchall()
        
        print("\n=== SCRAPING SUMMARY ===")
        for row in summary:
            print(f"{row[0]}: {row[1]} articles (avg: {row[2]:.0f} words, range: {row[3]}-{row[4]})")
        
        conn.close()

if __name__ == "__main__":
    scraper = NewsArticleScraper()
    scraper.run_scraping_session()