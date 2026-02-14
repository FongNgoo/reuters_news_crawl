# src/sources/guardian/search.py

from datetime import date, datetime
from typing import List, Optional

from common.http import HttpClient
from common.logger import get_logger
from common.rss_parser import RSSParser, RSSItem
from schema.news import ArticleMeta

logger = get_logger(__name__)


class GuardianSearcher:
    """Guardian RSS feed searcher"""
    
    def __init__(self, http_client: HttpClient, rss_feeds: List[str]):
        self.http = http_client
        self.rss_parser = RSSParser(http_client)
        self.rss_feeds = rss_feeds
    
    def search(
        self,
        company_name: str,
        start_date: date,
        end_date: date
    ) -> List[ArticleMeta]:
        """
        Search Guardian articles via RSS feeds
        
        Note: RSS feeds typically only contain recent articles (last ~30 days)
        So historical search is limited
        """
        logger.info(
            f"Searching Guardian | company={company_name} "
            f"range={start_date} → {end_date}"
        )
        
        all_items: List[RSSItem] = []
        
        # Fetch all RSS feeds
        for feed_url in self.rss_feeds:
            items = self.rss_parser.parse_feed(feed_url)
            all_items.extend(items)
        
        logger.info(f"Total RSS items fetched: {len(all_items)}")
        
        # Filter by company name and date range
        filtered = self._filter_by_company_and_date(
            all_items,
            company_name,
            start_date,
            end_date
        )
        
        # Convert to ArticleMeta
        results = self._convert_to_article_meta(filtered)
        
        logger.info(
            f"Guardian search completed | "
            f"total={len(all_items)} filtered={len(results)}"
        )
        
        return results
    
    def _filter_by_company_and_date(
        self,
        items: List[RSSItem],
        company_name: str,
        start_date: date,
        end_date: date
    ) -> List[RSSItem]:
        """Filter RSS items by company name and date range"""
        filtered = []
        company_lower = company_name.lower()
        
        # ✅ FIX: Also try common variations
        search_terms = [company_lower]
        
        # Add common company name variations
        if company_lower == "apple":
            search_terms.extend(["iphone", "ipad", "mac", "tim cook"])
        elif company_lower == "microsoft":
            search_terms.extend(["windows", "azure", "satya nadella"])
        elif company_lower == "tesla":
            search_terms.extend(["elon musk", "ev", "electric vehicle"])
        elif company_lower == "amazon":
            search_terms.extend(["aws", "jeff bezos", "andy jassy"])
        
        for item in items:
            # Check if company name is mentioned in title or description
            title_lower = item.title.lower()
            desc_lower = item.description.lower() if item.description else ""
            
            # ✅ FIX: More flexible matching
            found = False
            for term in search_terms:
                if term in title_lower or term in desc_lower:
                    found = True
                    break
            
            if not found:
                continue
            
            # Check date range
            if item.published_at:
                pub_date = item.published_at.date()
                if not (start_date <= pub_date <= end_date):
                    logger.debug(f"Skipping (date): {item.title} ({pub_date})")
                    continue
            
            filtered.append(item)
            logger.debug(f"Matched: {item.title}")
        
        return filtered
    
    def _convert_to_article_meta(self, items: List[RSSItem]) -> List[ArticleMeta]:
        """Convert RSS items to ArticleMeta"""
        results = []
        
        for item in items:
            results.append(
                ArticleMeta(
                    url=item.link,
                    title=item.title,
                    published_at=item.published_at or datetime.now(),
                    source="guardian",
                    search_query=None
                )
            )
        
        return results
    
    def get_latest_articles(self, limit: int = 50) -> List[ArticleMeta]:
        """Get latest articles from all feeds (useful for daily updates)"""
        logger.info(f"Fetching latest Guardian articles (limit={limit})")
        
        all_items: List[RSSItem] = []
        
        for feed_url in self.rss_feeds:
            items = self.rss_parser.parse_feed(feed_url)
            all_items.extend(items)
        
        # Sort by date (newest first)
        all_items.sort(
            key=lambda x: x.published_at or datetime.min,
            reverse=True
        )
        
        # Take top N
        latest = all_items[:limit]
        
        return self._convert_to_article_meta(latest)
