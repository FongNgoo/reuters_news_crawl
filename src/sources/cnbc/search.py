# src/sources/cnbc/search.py

from datetime import date, datetime
from typing import List
from bs4 import BeautifulSoup
import urllib.parse

from common.http import HttpClient
from common.logger import get_logger
from schema.news import ArticleMeta

logger = get_logger(__name__)


class CNBCSearcher:
    """Search CNBC articles"""
    
    def __init__(self, http_client: HttpClient, sections: List[str]):
        self.http = http_client
        self.sections = sections or []
        self.base_url = "https://www.cnbc.com"
    
    def search(
        self,
        company_name: str,
        start_date: date,
        end_date: date
    ) -> List[ArticleMeta]:
        """Search CNBC articles using site search"""
        logger.info(f"Searching CNBC | company={company_name}")
        
        # ✅ FIX: Primarily use search function
        search_results = self._search_via_search_page(company_name)
        
        # Also try sections if search returns few results
        if len(search_results) < 5:
            for section in self.sections:
                section_results = self._search_section(section, company_name)
                search_results.extend(section_results)
        
        # Deduplicate
        unique_results = self._deduplicate(search_results)
        
        logger.info(
            f"CNBC search completed | "
            f"total={len(search_results)} unique={len(unique_results)}"
        )
        
        return unique_results
    
    def _search_via_search_page(self, company_name: str) -> List[ArticleMeta]:
        """Search using CNBC's search functionality"""
        # ✅ FIX: Updated search URL format
        query = urllib.parse.quote(company_name)
        search_url = f"{self.base_url}/search/?query={query}"
        
        logger.debug(f"Fetching search page: {search_url}")
        
        html = self.http.get(search_url)
        if not html:
            logger.warning("Failed to fetch CNBC search page")
            return []
        
        soup = BeautifulSoup(html, "html.parser")
        results: List[ArticleMeta] = []
        
        # ✅ FIX: Updated selectors for CNBC search results
        # CNBC uses various card styles
        result_items = []
        
        # Try different selectors
        result_items.extend(soup.find_all("div", class_=lambda x: x and "SearchResult" in str(x)))
        result_items.extend(soup.find_all("div", class_=lambda x: x and "Card" in str(x)))
        result_items.extend(soup.find_all("div", class_="Card-titleContainer"))
        
        # Also try finding all article links
        if not result_items:
            articles = soup.find_all("a", href=lambda x: x and "/2" in str(x) and ".html" in str(x))
            result_items = articles[:20]  # Limit
        
        logger.debug(f"Found {len(result_items)} search result items")
        
        for item in result_items:
            try:
                # Get link
                if item.name == "a":
                    url = item.get("href")
                    link_tag = item
                else:
                    link_tag = item.find("a", href=True)
                    if not link_tag:
                        continue
                    url = link_tag.get("href")
                
                if not url:
                    continue
                
                # Make absolute URL
                if url.startswith("/"):
                    url = self.base_url + url
                elif not url.startswith("http"):
                    continue
                
                # Skip non-article URLs
                if not any(x in url for x in ["/2024/", "/2025/", "/2026/"]):
                    continue
                
                # Get title
                title_tag = item.find(["h3", "h2", "a"])
                if not title_tag:
                    title = link_tag.get_text(strip=True) if link_tag else ""
                else:
                    title = title_tag.get_text(strip=True)
                
                if not title or len(title) < 10:
                    continue
                
                # Try to extract date
                published_at = datetime.now()
                date_tag = item.find("time")
                if date_tag:
                    date_str = date_tag.get("datetime")
                    if date_str:
                        try:
                            published_at = datetime.fromisoformat(
                                date_str.replace('Z', '+00:00')
                            )
                        except:
                            pass
                
                results.append(
                    ArticleMeta(
                        url=url,
                        title=title,
                        published_at=published_at,
                        source="cnbc"
                    )
                )
                
            except Exception as e:
                logger.debug(f"Error parsing search item: {e}")
                continue
        
        logger.debug(f"Search page | found {len(results)} articles")
        return results
    
    def _search_section(self, section: str, company_name: str) -> List[ArticleMeta]:
        """Search within a section"""
        section_url = f"{self.base_url}/{section}/"
        
        logger.debug(f"Fetching section: {section_url}")
        
        html = self.http.get(section_url)
        if not html:
            logger.warning(f"Failed to fetch section: {section_url}")
            return []
        
        soup = BeautifulSoup(html, "html.parser")
        results: List[ArticleMeta] = []
        
        company_lower = company_name.lower()
        
        # Find article cards
        cards = soup.find_all("div", class_=lambda x: x and "Card" in str(x))
        
        if not cards:
            # Try alternative: all article links
            cards = soup.find_all("a", href=lambda x: x and ".html" in str(x))[:30]
        
        for card in cards:
            try:
                # Find link
                if card.name == "a":
                    url = card.get("href")
                    link_tag = card
                else:
                    link_tag = card.find("a", href=True)
                    if not link_tag:
                        continue
                    url = link_tag.get("href")
                
                if not url:
                    continue
                
                # Make absolute URL
                if url.startswith("/"):
                    url = self.base_url + url
                elif not url.startswith("http"):
                    continue
                
                # Get title
                if card.name == "a":
                    title = card.get_text(strip=True)
                else:
                    title_tag = card.find(["h2", "h3", "h4"])
                    if not title_tag:
                        title_tag = link_tag
                    title = title_tag.get_text(strip=True)
                
                # Check if company name is mentioned
                if company_lower not in title.lower():
                    continue
                
                # Try to extract date
                published_at = datetime.now()
                date_tag = card.find("time")
                if date_tag:
                    date_str = date_tag.get("datetime")
                    if date_str:
                        try:
                            published_at = datetime.fromisoformat(
                                date_str.replace('Z', '+00:00')
                            )
                        except:
                            pass
                
                results.append(
                    ArticleMeta(
                        url=url,
                        title=title,
                        published_at=published_at,
                        source="cnbc"
                    )
                )
                
            except Exception as e:
                logger.debug(f"Error parsing card: {e}")
                continue
        
        logger.debug(f"Section {section} | found {len(results)} articles")
        return results
    
    def _deduplicate(self, articles: List[ArticleMeta]) -> List[ArticleMeta]:
        """Remove duplicate articles by URL"""
        seen_urls = set()
        unique = []
        
        for article in articles:
            if article.url not in seen_urls:
                seen_urls.add(article.url)
                unique.append(article)
        
        return unique
