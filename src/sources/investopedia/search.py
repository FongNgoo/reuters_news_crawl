# src/sources/investopedia/search.py

from datetime import date, datetime
from typing import List
from bs4 import BeautifulSoup

from common.http import HttpClient
from common.logger import get_logger
from schema.news import ArticleMeta

logger = get_logger(__name__)


class InvestopediaSearcher:
    """Search Investopedia articles"""
    
    def __init__(self, http_client: HttpClient, categories: List[str]):
        self.http = http_client
        # ✅ FIX: Update to correct URL patterns
        self.categories = categories or ["news"]
        self.base_url = "https://www.investopedia.com"
    
    def search(
        self,
        company_name: str,
        start_date: date,
        end_date: date
    ) -> List[ArticleMeta]:
        """
        Search Investopedia articles
        
        Uses Investopedia's site search functionality
        """
        logger.info(
            f"Searching Investopedia | company={company_name}"
        )
        
        # ✅ FIX: Use search URL instead of category pages
        all_results = self._search_site(company_name)
        
        # Deduplicate
        unique_results = self._deduplicate(all_results)
        
        logger.info(
            f"Investopedia search completed | "
            f"total={len(all_results)} unique={len(unique_results)}"
        )
        
        return unique_results
    
    def _search_site(self, company_name: str) -> List[ArticleMeta]:
        """Use Investopedia's search function"""
        # ✅ FIX: Use proper search URL
        search_url = f"{self.base_url}/search?q={company_name.replace(' ', '+')}"
        
        logger.debug(f"Fetching search: {search_url}")
        
        html = self.http.get(search_url)
        if not html:
            logger.warning(f"Failed to fetch search: {search_url}")
            return []
        
        soup = BeautifulSoup(html, "html.parser")
        results: List[ArticleMeta] = []
        
        # ✅ FIX: Updated selectors for Investopedia search results
        # Try multiple selectors
        search_results = []
        
        # Try main search results
        search_results.extend(soup.find_all("div", class_="comp mntl-card-list-items"))
        search_results.extend(soup.find_all("div", class_="comp card"))
        search_results.extend(soup.find_all("a", class_="mntl-card-list-items"))
        
        if not search_results:
            # Fallback: find all links with article patterns
            all_links = soup.find_all("a", href=True)
            search_results = [
                link for link in all_links 
                if "/news/" in link.get("href", "") or "/articles/" in link.get("href", "")
            ]
        
        logger.debug(f"Found {len(search_results)} potential results")
        
        for item in search_results[:20]:  # Limit to first 20
            try:
                # Get URL
                if item.name == "a":
                    url = item.get("href")
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
                
                # Get title
                if item.name == "a":
                    title = item.get_text(strip=True)
                else:
                    title_tag = item.find(["h2", "h3", "h4", "span"])
                    if not title_tag:
                        continue
                    title = title_tag.get_text(strip=True)
                
                if not title or len(title) < 10:
                    continue
                
                # Get date if available
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
                        source="investopedia"
                    )
                )
                
            except Exception as e:
                logger.debug(f"Error parsing item: {e}")
                continue
        
        logger.debug(f"Parsed {len(results)} articles")
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
