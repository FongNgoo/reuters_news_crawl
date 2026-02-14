# src/sources/guardian/parser.py

from typing import Optional
from bs4 import BeautifulSoup

from common.http import HttpClient
from common.logger import get_logger
from schema.news import ArticleContent

logger = get_logger(__name__)


class GuardianParser:
    """Parse Guardian article content"""
    
    def __init__(self, http_client: HttpClient):
        self.http = http_client
    
    def parse(self, url: str) -> Optional[ArticleContent]:
        """Parse Guardian article"""
        html = self.http.get(url)
        
        if not html:
            logger.warning(f"Failed to fetch article | url={url}")
            return None
        
        soup = BeautifulSoup(html, "html.parser")
        
        body_text = self._extract_body(soup)
        if not body_text:
            logger.warning(f"No body found | url={url}")
            return None
        
        author = self._extract_author(soup)
        section = self._extract_section(soup)
        
        return ArticleContent(
            url=url,
            body_text=body_text,
            author=author,
            section=section,
            raw_html=None
        )
    
    def _extract_body(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article body from Guardian HTML"""
        
        # Try main content container
        container = soup.find("div", {"id": "maincontent"})
        
        if not container:
            # Try alternative selector
            container = soup.find("article")
        
        if not container:
            return None
        
        # Find all paragraphs in the article body
        paragraphs = container.find_all("p", class_=lambda x: x != "dcr-1eu361v")
        
        if not paragraphs:
            # Fallback: get all <p> tags
            paragraphs = container.find_all("p")
        
        texts = []
        for p in paragraphs:
            text = p.get_text(strip=True)
            
            if not text:
                continue
            
            # Skip common footer text
            if text.startswith("Topics"):
                continue
            if text.startswith("Reuse this content"):
                continue
            
            texts.append(text)
        
        return "\n".join(texts) if texts else None
    
    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract author from Guardian article"""
        
        # Try meta tag
        meta = soup.find("meta", {"name": "author"})
        if meta and meta.get("content"):
            return meta.get("content")
        
        # Try byline
        byline = soup.find("a", {"rel": "author"})
        if byline:
            return byline.get_text(strip=True)
        
        # Try alternative byline
        byline = soup.find("span", {"itemprop": "name"})
        if byline:
            return byline.get_text(strip=True)
        
        return None
    
    def _extract_section(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract section/category from Guardian article"""
        
        # Try breadcrumb
        breadcrumb = soup.find("nav", {"aria-label": "Breadcrumb"})
        if breadcrumb:
            links = breadcrumb.find_all("a")
            if links:
                # Return the first breadcrumb item (main section)
                return links[0].get_text(strip=True)
        
        # Try meta tag
        meta = soup.find("meta", {"property": "article:section"})
        if meta and meta.get("content"):
            return meta.get("content")
        
        return None
