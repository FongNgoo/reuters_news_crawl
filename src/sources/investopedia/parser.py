# src/sources/investopedia/parser.py

from typing import Optional
from bs4 import BeautifulSoup

from common.http import HttpClient
from common.logger import get_logger
from schema.news import ArticleContent

logger = get_logger(__name__)


class InvestopediaParser:
    """Parse Investopedia article content"""
    
    def __init__(self, http_client: HttpClient):
        self.http = http_client
    
    def parse(self, url: str) -> Optional[ArticleContent]:
        """Parse Investopedia article"""
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
        """Extract article body from Investopedia HTML"""
        
        # Try main article container
        container = soup.find("div", {"id": "article-body_1-0"})
        
        if not container:
            # Try alternative selectors
            container = soup.find("article")
        
        if not container:
            container = soup.find("div", class_=lambda x: x and "article-content" in str(x).lower())
        
        if not container:
            return None
        
        # Find all paragraphs
        paragraphs = container.find_all("p")
        
        texts = []
        for p in paragraphs:
            # Skip elements that are not main content
            if p.find_parent("aside") or p.find_parent("footer"):
                continue
            
            text = p.get_text(strip=True)
            
            if not text:
                continue
            
            # Skip common boilerplate
            if text.startswith("Article Sources"):
                continue
            if "Investopedia requires writers" in text:
                continue
            
            texts.append(text)
        
        return "\n".join(texts) if texts else None
    
    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract author from Investopedia article"""
        
        # Try multiple selectors
        author_tag = soup.find("a", {"rel": "author"})
        if author_tag:
            return author_tag.get_text(strip=True)
        
        # Try byline div
        author_tag = soup.find("div", class_=lambda x: x and "byline" in str(x).lower())
        if author_tag:
            return author_tag.get_text(strip=True)
        
        # Try meta tag
        meta = soup.find("meta", {"name": "author"})
        if meta and meta.get("content"):
            return meta.get("content")
        
        return None
    
    def _extract_section(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract section/category from Investopedia article"""
        
        # Try breadcrumb
        breadcrumb = soup.find("nav", {"aria-label": "Breadcrumb"})
        if breadcrumb:
            links = breadcrumb.find_all("a")
            if len(links) > 1:
                # Return second item (first is usually "Home")
                return links[1].get_text(strip=True)
        
        # Try meta tag
        meta = soup.find("meta", {"property": "article:section"})
        if meta and meta.get("content"):
            return meta.get("content")
        
        return None
