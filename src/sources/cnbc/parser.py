# src/sources/cnbc/parser.py

from typing import Optional
from bs4 import BeautifulSoup

from common.http import HttpClient
from common.logger import get_logger
from schema.news import ArticleContent

logger = get_logger(__name__)


class CNBCParser:
    """Parse CNBC article content"""
    
    def __init__(self, http_client: HttpClient):
        self.http = http_client
    
    def parse(self, url: str) -> Optional[ArticleContent]:
        """Parse CNBC article"""
        html = self.http.get(url)
        
        if not html:
            logger.warning(f"Failed to fetch article | url={url}")
            return None
        
        soup = BeautifulSoup(html, "html.parser")
        
        # Remove unwanted elements
        for element in soup.find_all(["script", "style", "video", "aside"]):
            element.decompose()
        
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
        """Extract article body from CNBC HTML"""
        
        # Try main article body
        container = soup.find("div", class_=lambda x: x and "ArticleBody" in str(x))
        
        if not container:
            # Try alternative selectors
            container = soup.find("div", {"itemprop": "articleBody"})
        
        if not container:
            container = soup.find("article")
        
        if not container:
            return None
        
        # Find all paragraphs
        paragraphs = container.find_all("p")
        
        texts = []
        for p in paragraphs:
            text = p.get_text(strip=True)
            
            if not text:
                continue
            
            # Skip boilerplate
            if text.startswith("WATCH LIVE"):
                continue
            if "Subscribe to CNBC" in text:
                continue
            
            texts.append(text)
        
        # Also try to get text from div groups (CNBC sometimes uses divs)
        if not texts:
            divs = container.find_all("div", class_=lambda x: x and "group" in str(x).lower())
            for div in divs:
                text = div.get_text(strip=True)
                if text and len(text) > 50:  # Only substantial text
                    texts.append(text)
        
        return "\n".join(texts) if texts else None
    
    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract author from CNBC article"""
        
        # Try author link
        author_tag = soup.find("a", {"rel": "author"})
        if author_tag:
            return author_tag.get_text(strip=True)
        
        # Try byline
        author_tag = soup.find("div", class_=lambda x: x and "Author-" in str(x))
        if author_tag:
            name_tag = author_tag.find("a")
            if name_tag:
                return name_tag.get_text(strip=True)
        
        # Try meta tag
        meta = soup.find("meta", {"name": "author"})
        if meta and meta.get("content"):
            return meta.get("content")
        
        return None
    
    def _extract_section(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract section/category from CNBC article"""
        
        # Try meta tag
        meta = soup.find("meta", {"property": "article:section"})
        if meta and meta.get("content"):
            return meta.get("content")
        
        # Try breadcrumb
        breadcrumb = soup.find("div", class_=lambda x: x and "breadcrumb" in str(x).lower())
        if breadcrumb:
            links = breadcrumb.find_all("a")
            if links:
                return links[0].get_text(strip=True)
        
        return None
