# src/common/rss_parser.py

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Optional, Dict
from dataclasses import dataclass

from common.logger import get_logger
from common.http import HttpClient

logger = get_logger(__name__)


@dataclass
class RSSItem:
    """RSS feed item"""
    title: str
    link: str
    description: Optional[str] = None
    published_at: Optional[datetime] = None
    author: Optional[str] = None
    category: Optional[str] = None


class RSSParser:
    """Generic RSS/Atom feed parser"""
    
    def __init__(self, http_client: HttpClient):
        self.http = http_client
    
    def parse_feed(self, feed_url: str) -> List[RSSItem]:
        """Parse RSS/Atom feed and return items"""
        logger.info(f"Fetching RSS feed: {feed_url}")
        
        xml_content = self.http.get(feed_url)
        if not xml_content:
            logger.warning(f"Failed to fetch feed: {feed_url}")
            return []
        
        try:
            root = ET.fromstring(xml_content)
            
            # Detect feed type (RSS 2.0 or Atom)
            if root.tag == 'rss':
                return self._parse_rss(root)
            elif root.tag.endswith('feed'):  # Atom feed
                return self._parse_atom(root)
            else:
                logger.warning(f"Unknown feed type: {root.tag}")
                return []
                
        except ET.ParseError as e:
            logger.error(f"XML parse error: {e}")
            return []
    
    def _parse_rss(self, root: ET.Element) -> List[RSSItem]:
        """Parse RSS 2.0 feed"""
        items = []
        
        for item in root.findall('.//item'):
            try:
                title = self._get_text(item, 'title')
                link = self._get_text(item, 'link')
                
                if not title or not link:
                    continue
                
                description = self._get_text(item, 'description')
                pub_date_str = self._get_text(item, 'pubDate')
                author = self._get_text(item, 'author') or self._get_text(item, '{http://purl.org/dc/elements/1.1/}creator')
                category = self._get_text(item, 'category')
                
                # Parse date
                pub_date = None
                if pub_date_str:
                    pub_date = self._parse_rss_date(pub_date_str)
                
                items.append(RSSItem(
                    title=title,
                    link=link,
                    description=description,
                    published_at=pub_date,
                    author=author,
                    category=category
                ))
                
            except Exception as e:
                logger.debug(f"Error parsing RSS item: {e}")
                continue
        
        logger.info(f"Parsed {len(items)} items from RSS feed")
        return items
    
    def _parse_atom(self, root: ET.Element) -> List[RSSItem]:
        """Parse Atom feed"""
        items = []
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        
        for entry in root.findall('atom:entry', ns):
            try:
                title = self._get_text(entry, 'atom:title', ns)
                
                # Get link
                link_elem = entry.find('atom:link[@rel="alternate"]', ns)
                if link_elem is None:
                    link_elem = entry.find('atom:link', ns)
                link = link_elem.get('href') if link_elem is not None else None
                
                if not title or not link:
                    continue
                
                # Get other fields
                summary = self._get_text(entry, 'atom:summary', ns) or self._get_text(entry, 'atom:content', ns)
                
                # Author
                author_elem = entry.find('atom:author/atom:name', ns)
                author = author_elem.text if author_elem is not None else None
                
                # Category
                category_elem = entry.find('atom:category', ns)
                category = category_elem.get('term') if category_elem is not None else None
                
                # Date
                pub_date_str = self._get_text(entry, 'atom:published', ns) or self._get_text(entry, 'atom:updated', ns)
                pub_date = None
                if pub_date_str:
                    pub_date = self._parse_iso_date(pub_date_str)
                
                items.append(RSSItem(
                    title=title,
                    link=link,
                    description=summary,
                    published_at=pub_date,
                    author=author,
                    category=category
                ))
                
            except Exception as e:
                logger.debug(f"Error parsing Atom entry: {e}")
                continue
        
        logger.info(f"Parsed {len(items)} items from Atom feed")
        return items
    
    def _get_text(self, element: ET.Element, tag: str, namespaces: Optional[Dict] = None) -> Optional[str]:
        """Safely get text from XML element"""
        child = element.find(tag, namespaces or {})
        if child is not None and child.text:
            return child.text.strip()
        return None
    
    def _parse_rss_date(self, date_str: str) -> Optional[datetime]:
        """Parse RSS date (RFC 822 format)"""
        try:
            # Try common RSS date formats
            formats = [
                '%a, %d %b %Y %H:%M:%S %z',
                '%a, %d %b %Y %H:%M:%S %Z',
                '%a, %d %b %Y %H:%M:%S GMT',
                '%d %b %Y %H:%M:%S %z',
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            
            logger.debug(f"Could not parse RSS date: {date_str}")
            return None
            
        except Exception as e:
            logger.debug(f"Date parse error: {e}")
            return None
    
    def _parse_iso_date(self, date_str: str) -> Optional[datetime]:
        """Parse ISO 8601 date"""
        try:
            # Handle timezone
            if date_str.endswith('Z'):
                date_str = date_str[:-1] + '+00:00'
            return datetime.fromisoformat(date_str)
        except Exception as e:
            logger.debug(f"ISO date parse error: {e}")
            return None
