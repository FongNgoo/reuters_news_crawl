from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

@dataclass
class ArticleMeta:
    url: str
    title: str
    published_at: datetime
    source: str

    search_query: Optional[str] = None
    window_start: Optional[date] = None
    window_end: Optional[date] = None

@dataclass
class ArticleContent:
    url: str
    body_text: str
    author: Optional[str] = None
    section: Optional[str] = None
    language: Optional[str] = "en"
    raw_html: Optional[str] = None

@dataclass
class NewsArticle:
    url: str
    title: str
    body_text: str
    published_at: datetime
    source: str

    company: Optional[str] = None
    ticker: Optional[str] = None
    sector: Optional[str] = None

    author: Optional[str] = None
    section: Optional[str] = None
    language: str = "en"
