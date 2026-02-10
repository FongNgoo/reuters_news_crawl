# src/sources/reuters/rss_index.py

from typing import Iterable
from datetime import datetime
import feedparser

from schema.news import ArticleMeta
from common.logger import get_logger

logger = get_logger(__name__)

REUTERS_RSS_FEEDS = [
    "https://www.reuters.com/rssFeed/worldNews",
    "https://www.reuters.com/rssFeed/businessNews",
    "https://www.reuters.com/rssFeed/technologyNews",
    "https://www.reuters.com/rssFeed/marketsNews",
]


class ReutersRSSIndex:
    def iter_articles(self) -> Iterable[ArticleMeta]:
        for feed_url in REUTERS_RSS_FEEDS:
            logger.info(f"Load RSS | {feed_url}")
            feed = feedparser.parse(feed_url)

            for entry in feed.entries:
                if not hasattr(entry, "link"):
                    continue

                published = self._parse_date(entry)

                if not published:
                    continue

                yield ArticleMeta(
                    url=entry.link,
                    title=entry.title,
                    published_at=published,
                )

    def _parse_date(self, entry) -> datetime | None:
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            return datetime(*entry.published_parsed[:6])
        return None
