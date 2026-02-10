# src/sources/reuters/article_index.py

from typing import Iterable, List
from datetime import datetime

from bs4 import BeautifulSoup

from common.http import HttpClient
from common.logger import get_logger
from schema.news import ArticleMeta
from sources.reuters.constants import REUTERS_SECTIONS

logger = get_logger(__name__)


class ReutersArticleIndex:
    def __init__(self, http_client: HttpClient):
        self.http = http_client

    def iter_articles(self) -> Iterable[ArticleMeta]:
        """
        Entry point:
        - iterate all Reuters sections
        - yield ArticleMeta (url, title, published_at)
        """
        for section_url in REUTERS_SECTIONS:
            logger.info(f"Indexing section | {section_url}")
            yield from self._crawl_section(section_url)

    # -------------------------
    # Internal
    # -------------------------

    def _crawl_section(self, section_url: str) -> Iterable[ArticleMeta]:
        """
        Crawl paginated section page
        """
        next_url = section_url

        while next_url:
            html = self.http.get(next_url)
            if not html:
                logger.warning(f"Fail to fetch section | {next_url}")
                break

            soup = BeautifulSoup(html, "html.parser")

            articles = self._extract_articles(soup)
            for article in articles:
                yield article

            next_url = self._extract_next_page(soup)

    def _extract_articles(self, soup: BeautifulSoup) -> List[ArticleMeta]:
        """
        Parse article cards from section page
        """
        results = []

        cards = soup.find_all("article")

        for card in cards:
            link = card.find("a", href=True)
            time_tag = card.find("time")

            if not link or not time_tag:
                continue

            url = self._normalize_url(link["href"])
            title = link.get_text(strip=True)

            published_at = self._parse_datetime(
                time_tag.get("datetime")
            )

            if not published_at:
                continue

            results.append(
                ArticleMeta(
                    url=url,
                    title=title,
                    published_at=published_at,
                )
            )

        return results

    def _extract_next_page(self, soup: BeautifulSoup) -> str | None:
        """
        Find pagination link if exists
        """
        next_link = soup.find("a", attrs={"aria-label": "Next"})
        if next_link and next_link.get("href"):
            return self._normalize_url(next_link["href"])
        return None

    @staticmethod
    def _normalize_url(href: str) -> str:
        if href.startswith("http"):
            return href
        return f"https://www.reuters.com{href}"

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
        except Exception:
            return None
