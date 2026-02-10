from datetime import date, datetime  # ✅ Fixed: Import datetime class
from typing import List, Tuple

from common.http import HttpClient
from common.logger import get_logger
from common.utils import split_date_range  # ✅ Fixed: Import split_date_range function
from schema.news import ArticleMeta
from bs4 import BeautifulSoup

logger = get_logger(__name__)


class ReutersSearcher:
    def __init__(self, http_client: HttpClient):
        self.http = http_client

    def search(
        self,
        company_name: str,
        start_date: date,
        end_date: date
    ) -> List[ArticleMeta]:

        logger.info(
            f"Searching Reuters | company={company_name} "
            f"range={start_date} → {end_date}"
        )

        windows = split_date_range(start_date, end_date)  # ✅ Fixed: Use imported function

        all_results: List[ArticleMeta] = []

        for window in windows:
            results = self._search_single_window(company_name, window)
            all_results.extend(results)

        unique_results = self._deduplicate(all_results)

        logger.info(
            f"Search completed | total={len(all_results)} "
            f"unique={len(unique_results)}"
        )

        return unique_results
    
    def _search_single_window(
        self,
        company_name: str,
        window: Tuple[date, date]
    ) -> List[ArticleMeta]:

        start_date, end_date = window

        search_url = (
            "https://www.reuters.com/site-search/"
            f"?query={company_name}"
            f"&dateRange=custom"
            f"&startDate={start_date.isoformat()}"
            f"&endDate={end_date.isoformat()}"
        )

        logger.debug(f"Fetching search URL | {search_url}")

        html = self.http.get(search_url)
        if not html:
            logger.warning(f"Empty search result | {search_url}")
            return []

        soup = BeautifulSoup(html, "html.parser")

        results: List[ArticleMeta] = []

        items = soup.find_all("li", class_="search-result-indiv")

        for item in items:
            link = item.find("a", href=True)
            title_tag = item.find("h3")
            time_tag = item.find("time")

            if not link or not title_tag or not time_tag:
                continue

            url = link["href"]
            if url.startswith("/"):
                url = "https://www.reuters.com" + url

            title = title_tag.get_text(strip=True)

            published_raw = time_tag.get("datetime")
            try:
                # ✅ Fixed: Use datetime class and keep as datetime object
                published_at = datetime.fromisoformat(
                    published_raw.replace("Z", "+00:00")
                )
            except Exception:
                continue

            results.append(
                ArticleMeta(
                    url=url,
                    title=title,
                    published_at=published_at,  # ✅ Fixed: Now datetime, not date
                    source="reuters"
                )
            )

        logger.debug(
            f"Window result | company={company_name} "
            f"{start_date}→{end_date} count={len(results)}"
        )

        return results

    def _deduplicate(
        self,
        articles: List[ArticleMeta]
    ) -> List[ArticleMeta]:

        seen_urls = set()
        unique_articles = []

        for article in articles:
            if article.url in seen_urls:
                continue
            seen_urls.add(article.url)
            unique_articles.append(article)

        return unique_articles
