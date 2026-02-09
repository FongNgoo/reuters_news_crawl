# src/pipelines/historical.py

from datetime import date
from typing import List

from common.logger import get_logger
from common.http import HttpClient
from schema.news import NewsArticle
from sources.reuters.search import ReutersSearcher
from sources.reuters.parser import ReutersParser
from sources.reuters.scraper import ReutersScraper

logger = get_logger(__name__)


class HistoricalReutersPipeline:
    def __init__(
        self,
        tickers: List[dict],
        start_date: date,
        end_date: date,
    ):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date

        # Init shared components
        self.http = HttpClient()
        self.searcher = ReutersSearcher(self.http)
        self.parser = ReutersParser(self.http)
        self.scraper = ReutersScraper(
            searcher=self.searcher,
            parser=self.parser
        )

    def run(self) -> List[NewsArticle]:
        logger.info(
            f"Start historical Reuters pipeline | "
            f"companies={len(self.tickers)} "
            f"range={self.start_date} → {self.end_date}"
        )

        all_articles: List[NewsArticle] = []

        for idx, t in enumerate(self.tickers, 1):
            company = t["company"]
            ticker = t["ticker"]
            sector = t["sector"]

            logger.info(
                f"[{idx}/{len(self.tickers)}] "
                f"Crawling company={company} ({ticker})"
            )

            try:
                articles = self.scraper.crawl(
                    company_name=company,
                    ticker=ticker,
                    sector=sector,
                    start_date=self.start_date,
                    end_date=self.end_date
                )

                all_articles.extend(articles)

            except Exception as e:
                logger.exception(
                    f"Pipeline error | company={company} | err={e}"
                )
                continue

        logger.info(
            f"Historical pipeline finished | "
            f"total_articles={len(all_articles)}"
        )

        return all_articles
