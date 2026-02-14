# src/sources/investopedia/scraper.py

from typing import List

from schema.news import NewsArticle
from common.logger import get_logger
from sources.investopedia.search import InvestopediaSearcher
from sources.investopedia.parser import InvestopediaParser

logger = get_logger(__name__)


class InvestopediaScraper:
    """Investopedia scraper orchestrator"""
    
    def __init__(self, searcher: InvestopediaSearcher, parser: InvestopediaParser):
        self.searcher = searcher
        self.parser = parser
    
    def crawl(
        self,
        company_name: str,
        ticker: str,
        sector: str,
        start_date,
        end_date
    ) -> List[NewsArticle]:
        """Crawl Investopedia articles for a company"""
        
        # Search for articles
        metas = self.searcher.search(company_name, start_date, end_date)
        
        articles = []
        seen_urls = set()
        
        for meta in metas:
            if meta.url in seen_urls:
                continue
            seen_urls.add(meta.url)
            
            # Parse article content
            content = self.parser.parse(meta.url)
            if not content:
                logger.debug(f"Skip article | url={meta.url}")
                continue
            
            articles.append(
                NewsArticle(
                    url=meta.url,
                    title=meta.title,
                    body_text=content.body_text,
                    published_at=meta.published_at,
                    source="investopedia",
                    company=company_name,
                    ticker=ticker,
                    sector=sector,
                    author=content.author,
                    section=content.section
                )
            )
        
        logger.info(
            f"Investopedia crawl done | company={company_name} "
            f"articles={len(articles)}"
        )
        
        return articles
