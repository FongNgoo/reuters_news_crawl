# src/pipelines/multi_source.py

from datetime import date
from typing import List, Dict

from common.logger import get_logger
from common.http import HttpClient
from common.config import load_yaml
from schema.news import NewsArticle

# Import all source scrapers
from sources.guardian.search import GuardianSearcher
from sources.guardian.parser import GuardianParser
from sources.guardian.scraper import GuardianScraper

from sources.investopedia.search import InvestopediaSearcher
from sources.investopedia.parser import InvestopediaParser
from sources.investopedia.scraper import InvestopediaScraper

from sources.cnbc.search import CNBCSearcher
from sources.cnbc.parser import CNBCParser
from sources.cnbc.scraper import CNBCScraper

logger = get_logger(__name__)


class MultiSourcePipeline:
    """Pipeline to crawl news from multiple sources"""
    
    def __init__(
        self,
        tickers: List[dict],
        start_date: date,
        end_date: date,
        sources_config: Dict = None
    ):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        
        # Load sources config if not provided
        if sources_config is None:
            sources_config = load_yaml("config/sources.yaml")
        self.sources_config = sources_config
        
        # Initialize HTTP client
        self.http = HttpClient(sleep_between=1.5)
        
        # Initialize scrapers for enabled sources
        self.scrapers = {}
        self._init_scrapers()
    
    def _init_scrapers(self):
        """Initialize scrapers based on config"""
        
        # Guardian
        if self.sources_config.get("guardian", {}).get("enabled", False):
            logger.info("Initializing Guardian scraper...")
            rss_feeds = self.sources_config["guardian"]["rss"]
            searcher = GuardianSearcher(self.http, rss_feeds)
            parser = GuardianParser(self.http)
            self.scrapers["guardian"] = GuardianScraper(searcher, parser)
        
        # Investopedia
        if self.sources_config.get("investopedia", {}).get("enabled", False):
            logger.info("Initializing Investopedia scraper...")
            categories = self.sources_config["investopedia"]["categories"]
            searcher = InvestopediaSearcher(self.http, categories)
            parser = InvestopediaParser(self.http)
            self.scrapers["investopedia"] = InvestopediaScraper(searcher, parser)
        
        # CNBC
        if self.sources_config.get("cnbc", {}).get("enabled", False):
            logger.info("Initializing CNBC scraper...")
            sections = self.sources_config["cnbc"]["sections"]
            searcher = CNBCSearcher(self.http, sections)
            parser = CNBCParser(self.http)
            self.scrapers["cnbc"] = CNBCScraper(searcher, parser)
        
        logger.info(f"Initialized {len(self.scrapers)} source(s): {list(self.scrapers.keys())}")
    
    def run(self) -> List[NewsArticle]:
        """Run multi-source crawling pipeline"""
        logger.info(
            f"Start multi-source pipeline | "
            f"sources={len(self.scrapers)} "
            f"companies={len(self.tickers)} "
            f"range={self.start_date} → {self.end_date}"
        )
        
        if not self.scrapers:
            logger.warning("No enabled sources! Check config/sources.yaml")
            return []
        
        all_articles: List[NewsArticle] = []
        
        # Crawl each company
        for idx, t in enumerate(self.tickers, 1):
            company = t["company"]
            ticker = t["ticker"]
            sector = t["sector"]
            
            logger.info(
                f"\n{'='*60}\n"
                f"[{idx}/{len(self.tickers)}] Crawling: {company} ({ticker})\n"
                f"{'='*60}"
            )
            
            company_articles = []
            
            # Try each source
            for source_name, scraper in self.scrapers.items():
                logger.info(f"→ Source: {source_name}")
                
                try:
                    articles = scraper.crawl(
                        company_name=company,
                        ticker=ticker,
                        sector=sector,
                        start_date=self.start_date,
                        end_date=self.end_date
                    )
                    
                    company_articles.extend(articles)
                    logger.info(f"  ✓ {source_name}: {len(articles)} articles")
                    
                except Exception as e:
                    logger.exception(
                        f"  ✗ {source_name} error | company={company} | {e}"
                    )
                    continue
            
            all_articles.extend(company_articles)
            
            logger.info(
                f"Company total: {len(company_articles)} articles\n"
                f"Pipeline total: {len(all_articles)} articles"
            )
        
        # Summary by source
        logger.info(f"\n{'='*60}")
        logger.info("CRAWL SUMMARY")
        logger.info(f"{'='*60}")
        
        source_counts = {}
        for article in all_articles:
            source = article.source
            source_counts[source] = source_counts.get(source, 0) + 1
        
        for source, count in sorted(source_counts.items()):
            logger.info(f"  {source:15s}: {count:4d} articles")
        
        logger.info(f"  {'TOTAL':15s}: {len(all_articles):4d} articles")
        logger.info(f"{'='*60}\n")
        
        logger.info("Multi-source pipeline completed!")
        
        return all_articles
