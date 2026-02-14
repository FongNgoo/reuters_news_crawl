# src/main.py

import sys
from datetime import date
from pathlib import Path
from typing import List

import pandas as pd

from common.logger import get_logger
from common.config import load_yaml
from schema.news import NewsArticle
from pipelines.multi_source import MultiSourcePipeline

logger = get_logger(__name__)

# ✅ FIX: Tự động tìm thư mục gốc của project
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"

# Thêm project root vào sys.path để import các module
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def articles_to_dataframe(articles: List[NewsArticle]) -> pd.DataFrame:
    """Convert list of NewsArticle to DataFrame"""
    records = []

    for a in articles:
        records.append({
            "url": a.url,
            "title": a.title,
            "body_text": a.body_text,
            "published_at": a.published_at,
            "source": a.source,
            "company": a.company,
            "ticker": a.ticker,
            "sector": a.sector,
            "author": a.author,
            "section": a.section,
        })

    df = pd.DataFrame(records)
    df["published_at"] = pd.to_datetime(df["published_at"], utc=True)

    df["year"] = df["published_at"].dt.year
    df["month"] = df["published_at"].dt.month

    return df


def export_parquet(df: pd.DataFrame, output_path: Path):
    """Export DataFrame to Parquet with partitioning"""
    df = (
        df.sort_values(by=["published_at", "url"])
          .drop_duplicates(subset=["url"])
    )

    # to_parquet with partition_cols will automatically create the folder structure
    df.to_parquet(
        output_path,
        engine="pyarrow",
        partition_cols=["year", "month", "source"],  # ✅ Added source partition
        index=False
    )


def main():
    logger.info("="*60)
    logger.info("MULTI-SOURCE NEWS CRAWLER")
    logger.info("="*60)
    
    logger.info("Loading configuration...")
    logger.info(f"Project root: {PROJECT_ROOT}")
    logger.info(f"Config directory: {CONFIG_DIR}")

    # ✅ FIX: Sử dụng đường dẫn tuyệt đối
    crawler_cfg = load_yaml(CONFIG_DIR / "crawler.yaml")
    tickers_cfg = load_yaml(CONFIG_DIR / "tickers.yaml")
    sources_cfg = load_yaml(CONFIG_DIR / "sources.yaml")

    start_date = date.fromisoformat(
        crawler_cfg["crawler"]["start_date"]
    )
    end_date = date.fromisoformat(
        crawler_cfg["crawler"]["end_date"]
    )

    tickers = tickers_cfg["tickers"]
    
    # ✅ FIX: Chuyển output path thành absolute path
    output_path_str = crawler_cfg["crawler"]["output"]["path"]
    output_path = PROJECT_ROOT / output_path_str

    logger.info(
        f"Config loaded | tickers={len(tickers)} "
        f"range={start_date} → {end_date}"
    )
    logger.info(f"Output path: {output_path}")

    # Show enabled sources
    enabled_sources = [
        name for name, cfg in sources_cfg.items()
        if cfg.get("enabled", False)
    ]
    logger.info(f"Enabled sources: {', '.join(enabled_sources)}")

    # ✅ NEW: Use MultiSourcePipeline
    pipeline = MultiSourcePipeline(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        sources_config=sources_cfg
    )

    articles = pipeline.run()

    if not articles:
        logger.warning("No articles collected")
        return

    logger.info(f"\nConverting {len(articles)} articles to DataFrame...")
    df = articles_to_dataframe(articles)

    logger.info("Exporting to Parquet...")
    export_parquet(df, output_path)

    logger.info(f"\n{'='*60}")
    logger.info("✓ CRAWL COMPLETED SUCCESSFULLY")
    logger.info(f"{'='*60}")
    logger.info(f"Total articles: {len(articles)}")
    logger.info(f"Data saved to: {output_path}")
    
    # Show breakdown by source
    logger.info("\nBreakdown by source:")
    for source in df['source'].unique():
        count = len(df[df['source'] == source])
        logger.info(f"  {source:15s}: {count:4d} articles")


if __name__ == "__main__":
    main()
