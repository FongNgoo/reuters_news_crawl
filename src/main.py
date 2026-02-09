# src/main.py

from datetime import date
from pathlib import Path
from typing import List

import pandas as pd

from common.logger import get_logger
from common.config import load_yaml
from schema.news import NewsArticle
from pipelines.historical import HistoricalReutersPipeline

logger = get_logger(__name__)


def articles_to_dataframe(articles: List[NewsArticle]) -> pd.DataFrame:
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
    output_path.mkdir(parents=True, exist_ok=True)

    df = (
        df.sort_values(by=["published_at", "url"])
          .drop_duplicates(subset=["url"])
    )

    df.to_parquet(
        output_path,
        engine="pyarrow",
        partition_cols=["year", "month"],
        index=False
    )


def main():
    logger.info("Load configuration")

    crawler_cfg = load_yaml("config/crawler.yaml")
    tickers_cfg = load_yaml("config/tickers.yaml")

    start_date = date.fromisoformat(
        crawler_cfg["crawler"]["start_date"]
    )
    end_date = date.fromisoformat(
        crawler_cfg["crawler"]["end_date"]
    )

    tickers = tickers_cfg["tickers"]
    output_path = Path(
        crawler_cfg["crawler"]["output"]["path"]
    )

    logger.info(
        f"Config loaded | tickers={len(tickers)} "
        f"range={start_date} → {end_date}"
    )

    pipeline = HistoricalReutersPipeline(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date
    )

    articles = pipeline.run()

    if not articles:
        logger.warning("No articles collected")
        return

    df = articles_to_dataframe(articles)

    export_parquet(df, output_path)

    logger.info("Historical Reuters dataset build completed")


if __name__ == "__main__":
    main()
