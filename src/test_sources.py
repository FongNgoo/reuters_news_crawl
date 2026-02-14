#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Improved Test Script - Test sources with better reporting
"""

import sys
from pathlib import Path
from datetime import date, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from common.http import HttpClient
from common.logger import get_logger

# Import source modules
from sources.guardian.search import GuardianSearcher
from sources.guardian.parser import GuardianParser

from sources.investopedia.search import InvestopediaSearcher
from sources.investopedia.parser import InvestopediaParser

from sources.cnbc.search import CNBCSearcher
from sources.cnbc.parser import CNBCParser

logger = get_logger(__name__)


def test_guardian():
    """Test Guardian crawler"""
    print("\n" + "="*60)
    print("TESTING GUARDIAN (RSS)")
    print("="*60)
    
    http = HttpClient(sleep_between=1.0)
    
    # RSS feeds (including tech)
    rss_feeds = [
        "https://www.theguardian.com/business/rss",
        "https://www.theguardian.com/world/rss",
        "https://www.theguardian.com/technology/rss"
    ]
    
    searcher = GuardianSearcher(http, rss_feeds)
    parser = GuardianParser(http)
    
    # Search for Apple with broad date range
    company = "Apple"
    end_date = date.today()
    start_date = end_date - timedelta(days=60)  # Wider range
    
    print(f"Searching for: {company}")
    print(f"Date range: {start_date} to {end_date}")
    
    metas = searcher.search(company, start_date, end_date)
    
    print(f"\n✓ Found {len(metas)} articles")
    
    if metas:
        print(f"\nSample articles:")
        for i, article in enumerate(metas[:3], 1):
            print(f"  {i}. {article.title[:70]}...")
        
        print(f"\nTesting parser with first article...")
        article = metas[0]
        print(f"URL: {article.url}")
        
        content = parser.parse(article.url)
        if content:
            print(f"✓ Parse successful")
            print(f"Body length: {len(content.body_text)} chars")
            print(f"Author: {content.author or 'N/A'}")
            print(f"Section: {content.section or 'N/A'}")
        else:
            print(f"✗ Parse failed")
    else:
        print("\n⚠️  No articles found. This might be normal if:")
        print("  - Company name not in recent RSS feed")
        print("  - Date range too restrictive")
    
    return len(metas) > 0


def test_investopedia():
    """Test Investopedia crawler"""
    print("\n" + "="*60)
    print("TESTING INVESTOPEDIA (HTML - Site Search)")
    print("="*60)
    
    http = HttpClient(sleep_between=0.5)
    
    categories = ["news"]
    
    searcher = InvestopediaSearcher(http, categories)
    parser = InvestopediaParser(http)
    
    company = "Apple"
    end_date = date.today()
    start_date = end_date - timedelta(days=30)
    
    print(f"Searching for: {company}")
    print("Using Investopedia site search...")
    
    metas = searcher.search(company, start_date, end_date)
    
    print(f"\n✓ Found {len(metas)} articles")
    
    if metas:
        print(f"\nSample articles:")
        for i, article in enumerate(metas[:3], 1):
            print(f"  {i}. {article.title[:70]}...")
        
        print(f"\nTesting parser with first article...")
        article = metas[0]
        print(f"URL: {article.url}")
        
        content = parser.parse(article.url)
        if content:
            print(f"✓ Parse successful")
            print(f"Body length: {len(content.body_text)} chars")
            print(f"Author: {content.author or 'N/A'}")
        else:
            print(f"✗ Parse failed")
    else:
        print("\n⚠️  No articles found. This might be normal if:")
        print("  - Company not featured in recent Investopedia content")
        print("  - Search results limited")
    
    return len(metas) > 0


def test_cnbc():
    """Test CNBC crawler"""
    print("\n" + "="*60)
    print("TESTING CNBC (HTML - Site Search)")
    print("="*60)
    
    http = HttpClient(sleep_between=1.5)
    
    sections = ["markets", "investing", "technology"]
    
    searcher = CNBCSearcher(http, sections)
    parser = CNBCParser(http)
    
    company = "Apple"
    end_date = date.today()
    start_date = end_date - timedelta(days=30)
    
    print(f"Searching for: {company}")
    print("Using CNBC site search + sections...")
    
    metas = searcher.search(company, start_date, end_date)
    
    print(f"\n✓ Found {len(metas)} articles")
    
    if metas:
        print(f"\nSample articles:")
        for i, article in enumerate(metas[:3], 1):
            print(f"  {i}. {article.title[:70]}...")
        
        print(f"\nTesting parser with first article...")
        article = metas[0]
        print(f"URL: {article.url}")
        
        content = parser.parse(article.url)
        if content:
            print(f"✓ Parse successful")
            print(f"Body length: {len(content.body_text)} chars")
            print(f"Author: {content.author or 'N/A'}")
        else:
            print(f"✗ Parse failed")
    else:
        print("\n⚠️  No articles found. This might be normal if:")
        print("  - CNBC search returned no results")
        print("  - HTML structure changed")
    
    return len(metas) > 0


def main():
    print("\n" + "="*60)
    print("MULTI-SOURCE CRAWLER - IMPROVED TEST")
    print("="*60)
    print("\nTesting each source independently...")
    print("Company: Apple")
    print("\nNote: Results depend on current news cycle.")
    print("It's normal if some sources find 0 articles.")
    
    results = {}
    details = {}
    
    # Test Guardian
    print("\n" + "-"*60)
    try:
        results['Guardian'] = test_guardian()
        details['Guardian'] = "RSS-based, reliable"
    except Exception as e:
        logger.exception(f"Guardian test error: {e}")
        results['Guardian'] = False
        details['Guardian'] = f"Error: {str(e)[:50]}"
    
    # Test Investopedia
    print("\n" + "-"*60)
    try:
        results['Investopedia'] = test_investopedia()
        details['Investopedia'] = "Site search"
    except Exception as e:
        logger.exception(f"Investopedia test error: {e}")
        results['Investopedia'] = False
        details['Investopedia'] = f"Error: {str(e)[:50]}"
    
    # Test CNBC
    print("\n" + "-"*60)
    try:
        results['CNBC'] = test_cnbc()
        details['CNBC'] = "Site search + sections"
    except Exception as e:
        logger.exception(f"CNBC test error: {e}")
        results['CNBC'] = False
        details['CNBC'] = f"Error: {str(e)[:50]}"
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    for source, success in results.items():
        status = "✓ FOUND ARTICLES" if success else "✗ NO ARTICLES"
        detail = details.get(source, "")
        print(f"{status:20s} - {source:15s} ({detail})")
    
    passed = sum(1 for s in results.values() if s)
    total = len(results)
    
    print(f"\n{'='*60}")
    print(f"Result: {passed}/{total} sources found articles")
    print(f"{'='*60}")
    
    if passed == total:
        print("\n🎉 All sources working! Ready to crawl.")
        return 0
    elif passed > 0:
        print(f"\n✓ {passed} source(s) working.")
        print("This is acceptable - not all sources may have recent")
        print("articles about every company.")
        return 0
    else:
        print("\n⚠️  No sources found articles.")
        print("Check:")
        print("  1. Internet connection")
        print("  2. Website accessibility")
        print("  3. Try different company (e.g., Tesla, Microsoft)")
        return 1


if __name__ == "__main__":
    sys.exit(main())
