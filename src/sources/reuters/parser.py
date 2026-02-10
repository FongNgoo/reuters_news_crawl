from typing import Optional
from bs4 import BeautifulSoup

from common.http import HttpClient
from common.logger import get_logger
from schema.news import ArticleContent

logger = get_logger(__name__)

class ReutersParser:
    def __init__(self, http_client: HttpClient):  # ✅ Fixed: Added space after comma
        self.http = http_client

    def parse(self, url: str) -> Optional[ArticleContent]:
        html = self.http.get(url)

        if not html:
            logger.warning(f"Fail to fetch article | url={url}")
            return None
        
        soup = BeautifulSoup(html, "html.parser")

        body_text = self._extract_body(soup)
        if not body_text:
            logger.warning(f"No body found | url={url}")
            return None
        
        author = self._extract_author(soup)
        section = self._extract_section(soup)
        
        return ArticleContent(
            url=url,
            body_text=body_text,
            author=author,
            section=section,
            raw_html=None
        )

    def _extract_body(self, soup: BeautifulSoup) -> Optional[str]:  # ✅ Fixed: Added space after comma
        container = soup.find("div", attrs={"data-testid": "article-body"})

        if not container:
            return None

        paragraphs = container.find_all("p")

        texts = []
        for p in paragraphs:
            text = p.get_text(strip=True)

            if not text:
                continue

            if text.lower().startswith("reporting by"):
                continue

            texts.append(text)

        return "\n".join(texts) if texts else None

    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:  # ✅ Fixed: Added space after comma
        author_tag = soup.find("span", attrs={"data-testid": "AuthorName"})
        if author_tag:
            return author_tag.get_text(strip=True)
        return None

    def _extract_section(self, soup: BeautifulSoup) -> Optional[str]:  # ✅ Fixed: Added space after comma
        section_tag = soup.find("a", attrs={"data-testid": "section-link"})
        if section_tag:
            return section_tag.get_text(strip=True)
        return None
