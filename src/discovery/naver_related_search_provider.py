from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.exceptions import RequestException

from projects.keyword.src.discovery.base import BaseKeywordDiscoveryProvider


class NaverRelatedSearchProvider(BaseKeywordDiscoveryProvider):
    """
    Discover candidate keywords by parsing Naver search result HTML.
    """

    provider_name = "naver_related_search"
    source_name = "naver_related_search"
    SEARCH_URL = "https://search.naver.com/search.naver"

    # Keep selectors isolated so they are easy to tune later.
    RELATED_SECTION_SELECTORS = [
        "div.related_srch",
        "section.related_srch",
        "div.api_subject_bx",
        "div.api_group_option_filter",
        "div.api_group_inner",
    ]
    RELATED_LINK_SELECTORS = [
        "a[data-kgs*='related']",
        "a[data-kgs*='RC']",
        "a[href*='where=nexearch']",
        "a[href*='sm=tab_opt']",
        "a[href*='sm=tab_sug']",
    ]
    SECTION_HINT_TEXTS = [
        "\uc5f0\uad00\uac80\uc0c9\uc5b4",
        "\uad00\ub828\uac80\uc0c9\uc5b4",
        "\ud568\uaed8 \ucc3e\ub294",
        "\ucd94\ucc9c",
    ]

    def __init__(self, top_n: int = 20, timeout: int = 10):
        super().__init__(top_n=top_n)
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Referer": "https://search.naver.com/",
            }
        )

    def _build_params(self, seed_keyword: str) -> dict[str, Any]:
        return {
            "where": "nexearch",
            "sm": "top_hty",
            "query": seed_keyword,
        }

    def _request_html(self, seed_keyword: str) -> str | None:
        try:
            response = self.session.get(
                self.SEARCH_URL,
                params=self._build_params(seed_keyword),
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.text
        except RequestException:
            return None

    def _find_related_sections(self, soup: BeautifulSoup) -> list[Tag]:
        sections: list[Tag] = []
        seen_ids: set[int] = set()

        for selector in self.RELATED_SECTION_SELECTORS:
            for section in soup.select(selector):
                if not isinstance(section, Tag):
                    continue

                section_text = section.get_text(" ", strip=True)
                if not any(hint in section_text for hint in self.SECTION_HINT_TEXTS):
                    continue

                section_id = id(section)
                if section_id in seen_ids:
                    continue

                seen_ids.add(section_id)
                sections.append(section)

        return sections

    def _extract_keyword_from_href(self, href: str) -> str:
        if not href:
            return ""

        parsed = urlparse(href)
        query_params = parse_qs(parsed.query)

        for key in ("query", "q"):
            values = query_params.get(key)
            if values:
                return self.normalize_keyword(values[0])

        return ""

    def _extract_keyword_from_anchor(self, anchor: Tag) -> str:
        href = str(anchor.get("href", "")).strip()
        keyword_from_href = self._extract_keyword_from_href(href)

        if keyword_from_href:
            return keyword_from_href

        return self.normalize_keyword(anchor.get_text(" ", strip=True))

    def _collect_section_keywords(self, section: Tag, seed_keyword: str) -> list[str]:
        normalized_seed = self.normalize_keyword(seed_keyword)
        keywords: list[str] = []

        for selector in self.RELATED_LINK_SELECTORS:
            for anchor in section.select(selector):
                if not isinstance(anchor, Tag):
                    continue

                keyword = self._extract_keyword_from_anchor(anchor)
                if not keyword:
                    continue
                if keyword == normalized_seed:
                    continue
                if keyword in self.SECTION_HINT_TEXTS:
                    continue
                keywords.append(keyword)

        return keywords

    def collect_keywords(self, seed_keyword: str) -> list[str]:
        normalized_seed = self.normalize_keyword(seed_keyword)
        if not normalized_seed:
            return []

        html = self._request_html(normalized_seed)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        sections = self._find_related_sections(soup)

        keywords: list[str] = []
        for section in sections:
            keywords.extend(self._collect_section_keywords(section, normalized_seed))

        deduped: list[str] = []
        seen: set[str] = set()

        for keyword in keywords:
            normalized_keyword = self.normalize_keyword(keyword)
            if not normalized_keyword:
                continue
            if normalized_keyword == normalized_seed:
                continue
            if normalized_keyword in seen:
                continue
            seen.add(normalized_keyword)
            deduped.append(normalized_keyword)

        return deduped[: self.top_n]

    def collect(self, seed_keyword: str) -> pd.DataFrame:
        keywords = self.collect_keywords(seed_keyword)
        return self.build_rows(
            seed_keyword=seed_keyword,
            keywords=keywords,
        )
