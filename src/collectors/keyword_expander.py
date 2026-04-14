from __future__ import annotations

from typing import List

import pandas as pd


class KeywordExpander:
    """
    Prepare a keyword list without external crawling.
    """

    FALLBACK_SOURCE = "seed_fallback"

    def __init__(self, top_n: int = 20):
        self.top_n = top_n

    @staticmethod
    def normalize_keyword(keyword: str) -> str:
        return str(keyword).strip()

    @classmethod
    def deduplicate_keywords(cls, keywords: List[str]) -> List[str]:
        seen = set()
        deduped: List[str] = []

        for kw in keywords:
            normalized = cls.normalize_keyword(kw)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)

        return deduped

    def limit_keywords(self, keywords: List[str]) -> List[str]:
        return keywords[: self.top_n]

    @staticmethod
    def to_dataframe(
        seed_keyword: str,
        keywords: List[str],
        source: str = "seed_fallback",
    ) -> pd.DataFrame:
        rows = []
        for idx, kw in enumerate(keywords, start=1):
            rows.append(
                {
                    "seed_keyword": seed_keyword,
                    "keyword": kw,
                    "source": source,
                    "rank": idx,
                }
            )

        return pd.DataFrame(
            rows,
            columns=["seed_keyword", "keyword", "source", "rank"],
        )

    def expand_from_seed(self, seed_keyword: str) -> List[str]:
        normalized_seed = self.normalize_keyword(seed_keyword)
        if not normalized_seed:
            return []
        return self.limit_keywords([normalized_seed])

    def expand(self, seed_keyword: str) -> pd.DataFrame:
        normalized_seed = self.normalize_keyword(seed_keyword)
        keywords = self.expand_from_seed(normalized_seed)
        return self.to_dataframe(
            seed_keyword=normalized_seed,
            keywords=keywords,
            source=self.FALLBACK_SOURCE,
        )


if __name__ == "__main__":
    expander = KeywordExpander(top_n=20)
    seed = "diet"

    keywords = expander.expand_from_seed(seed)
    print("[LIST]")
    print(keywords)

    df = expander.expand(seed)
    print("\n[DATAFRAME]")
    print(df.head(20))
