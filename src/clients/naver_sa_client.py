from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Any, Dict, Optional

import pandas as pd
import requests

from projects.keyword.src.config import Settings


class NaverSearchAdClient:
    """
    Naver SearchAd API client

    주요 역할:
    - 연관 키워드 조회
    - 월간 PC/모바일 검색량 조회
    - 클릭/CTR/경쟁도 등 지표 조회
    """

    BASE_URL = "https://api.searchad.naver.com"

    def __init__(self, timeout: int = 20):
        self.api_key = Settings.NAVER_SA_API_KEY
        self.secret_key = Settings.NAVER_SA_SECRET_KEY
        self.customer_id = Settings.NAVER_SA_CUSTOMER_ID
        self.timeout = timeout
        self.session = requests.Session()

    def _generate_signature(self, timestamp: str, method: str, uri: str) -> str:
        """
        message = "{timestamp}.{method}.{uri}"
        HMAC-SHA256(secret_key, message) -> Base64
        """
        message = f"{timestamp}.{method}.{uri}"
        digest = hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _build_headers(self, method: str, uri: str) -> Dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        signature = self._generate_signature(timestamp, method, uri)

        return {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Timestamp": timestamp,
            "X-API-KEY": self.api_key,
            "X-Customer": str(self.customer_id),
            "X-Signature": signature,
        }

    def get(
        self,
        uri: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{uri}"
        headers = self._build_headers("GET", uri)

        response = self.session.get(
            url,
            headers=headers,
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def get_related_keywords(
        self,
        hint_keywords: str,
        show_detail: int = 1,
    ) -> pd.DataFrame:
        """
        hint_keywords:
            단일 키워드 또는 쉼표로 구분한 키워드 문자열
            예) "비뇨기과" / "비뇨기과,발기부전"

        show_detail:
            1 = 상세 지표 포함
        """
        uri = "/keywordstool"
        params = {
            "hintKeywords": hint_keywords,
            "showDetail": show_detail,
        }

        data = self.get(uri=uri, params=params)
        keyword_list = data.get("keywordList", [])
        df = pd.DataFrame(keyword_list)

        if df.empty:
            return df

        numeric_cols = [
            "monthlyPcQcCnt",
            "monthlyMobileQcCnt",
            "monthlyAvePcClkCnt",
            "monthlyAveMobileClkCnt",
            "monthlyAvePcCtr",
            "monthlyAveMobileCtr",
            "plAvgDepth",
            "compIdx",
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if {"monthlyPcQcCnt", "monthlyMobileQcCnt"}.issubset(df.columns):
            df["naver_total_qc"] = (
                df["monthlyPcQcCnt"].fillna(0) +
                df["monthlyMobileQcCnt"].fillna(0)
            )

            total = df["naver_total_qc"].replace(0, pd.NA)
            df["mobile_ratio"] = df["monthlyMobileQcCnt"] / total
            df["pc_ratio"] = df["monthlyPcQcCnt"] / total

        return df


if __name__ == "__main__":
    client = NaverSearchAdClient()

    try:
        df = client.get_related_keywords("비뇨기과")

        print("=== SearchAd Result ===")
        print(df.head(20))

        if not df.empty:
            print("\n=== Columns ===")
            print(df.columns.tolist())

    except Exception as e:
        print("오류:", e)
