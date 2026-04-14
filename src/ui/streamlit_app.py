from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

# Allow direct execution via `streamlit run .../streamlit_app.py` from arbitrary cwd.
ROOT_DIR = Path(__file__).resolve().parents[4]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from projects.keyword.src.services.pipeline_runner import (
    ParallelPipelineResult,
    PipelineRunConfig,
    PipelineRunResult,
    run_parallel,
)


MAX_GRAPH_KEYWORDS = 20
SHARED_LOGO_PATH = ROOT_DIR / "projects" / "shared_assets" / "gn_logo.jpg"
CHART_COLOR_RANGE = [
    "#ff3b30",
    "#ff6b6b",
    "#ff8a65",
    "#ff1744",
    "#d50000",
    "#ff5252",
    "#ff7043",
    "#ffab91",
]


WINDOW_OPTIONS = {
    "최근 1일": 1,
    "최근 7일": 7,
    "최근 30일": 30,
}


def _apply_modern_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&display=swap');

        :root {
            --bg-primary: #050505;
            --surface: rgba(255, 255, 255, 0.04);
            --surface-strong: rgba(255, 255, 255, 0.08);
            --border: rgba(255, 75, 75, 0.28);
            --border-strong: rgba(255, 75, 75, 0.55);
            --text-primary: #f7f7f7;
            --text-secondary: #b8b8b8;
            --accent: #ff3b30;
            --shadow: 0 24px 60px rgba(0, 0, 0, 0.45);
        }

        .stApp {
            background:
                radial-gradient(circle at top left, rgba(255, 59, 48, 0.24), transparent 28%),
                radial-gradient(circle at top right, rgba(170, 0, 0, 0.22), transparent 30%),
                linear-gradient(180deg, #090909 0%, #050505 55%, #020202 100%);
            color: var(--text-primary);
            font-family: "Space Grotesk", sans-serif;
        }

        .block-container {
            max-width: 1280px;
            padding-top: 2.25rem;
            padding-bottom: 2rem;
        }

        h1, h2, h3, h4, .stMarkdown, .stCaption, label, [data-testid="stMetricLabel"], [data-testid="stMetricValue"] {
            font-family: "Space Grotesk", sans-serif !important;
            color: var(--text-primary) !important;
        }

        p, span, div {
            color: inherit;
        }

        [data-testid="stHeader"] {
            background: transparent;
        }

        .brand-bar {
            display: flex;
            align-items: center;
            gap: 1rem;
            margin-bottom: 1rem;
            padding: 0.2rem 0 0.35rem 0;
        }

        .brand-logo-wrap {
            width: 92px;
            flex: 0 0 92px;
        }

        .brand-copy {
            display: flex;
            flex-direction: column;
            justify-content: center;
            gap: 0.15rem;
            min-height: 92px;
        }

        .brand-label {
            color: #ff9a94;
            font-size: 0.82rem;
            font-weight: 700;
            letter-spacing: 0.12em;
            text-transform: uppercase;
        }

        .brand-name {
            color: var(--text-primary);
            font-size: 1.12rem;
            font-weight: 700;
            letter-spacing: -0.02em;
        }

        .brand-subtitle {
            color: var(--text-secondary);
            font-size: 0.92rem;
            line-height: 1.45;
        }

        .hero-shell {
            position: relative;
            overflow: hidden;
            padding: 1.6rem 1.7rem;
            margin-bottom: 1.4rem;
            border: 1px solid var(--border);
            border-radius: 24px;
            background:
                linear-gradient(135deg, rgba(255, 59, 48, 0.18), rgba(12, 12, 12, 0.96) 42%),
                linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.015));
            box-shadow: var(--shadow);
        }

        .hero-shell::after {
            content: "";
            position: absolute;
            inset: auto -10% -55% 35%;
            height: 280px;
            background: radial-gradient(circle, rgba(255, 59, 48, 0.28), transparent 62%);
            pointer-events: none;
        }

        .hero-kicker {
            display: inline-block;
            margin-bottom: 0.65rem;
            padding: 0.35rem 0.65rem;
            border: 1px solid rgba(255, 107, 107, 0.38);
            border-radius: 999px;
            background: rgba(255, 59, 48, 0.1);
            color: #ffd8d6;
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }

        .hero-title {
            margin: 0;
            font-size: clamp(2rem, 4vw, 3.3rem);
            line-height: 0.98;
            letter-spacing: -0.04em;
        }

        .hero-copy {
            max-width: 760px;
            margin: 0.7rem 0 0;
            color: var(--text-secondary);
            font-size: 1rem;
            line-height: 1.6;
        }

        .stForm {
            border: 1px solid var(--border) !important;
            border-radius: 22px !important;
            background: linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02)) !important;
            box-shadow: var(--shadow);
            padding: 1rem 1rem 0.35rem 1rem;
        }

        div[data-testid="stTextArea"] textarea,
        div[data-testid="stTextInput"] input {
            color: var(--text-primary) !important;
            background: rgba(0, 0, 0, 0.55) !important;
            border: 1px solid rgba(255, 255, 255, 0.08) !important;
            border-radius: 18px !important;
        }

        div[data-testid="stTextArea"] textarea:focus,
        div[data-testid="stTextInput"] input:focus {
            border-color: var(--border-strong) !important;
            box-shadow: 0 0 0 1px rgba(255, 59, 48, 0.35) !important;
        }

        .stButton > button,
        div[data-testid="stFormSubmitButton"] > button {
            border: 1px solid rgba(255, 107, 107, 0.55) !important;
            border-radius: 999px !important;
            background: linear-gradient(135deg, #ff3b30, #7a0000) !important;
            color: white !important;
            font-weight: 700 !important;
            box-shadow: 0 16px 30px rgba(122, 0, 0, 0.32);
            transition: transform 0.18s ease, box-shadow 0.18s ease, filter 0.18s ease;
        }

        .stButton > button:hover,
        div[data-testid="stFormSubmitButton"] > button:hover {
            transform: translateY(-1px);
            filter: brightness(1.06);
            box-shadow: 0 18px 34px rgba(255, 59, 48, 0.25);
        }

        [data-testid="stRadio"] div[role="radiogroup"] {
            gap: 0.5rem;
            padding: 0.35rem;
            border: 1px solid rgba(255,255,255,0.06);
            border-radius: 999px;
            background: rgba(255,255,255,0.03);
            width: fit-content;
        }

        [data-testid="stMetric"] {
            padding: 1rem 1.1rem;
            border: 1px solid rgba(255,255,255,0.06);
            border-radius: 20px;
            background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.025));
            box-shadow: var(--shadow);
        }

        [data-testid="stDataFrame"], div[data-testid="stExpander"], .stAlert {
            border: 1px solid rgba(255,255,255,0.07) !important;
            border-radius: 22px !important;
            background: linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02)) !important;
            overflow: hidden;
        }

        [data-testid="stTabs"] button[role="tab"] {
            border-radius: 999px;
            color: var(--text-secondary);
        }

        [data-testid="stTabs"] button[aria-selected="true"] {
            background: rgba(255, 59, 48, 0.18) !important;
            color: var(--text-primary) !important;
        }

        .stCaption {
            color: var(--text-secondary) !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_hero() -> None:
    st.markdown(
        """
        <section class="hero-shell">
            <div class="hero-kicker">Black / Red UI</div>
            <h1 class="hero-title">Keyword Discovery Dashboard</h1>
            <p class="hero-copy">
                Explore related and autocomplete keyword momentum with a darker, modern control room feel.
            </p>
        </section>
        """,
        unsafe_allow_html=True,
    )


def _render_brand_header() -> None:
    logo_col, text_col = st.columns([1, 6])
    with logo_col:
        if SHARED_LOGO_PATH.exists():
            st.image(str(SHARED_LOGO_PATH), width=92)
    with text_col:
        st.markdown(
            """
            <div class="brand-copy">
                <div class="brand-label">GN Brand</div>
                <div class="brand-name">Keyword Discovery</div>
                <div class="brand-subtitle">Search intelligence dashboard</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _build_config() -> PipelineRunConfig:
    return PipelineRunConfig(
        top_n=30,
        save_outputs=True,
        related_weight_naver=0.7,
        related_weight_trend=0.3,
        save_to_db=True,
    )


def _render_messages(messages: list[str]) -> None:
    if not messages:
        return
    with st.expander("실행 로그", expanded=False):
        for message in messages:
            st.write(f"- {message}")


def _format_keywords_table_by_window(df: pd.DataFrame, window_days: int) -> pd.DataFrame:
    work = df.copy()
    trend_col = f"trend_avg_{window_days}d"

    if "keyword" not in work.columns:
        work["keyword"] = pd.NA
    if "predicted_search_volume" not in work.columns:
        work["predicted_search_volume"] = 0.0
    if trend_col not in work.columns:
        work[trend_col] = 0.0

    work["predicted_search_volume"] = pd.to_numeric(work["predicted_search_volume"], errors="coerce").fillna(0.0)
    work[trend_col] = pd.to_numeric(work[trend_col], errors="coerce").fillna(0.0)

    vol_min = work["predicted_search_volume"].min()
    vol_max = work["predicted_search_volume"].max()
    if pd.isna(vol_min) or pd.isna(vol_max) or vol_min == vol_max:
        work["volume_norm"] = 1.0
    else:
        work["volume_norm"] = (work["predicted_search_volume"] - vol_min) / (vol_max - vol_min)

    tr_min = work[trend_col].min()
    tr_max = work[trend_col].max()
    if pd.isna(tr_min) or pd.isna(tr_max) or tr_min == tr_max:
        work["trend_norm"] = 1.0
    else:
        work["trend_norm"] = (work[trend_col] - tr_min) / (tr_max - tr_min)

    work["window_weighted_score"] = work["volume_norm"] * 0.7 + work["trend_norm"] * 0.3
    work = work.sort_values(
        ["window_weighted_score", "predicted_search_volume", trend_col, "keyword"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    work["랭크"] = range(1, len(work) + 1)
    work["기간 트렌드 평균"] = work[trend_col].fillna(0).astype(int)

    return work.loc[:, ["keyword", "predicted_search_volume", "기간 트렌드 평균", "랭크"]].rename(
        columns={
            "keyword": "키워드",
            "predicted_search_volume": "예측 검색량",
        }
    )


def _format_combined_table_by_window(df: pd.DataFrame, window_days: int) -> pd.DataFrame:
    """Format combined ranked df (has seed_keyword column) for display."""
    if df.empty:
        return pd.DataFrame()
    work = _format_keywords_table_by_window(df, window_days)
    # Re-attach seed_keyword from original df aligned by index
    if "seed_keyword" in df.columns:
        seed_col = df["seed_keyword"].reset_index(drop=True)
        # After sort inside _format_keywords_table_by_window the index is reset,
        # so we need to join on keyword instead.
        kw_to_seed = df.set_index("keyword")["seed_keyword"].to_dict() if "keyword" in df.columns else {}
        work.insert(0, "시드 키워드", work["키워드"].map(kw_to_seed).fillna(""))
    return work


def _filter_window_timeseries(ts_df: pd.DataFrame, window_days: int) -> pd.DataFrame:
    if ts_df.empty or "period" not in ts_df.columns:
        return pd.DataFrame()

    work = ts_df.copy()
    work["period"] = pd.to_datetime(work["period"], errors="coerce")
    work["naver_index"] = pd.to_numeric(work.get("naver_index"), errors="coerce")
    work = work.dropna(subset=["period", "keyword", "naver_index"]).copy()
    if work.empty:
        return work

    max_date = work["period"].max()
    cutoff = max_date - pd.Timedelta(days=window_days - 1)
    return work[work["period"] >= cutoff].copy()


def _build_combined_timeseries(parallel_result: ParallelPipelineResult) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for seed_keyword in parallel_result.keywords:
        result = parallel_result.results.get(seed_keyword)
        if result is None or result.status != "completed":
            continue

        for category, frame in (
            ("related", result.related_trend_df),
            ("autocomplete", result.autocomplete_trend_df),
        ):
            if frame.empty:
                continue
            work = frame.copy()
            work["seed_keyword"] = seed_keyword
            work["category"] = category
            frames.append(work)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = combined.drop_duplicates(
        subset=["seed_keyword", "category", "keyword", "period"],
        keep="first",
    )
    return combined.reset_index(drop=True)


def _select_combined_graph_keywords(
    combined_ranked_df: pd.DataFrame,
    seed_keywords: list[str],
    limit: int = MAX_GRAPH_KEYWORDS,
) -> list[str]:
    if combined_ranked_df.empty or "keyword" not in combined_ranked_df.columns:
        return []

    work = combined_ranked_df.copy()
    work["keyword"] = work["keyword"].astype(str).str.strip()
    work = work[work["keyword"] != ""].copy()
    if work.empty:
        return []

    if "predicted_search_volume" not in work.columns:
        work["predicted_search_volume"] = 0.0
    work["predicted_search_volume"] = pd.to_numeric(work["predicted_search_volume"], errors="coerce").fillna(0.0)

    deduped = (
        work.sort_values(["predicted_search_volume", "keyword"], ascending=[False, True])
        .drop_duplicates(subset=["keyword"], keep="first")
        .reset_index(drop=True)
    )

    selected: list[str] = []
    for keyword in seed_keywords:
        normalized = str(keyword).strip()
        if normalized and normalized not in selected:
            selected.append(normalized)
        if len(selected) >= limit:
            return selected[:limit]

    for keyword in deduped["keyword"].tolist():
        if keyword not in selected:
            selected.append(keyword)
        if len(selected) >= limit:
            break

    return selected[:limit]


def _build_combined_chart_ranked_df(
    combined_ranked_df: pd.DataFrame,
    seed_keywords: list[str],
    limit: int = MAX_GRAPH_KEYWORDS,
) -> pd.DataFrame:
    selected_keywords = _select_combined_graph_keywords(
        combined_ranked_df,
        seed_keywords,
        limit=limit,
    )
    if not selected_keywords:
        return pd.DataFrame()

    work = combined_ranked_df.copy()
    work["keyword"] = work["keyword"].astype(str).str.strip()
    work["predicted_search_volume"] = pd.to_numeric(
        work.get("predicted_search_volume"),
        errors="coerce",
    ).fillna(0.0)
    work = work[work["keyword"].isin(selected_keywords)].copy()
    if work.empty:
        return pd.DataFrame()

    work = (
        work.sort_values(["predicted_search_volume", "keyword"], ascending=[False, True])
        .drop_duplicates(subset=["keyword"], keep="first")
        .reset_index(drop=True)
    )
    work["display_order"] = range(1, len(work) + 1)
    return work


def _format_combined_volume_table(df: pd.DataFrame, window_days: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    work = df.copy()
    trend_col = f"trend_avg_{window_days}d"
    if trend_col not in work.columns:
        work[trend_col] = 0.0

    work["predicted_search_volume"] = pd.to_numeric(work.get("predicted_search_volume"), errors="coerce").fillna(0.0)
    work[trend_col] = pd.to_numeric(work.get(trend_col), errors="coerce").fillna(0).astype(int)
    work = work.sort_values(["predicted_search_volume", "keyword"], ascending=[False, True]).reset_index(drop=True)
    work["순위"] = range(1, len(work) + 1)

    columns = [col for col in ["seed_keyword", "category", "keyword", "predicted_search_volume", trend_col, "순위"] if col in work.columns]
    return work.loc[:, columns].rename(
        columns={
            "seed_keyword": "시드 키워드",
            "category": "구분",
            "keyword": "키워드",
            "predicted_search_volume": "도출 검색량",
            trend_col: f"{window_days}일 평균 트렌드",
        }
    )


def _render_line_chart(title: str, ts_df: pd.DataFrame, ranked_df: pd.DataFrame, window_days: int) -> None:
    st.subheader(title)

    if ts_df.empty or ranked_df.empty:
        st.info("표시할 시계열 데이터가 없습니다.")
        return

    top_keywords = ranked_df["keyword"].astype(str).head(MAX_GRAPH_KEYWORDS).tolist()
    work = _filter_window_timeseries(ts_df, window_days)
    if work.empty:
        st.info("선택한 기간의 시계열 데이터가 없습니다.")
        return

    work = work[work["keyword"].astype(str).isin(top_keywords)].copy()
    if work.empty:
        st.info("상위 키워드와 매칭되는 시계열 데이터가 없습니다.")
        return

    y_max = float(work["naver_index"].max()) if work["naver_index"].notna().any() else 100.0
    y_domain_max = max(100.0, y_max * 1.05)

    chart = (
        alt.Chart(work)
        .mark_line(point=alt.OverlayMarkDef(size=54, filled=True), strokeWidth=3)
        .encode(
            x=alt.X("period:T", title="날짜"),
            y=alt.Y("naver_index:Q", title="트렌드 지수", scale=alt.Scale(domain=[0, y_domain_max])),
            color=alt.Color("keyword:N", title="키워드", scale=alt.Scale(range=CHART_COLOR_RANGE)),
            tooltip=["keyword", "period", "naver_index"],
        )
        .properties(height=360)
        .configure_view(strokeWidth=0)
        .configure_axis(
            gridColor="rgba(255,255,255,0.08)",
            domainColor="rgba(255,255,255,0.18)",
            tickColor="rgba(255,255,255,0.18)",
            labelColor="#f3f3f3",
            titleColor="#f3f3f3",
        )
        .configure_legend(
            labelColor="#f3f3f3",
            titleColor="#f3f3f3",
            orient="bottom",
        )
        .configure(background="transparent")
    )
    st.caption(f"그래프는 기본으로 상위 {MAX_GRAPH_KEYWORDS}개 키워드만 표시합니다.")
    st.altair_chart(chart, use_container_width=True)


def _render_combined_line_chart(ts_df: pd.DataFrame, ranked_df: pd.DataFrame, window_days: int) -> None:
    st.subheader("전체 키워드 트렌드")

    if ts_df.empty or ranked_df.empty:
        st.info("전체 시각화에 사용할 시계열 데이터가 없습니다.")
        return

    selected_keywords = ranked_df["keyword"].astype(str).tolist()
    work = _filter_window_timeseries(ts_df, window_days)
    if work.empty:
        st.info("선택한 기간에 사용할 시계열 데이터가 없습니다.")
        return

    work = work[work["keyword"].astype(str).isin(selected_keywords)].copy()
    if work.empty:
        st.info("선택된 키워드와 매칭되는 시계열 데이터가 없습니다.")
        return

    work["period"] = pd.to_datetime(work["period"], errors="coerce")
    work["naver_index"] = pd.to_numeric(work.get("naver_index"), errors="coerce")
    work = work.dropna(subset=["period", "keyword", "naver_index"]).copy()
    work = work.sort_values(["keyword", "period", "naver_index"], ascending=[True, True, False])
    work = work.drop_duplicates(subset=["keyword", "period"], keep="first").reset_index(drop=True)
    if work.empty:
        st.info("선택된 키워드와 매칭되는 시계열 데이터가 없습니다.")
        return

    keyword_order = ranked_df["keyword"].astype(str).tolist()

    y_max = float(work["naver_index"].max()) if work["naver_index"].notna().any() else 100.0
    y_domain_max = max(100.0, y_max * 1.05)

    chart = (
        alt.Chart(work)
        .mark_line(point=alt.OverlayMarkDef(size=54, filled=True), strokeWidth=3)
        .encode(
            x=alt.X("period:T", title="날짜"),
            y=alt.Y("naver_index:Q", title="트렌드 지수", scale=alt.Scale(domain=[0, y_domain_max])),
            color=alt.Color(
                "keyword:N",
                title="키워드",
                sort=keyword_order,
                scale=alt.Scale(range=CHART_COLOR_RANGE),
            ),
            tooltip=[
                alt.Tooltip("keyword:N", title="키워드"),
                alt.Tooltip("seed_keyword:N", title="Seed"),
                alt.Tooltip("category:N", title="구분"),
                alt.Tooltip("period:T", title="날짜"),
                alt.Tooltip("naver_index:Q", title="트렌드 지수", format=".2f"),
            ],
        )
        .properties(height=420)
        .configure_view(strokeWidth=0)
        .configure_axis(
            gridColor="rgba(255,255,255,0.08)",
            domainColor="rgba(255,255,255,0.18)",
            tickColor="rgba(255,255,255,0.18)",
            labelColor="#f3f3f3",
            titleColor="#f3f3f3",
        )
        .configure_legend(
            labelColor="#f3f3f3",
            titleColor="#f3f3f3",
            orient="bottom",
        )
        .configure(background="transparent")
    )
    st.caption(f"seed 키워드를 포함해 검색량 기준 상위 {len(ranked_df)}개 키워드를 표시합니다.")
    st.altair_chart(chart, use_container_width=True)


def _render_summary(summary_df: pd.DataFrame) -> None:
    if summary_df.empty:
        return

    related_row = summary_df[summary_df["category"] == "related"]
    auto_row = summary_df[summary_df["category"] == "autocomplete"]

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### 연관 키워드 요약")
        if not related_row.empty:
            row = related_row.iloc[0]
            st.metric("최근 30일 평균 검색량", f"{float(row['avg_search_volume']):,.1f}")
            st.write(f"Top 성장 키워드: **{row['top_growth_keyword_30d']}**")
        else:
            st.write("요약 데이터 없음")

    with c2:
        st.markdown("### 자동완성 키워드 요약")
        if not auto_row.empty:
            row = auto_row.iloc[0]
            st.metric("최근 30일 평균 검색량", f"{float(row['avg_search_volume']):,.1f}")
            st.write(f"Top 성장 키워드: **{row['top_growth_keyword_30d']}**")
        else:
            st.write("요약 데이터 없음")


def _render_saved_paths(saved_paths: dict[str, object]) -> None:
    if not saved_paths:
        return

    with st.expander("저장된 파일 경로", expanded=False):
        for name, path in saved_paths.items():
            st.code(f"{name}: {path}")


def _render_single_result(result: PipelineRunResult, window_days: int) -> None:
    """Render dashboard for one keyword result."""
    _render_summary(result.summary_df)

    col1, col2 = st.columns(2)
    with col1:
        _render_line_chart(
            "연관 키워드 트렌드",
            result.related_trend_df,
            result.related_keywords_df,
            window_days,
        )
    with col2:
        _render_line_chart(
            "자동완성 키워드 트렌드",
            result.autocomplete_trend_df,
            result.autocomplete_keywords_df,
            window_days,
        )

    table_col1, table_col2 = st.columns(2)
    with table_col1:
        st.subheader("연관 키워드 테이블")
        st.dataframe(
            _format_keywords_table_by_window(result.related_keywords_df, window_days),
            use_container_width=True,
            hide_index=True,
        )
    with table_col2:
        st.subheader("자동완성 키워드 테이블")
        st.dataframe(
            _format_keywords_table_by_window(result.autocomplete_keywords_df, window_days),
            use_container_width=True,
            hide_index=True,
        )

    _render_messages(result.messages)
    _render_saved_paths(result.saved_paths)


def _render_combined_tab(parallel_result: ParallelPipelineResult, window_days: int) -> None:
    """Render the 전체 combined tab."""
    if parallel_result.failed_keywords:
        st.warning(
            f"실패한 키워드 ({len(parallel_result.failed_keywords)}개): "
            + ", ".join(parallel_result.failed_keywords)
        )
        for kw in parallel_result.failed_keywords:
            r = parallel_result.results.get(kw)
            if r and r.messages:
                with st.expander(f"오류 로그: {kw}", expanded=False):
                    for msg in r.messages:
                        st.write(f"- {msg}")

    if parallel_result.combined_ranked_df.empty:
        st.info("표시할 합산 결과가 없습니다.")
        return

    combined_chart_ranked_df = _build_combined_chart_ranked_df(
        parallel_result.combined_ranked_df,
        parallel_result.keywords,
    )
    combined_timeseries_df = _build_combined_timeseries(parallel_result)
    _render_combined_line_chart(combined_timeseries_df, combined_chart_ranked_df, window_days)

    st.subheader("합산 키워드 테이블")
    st.dataframe(
        _format_combined_volume_table(
            combined_chart_ranked_df if not combined_chart_ranked_df.empty else parallel_result.combined_ranked_df,
            window_days,
        ),
        use_container_width=True,
        hide_index=True,
    )

    if not parallel_result.combined_summary_df.empty:
        st.subheader("키워드별 요약")
        st.dataframe(parallel_result.combined_summary_df, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Keyword Discovery Dashboard", layout="wide")
    _apply_modern_theme()
    _render_brand_header()
    _render_hero()

    if "parallel_result" not in st.session_state:
        st.session_state["parallel_result"] = None

    with st.form("run_form", clear_on_submit=False):
        keywords_input = st.text_area(
            "Seed keywords (줄바꿈으로 구분)",
            placeholder="예:\n비뇨기과\n피부과\n성형외과",
            height=120,
        )
        run_clicked = st.form_submit_button("실행", type="primary")

    if run_clicked:
        raw_lines = keywords_input.strip().splitlines()
        unique_keywords = list(dict.fromkeys(kw.strip() for kw in raw_lines if kw.strip()))
        if not unique_keywords:
            st.error("키워드를 입력하십쇼.")
        else:
            config = _build_config()
            try:
                with st.spinner(f"{len(unique_keywords)}개 키워드 병렬 실행 중..."):
                    parallel_result = run_parallel(
                        unique_keywords,
                        config=config,
                        enrich=True,
                    )
            except Exception as exc:
                st.error(f"Pipeline failed: {exc}")
            else:
                st.session_state["parallel_result"] = parallel_result
                succeeded = len(parallel_result.succeeded_keywords)
                failed = len(parallel_result.failed_keywords)
                if failed:
                    st.warning(f"완료: {succeeded}개 성공, {failed}개 실패")
                else:
                    st.success(f"파이프라인 실행 완료 ({succeeded}개 키워드)")

    parallel_result: ParallelPipelineResult | None = st.session_state.get("parallel_result")
    if parallel_result is None:
        st.info("키워드를 입력하고 실행 버튼을 누르면 결과 대시보드가 표시됩니다.")
        return

    window_label = st.radio("대시보드 기간 브래킷", list(WINDOW_OPTIONS.keys()), horizontal=True)
    window_days = WINDOW_OPTIONS[window_label]

    keywords = parallel_result.keywords
    if len(keywords) == 1:
        # Single keyword — no tab overhead
        kw = keywords[0]
        result = parallel_result.results[kw]
        if result.status != "completed":
            st.error(f"'{kw}' 파이프라인 실패")
            _render_messages(result.messages)
        else:
            _render_single_result(result, window_days)
    else:
        # Multiple keywords — make the combined view the default first tab.
        tab_labels = ["전체"] + keywords
        tabs = st.tabs(tab_labels)

        with tabs[0]:
            _render_combined_tab(parallel_result, window_days)

        for i, kw in enumerate(keywords, start=1):
            with tabs[i]:
                result = parallel_result.results[kw]
                if result.status != "completed":
                    st.error(f"'{kw}' 파이프라인 실패")
                    _render_messages(result.messages)
                else:
                    _render_single_result(result, window_days)


if __name__ == "__main__":
    main()
