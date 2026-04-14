-- ============================================================
-- Keyword Analytics Warehouse
-- Target  : MySQL 8.0+
-- Schema  : keywords (create manually or via first block below)
-- ============================================================

-- CREATE SCHEMA IF NOT EXISTS `keywords` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE `keywords`;

-- ============================================================
-- 1. pipeline_runs
--    파이프라인 실행 단위 메타데이터. 한 번의 run() 호출 = 1행.
-- ============================================================
CREATE TABLE IF NOT EXISTS `pipeline_runs` (
    `run_id`          CHAR(36)      NOT NULL                    COMMENT 'UUID — 앱에서 생성 후 전달',
    `seed_keyword`    VARCHAR(200)  NOT NULL                    COMMENT '입력 시드 키워드',
    `started_at`      DATETIME(3)   NOT NULL                    COMMENT '실행 시작 시각',
    `finished_at`     DATETIME(3)   NULL                        COMMENT '실행 완료 시각 (실패/진행 중이면 NULL)',
    `status`          VARCHAR(20)   NOT NULL DEFAULT 'running'  COMMENT 'running | success | failed',
    `error_message`   TEXT          NULL                        COMMENT '실패 시 에러 메시지',
    `candidate_count` SMALLINT      NULL                        COMMENT 'discovery 후보 키워드 수',
    `config`          JSON          NULL                        COMMENT 'top_n, timeout, weights 등 실행 설정',
    PRIMARY KEY (`run_id`),
    INDEX `idx_runs_seed` (`seed_keyword`, `started_at` DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='파이프라인 실행 단위 메타데이터';


-- ============================================================
-- 2. keyword_candidates
--    SA 보강 이전 discovery 원본.
--    어떤 provider가 어떤 키워드를 몇 위로 발견했는지 추적.
-- ============================================================
CREATE TABLE IF NOT EXISTS `keyword_candidates` (
    `id`              BIGINT        NOT NULL AUTO_INCREMENT,
    `run_id`          CHAR(36)      NOT NULL                    COMMENT '→ pipeline_runs.run_id',
    `seed_keyword`    VARCHAR(200)  NOT NULL,
    `keyword`         VARCHAR(200)  NOT NULL                    COMMENT '발견된 후보 키워드',
    `track`           VARCHAR(20)   NOT NULL                    COMMENT 'related | autocomplete',
    `provider`        VARCHAR(50)   NOT NULL                    COMMENT 'google_trends | naver_autocomplete | naver_related_search | seed_fallback',
    `rank`            SMALLINT      NULL                        COMMENT 'provider 내 순위',
    `score_hint`      DECIMAL(10,4) NULL                        COMMENT 'provider 힌트 점수',
    `discovery_score` DECIMAL(10,4) NULL                        COMMENT '정규화 발견 점수',
    `inserted_at`     DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    INDEX `idx_cand_run`    (`run_id`),
    INDEX `idx_cand_kw`     (`keyword`, `provider`),
    CONSTRAINT `fk_cand_run` FOREIGN KEY (`run_id`) REFERENCES `pipeline_runs` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='SA 보강 이전 discovery 원본 — provenance 추적용';


-- ============================================================
-- 3. keyword_sa_raw
--    Naver SearchAd API 전체 응답 원본.
--    pipeline 내부에서 _build_searchad_metrics()가 compIdx/CTR/클릭
--    등을 버리므로, 분석·모델 고도화를 위해 원본 전체 보존.
-- ============================================================
CREATE TABLE IF NOT EXISTS `keyword_sa_raw` (
    `id`                      BIGINT        NOT NULL AUTO_INCREMENT,
    `run_id`                  CHAR(36)      NOT NULL                    COMMENT '→ pipeline_runs.run_id',
    `seed_keyword`            VARCHAR(200)  NOT NULL,
    `keyword`                 VARCHAR(200)  NOT NULL                    COMMENT 'SA API 반환 키워드',
    -- 검색량
    `monthly_pc_qc_cnt`       DECIMAL(14,2) NULL                        COMMENT 'PC 월 검색량',
    `monthly_mobile_qc_cnt`   DECIMAL(14,2) NULL                        COMMENT '모바일 월 검색량',
    `monthly_total_qc`        DECIMAL(14,2) NULL                        COMMENT '합산 검색량 (pc + mobile)',
    -- 클릭
    `monthly_avg_pc_clk`      DECIMAL(14,2) NULL                        COMMENT 'PC 월 평균 클릭수',
    `monthly_avg_mobile_clk`  DECIMAL(14,2) NULL                        COMMENT '모바일 월 평균 클릭수',
    -- CTR
    `monthly_avg_pc_ctr`      DECIMAL(8,6)  NULL                        COMMENT 'PC CTR',
    `monthly_avg_mobile_ctr`  DECIMAL(8,6)  NULL                        COMMENT '모바일 CTR',
    -- 기타
    `pl_avg_depth`            DECIMAL(8,4)  NULL                        COMMENT '랜딩페이지 평균 depth',
    `comp_idx`                DECIMAL(6,2)  NULL                        COMMENT '경쟁 지수 0–100',
    `mobile_ratio`            DECIMAL(8,6)  NULL                        COMMENT '모바일 비율',
    `pc_ratio`                DECIMAL(8,6)  NULL                        COMMENT 'PC 비율',
    `inserted_at`             DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    INDEX `idx_sa_run` (`run_id`),
    INDEX `idx_sa_kw`  (`keyword`),
    CONSTRAINT `fk_sa_run` FOREIGN KEY (`run_id`) REFERENCES `pipeline_runs` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Naver SearchAd API 전체 응답 원본 — pipeline이 버리는 필드 포함';


-- ============================================================
-- 4. keyword_metrics
--    _build_category_rank() + _apply_google_anchor_scaling() 출력.
--    UI가 직접 읽는 핵심 테이블.
--    * UI는 window별로 weighted_score를 재계산하므로
--      pipeline final_rank만 저장 (UI rank는 저장 불필요).
-- ============================================================
CREATE TABLE IF NOT EXISTS `keyword_metrics` (
    `id`               BIGINT        NOT NULL AUTO_INCREMENT,
    `run_id`           CHAR(36)      NOT NULL                    COMMENT '→ pipeline_runs.run_id',
    `seed_keyword`     VARCHAR(200)  NOT NULL,
    `keyword`          VARCHAR(200)  NOT NULL,
    `track`            VARCHAR(20)   NOT NULL                    COMMENT 'related | autocomplete',
    `provider`         VARCHAR(50)   NULL                        COMMENT '주 discovery provider',
    `discovery_rank`   SMALLINT      NULL                        COMMENT 'discovery 시 순위',

    -- ── Naver SearchAd (pipeline이 실제 사용하는 필드) ──────────
    `monthly_pc_qc_cnt`     DECIMAL(14,2) NULL  COMMENT 'PC 월 검색량',
    `monthly_mobile_qc_cnt` DECIMAL(14,2) NULL  COMMENT '모바일 월 검색량',
    `monthly_total_qc`      DECIMAL(14,2) NULL  COMMENT '합산 검색량 = naver_total_qc',
    `mobile_ratio`          DECIMAL(8,6)  NULL  COMMENT '모바일 비율',
    `pc_ratio`              DECIMAL(8,6)  NULL  COMMENT 'PC 비율',

    -- ── Trend 집계 — 1일 ────────────────────────────────────────
    `trend_avg_1d`    DECIMAL(10,6) NULL,
    `trend_max_1d`    DECIMAL(10,6) NULL,
    `trend_min_1d`    DECIMAL(10,6) NULL,
    `trend_first_1d`  DECIMAL(10,6) NULL,
    `trend_last_1d`   DECIMAL(10,6) NULL,
    `trend_growth_1d` DECIMAL(10,6) NULL  COMMENT 'last − first',

    -- ── Trend 집계 — 7일 ────────────────────────────────────────
    `trend_avg_7d`    DECIMAL(10,6) NULL,
    `trend_max_7d`    DECIMAL(10,6) NULL,
    `trend_min_7d`    DECIMAL(10,6) NULL,
    `trend_first_7d`  DECIMAL(10,6) NULL,
    `trend_last_7d`   DECIMAL(10,6) NULL,
    `trend_growth_7d` DECIMAL(10,6) NULL,

    -- ── Trend 집계 — 30일 ───────────────────────────────────────
    `trend_avg_30d`    DECIMAL(10,6) NULL,
    `trend_max_30d`    DECIMAL(10,6) NULL,
    `trend_min_30d`    DECIMAL(10,6) NULL,
    `trend_first_30d`  DECIMAL(10,6) NULL,
    `trend_last_30d`   DECIMAL(10,6) NULL,
    `trend_growth_30d` DECIMAL(10,6) NULL,

    `trend_data_source` VARCHAR(20)  NULL  COMMENT 'naver_datalab | ml_inferred',

    -- ── Google Anchor (optional) ─────────────────────────────────
    `google_absolute_volume`    DECIMAL(14,2) NULL  COMMENT 'Google 절대 검색량 (CSV)',
    `google_anchor_scale`       DECIMAL(14,6) NULL  COMMENT '스케일 인자',
    `anchored_search_volume_1d` DECIMAL(14,4) NULL,
    `anchored_search_volume_7d` DECIMAL(14,4) NULL,
    `anchored_search_volume_30d`DECIMAL(14,4) NULL,
    `search_volume_source`      VARCHAR(30)   NULL  COMMENT 'google_anchor_scaled | naver_searchad',

    -- ── Scoring ─────────────────────────────────────────────────
    `predicted_search_volume` DECIMAL(14,4) NULL  COMMENT 'anchored 또는 naver_total_qc',
    `naver_score_norm`        DECIMAL(8,6)  NULL  COMMENT 'min-max 정규화 검색량 점수',
    `trend_score_norm`        DECIMAL(8,6)  NULL  COMMENT 'min-max 정규화 트렌드 점수',
    `weighted_score`          DECIMAL(8,6)  NULL  COMMENT '0.7 × naver + 0.3 × trend',
    `final_rank`              SMALLINT      NULL  COMMENT 'pipeline 산출 순위',

    `inserted_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (`id`),
    INDEX `idx_met_run`        (`run_id`),
    INDEX `idx_met_seed_track` (`seed_keyword`, `track`, `final_rank`),  -- UI 테이블 조회
    INDEX `idx_met_kw_ins`     (`keyword`, `inserted_at` DESC),          -- 키워드 히스토리
    CONSTRAINT `fk_met_run` FOREIGN KEY (`run_id`) REFERENCES `pipeline_runs` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='pipeline 최종 ranked 결과 — UI가 직접 읽는 핵심 테이블';


-- ============================================================
-- 5. keyword_trend_daily
--    DataLab API 시계열 원본 + ML 추론 행 포함.
--    UI 꺾은선 차트용. 키워드당 최대 30행.
-- ============================================================
CREATE TABLE IF NOT EXISTS `keyword_trend_daily` (
    `id`          BIGINT        NOT NULL AUTO_INCREMENT,
    `run_id`      CHAR(36)      NOT NULL                    COMMENT '→ pipeline_runs.run_id',
    `seed_keyword`VARCHAR(200)  NOT NULL,
    `keyword`     VARCHAR(200)  NOT NULL,
    `track`       VARCHAR(20)   NOT NULL                    COMMENT 'related | autocomplete',
    `trend_date`  DATE          NOT NULL                    COMMENT 'YYYY-MM-DD',
    `trend_index` DECIMAL(10,6) NULL                        COMMENT 'Naver 검색 지수 0–100',
    `data_source` VARCHAR(20)   NOT NULL DEFAULT 'naver_datalab'  COMMENT 'naver_datalab | ml_inferred',
    `inserted_at` DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    INDEX `idx_trend_run`     (`run_id`),
    INDEX `idx_trend_kw_date` (`keyword`, `track`, `trend_date`),   -- 차트 쿼리
    CONSTRAINT `fk_trend_run` FOREIGN KEY (`run_id`) REFERENCES `pipeline_runs` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='DataLab 시계열 — UI 꺾은선 차트용';


-- ============================================================
-- 6. Views (UI facing, Korean labels)
-- ============================================================

-- 키워드 랭킹 뷰 (30일 기준)
CREATE OR REPLACE VIEW `vw_keyword_ranking` AS
SELECT
    r.seed_keyword                              AS `시드키워드`,
    m.keyword                                   AS `키워드`,
    m.track                                     AS `트랙`,
    m.predicted_search_volume                   AS `예측검색량`,
    m.monthly_total_qc                          AS `네이버검색량`,
    m.trend_avg_30d                             AS `30일평균지수`,
    m.trend_growth_30d                          AS `30일성장률`,
    m.trend_avg_7d                              AS `7일평균지수`,
    m.trend_growth_7d                           AS `7일성장률`,
    m.trend_avg_1d                              AS `1일평균지수`,
    m.trend_data_source                         AS `트렌드출처`,
    m.weighted_score                            AS `파이프라인점수`,
    m.final_rank                                AS `파이프라인순위`,
    r.started_at                                AS `실행시각`
FROM `keyword_metrics` m
JOIN `pipeline_runs`   r ON r.run_id = m.run_id;


-- SA 원본 vs 파이프라인 사용값 비교 뷰 (분석용)
CREATE OR REPLACE VIEW `vw_sa_full` AS
SELECT
    r.seed_keyword,
    s.keyword,
    s.monthly_pc_qc_cnt,
    s.monthly_mobile_qc_cnt,
    s.monthly_total_qc,
    s.monthly_avg_pc_clk,
    s.monthly_avg_mobile_clk,
    s.monthly_avg_pc_ctr,
    s.monthly_avg_mobile_ctr,
    s.pl_avg_depth,
    s.comp_idx,
    s.mobile_ratio,
    s.pc_ratio,
    r.started_at                                AS `실행시각`
FROM `keyword_sa_raw` s
JOIN `pipeline_runs`  r ON r.run_id = s.run_id;
