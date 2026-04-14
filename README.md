# Keyword Project

Streamlit dashboard for two-track keyword analysis.

## Tracks

- Related Keywords: Google Trends candidates + Naver SA weighted ranking (0.7 / 0.3)
- Autocomplete Keywords: Naver autocomplete candidates

## Time windows

- Recent 1 day
- Recent 7 days
- Recent 30 days

Data is collected on daily granularity for the recent 30-day period, then aggregated per window.

## Run

From `C:\Users\user\Analysis\projects\keyword`:

```bash
streamlit run streamlit_app.py
```

Or with launcher:

```bash
projects\keyword\run_pipeline.bat
```

Custom port:

```bash
projects\keyword\run_pipeline.bat --port 8502
```

## UI output

- Summary cards by category
- 2 line charts (related/autocomplete)
- 2 tables (related/autocomplete)
- Window toggle: 1d / 7d / 30d

## Database SQL

Warehouse DDL is in:

- `projects/keyword/sql/keyword_warehouse.sql`

Includes 1 database model, 2 schemas, and 2 UI views with `inserted_date_time` columns.
