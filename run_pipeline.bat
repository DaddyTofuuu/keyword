@echo off
setlocal

cd /d C:\Users\user\Analysis
call venv\Scripts\activate

REM Streamlit-only entrypoint for keyword project.
python -m projects.keyword.src.main %*

endlocal
