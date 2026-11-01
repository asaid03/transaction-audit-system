Set-Location $PSScriptRoot
if (-not (Test-Path 'frontend/dist/index.html')) {
    throw 'Build the frontend first: npm --prefix frontend ci; npm --prefix frontend run build'
}
.\.venv\Scripts\python.exe -m uvicorn transaction_audit.api:app --host 127.0.0.1 --port 8000
