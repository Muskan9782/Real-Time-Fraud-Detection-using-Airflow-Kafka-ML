$ErrorActionPreference = "Stop"

if (-not $env:BUCKET) {
    throw "Set BUCKET before running sync_gcs.ps1."
}
if (-not (Test-Path -LiteralPath "data\lake")) {
    throw "data\lake is missing. Run the Phase 15/18 backfill first."
}
if (-not (Test-Path -LiteralPath "data\dashboard")) {
    throw "data\dashboard is missing. Run dashboard\export_powerbi.py first."
}

gcloud storage rsync --recursive "data\lake" "gs://$env:BUCKET/lake"
gcloud storage rsync --recursive "data\dashboard" "gs://$env:BUCKET/dashboard"
Write-Output "Uploaded Delta lake and Power BI extracts to gs://$env:BUCKET"
