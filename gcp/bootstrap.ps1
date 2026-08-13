$ErrorActionPreference = "Stop"

function Invoke-Gcloud {
    param(
        [Parameter(Mandatory = $true, ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )
    & gcloud @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "gcloud command failed with exit code ${LASTEXITCODE}: gcloud $($Arguments -join ' ')"
    }
}

if (-not $env:PROJECT_ID -or -not $env:BUCKET -or -not $env:REGION) {
    throw "Set PROJECT_ID, BUCKET, and REGION before running bootstrap.ps1."
}

$serviceAccount = "fraud-runtime"
$serviceAccountEmail = "$serviceAccount@$env:PROJECT_ID.iam.gserviceaccount.com"

Invoke-Gcloud config set project $env:PROJECT_ID
Invoke-Gcloud services enable storage.googleapis.com compute.googleapis.com logging.googleapis.com monitoring.googleapis.com

& gcloud storage buckets describe "gs://$env:BUCKET" 2>$null | Out-Null
$bucketExists = ($LASTEXITCODE -eq 0)
if (-not $bucketExists) {
    Invoke-Gcloud storage buckets create "gs://$env:BUCKET" --location=$env:REGION --uniform-bucket-level-access
}

& gcloud iam service-accounts describe $serviceAccountEmail --format="value(email)" 2>$null | Out-Null
$accountExists = ($LASTEXITCODE -eq 0)
if (-not $accountExists) {
    Invoke-Gcloud iam service-accounts create $serviceAccount --display-name="Fraud engine runtime"
}

# IAM service-account creation is eventually consistent. Wait until the
# account can be resolved before applying the bucket-level IAM binding.
$accountReady = $false
for ($attempt = 1; $attempt -le 12; $attempt++) {
    & gcloud iam service-accounts describe $serviceAccountEmail --format="value(email)" 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
        $accountReady = $true
        break
    }
    Start-Sleep -Seconds 5
}
if (-not $accountReady) {
    throw "Service account $serviceAccountEmail was not available after 60 seconds."
}

Invoke-Gcloud storage buckets add-iam-policy-binding "gs://$env:BUCKET" `
    --member="serviceAccount:$serviceAccountEmail" --role="roles/storage.objectViewer"
Invoke-Gcloud projects add-iam-policy-binding $env:PROJECT_ID `
    --member="serviceAccount:$serviceAccountEmail" --role="roles/logging.logWriter"
Invoke-Gcloud projects add-iam-policy-binding $env:PROJECT_ID `
    --member="serviceAccount:$serviceAccountEmail" --role="roles/monitoring.metricWriter"

Write-Output "GCP bootstrap complete. Runtime identity: $serviceAccountEmail"
