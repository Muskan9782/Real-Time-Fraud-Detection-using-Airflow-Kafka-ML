$ErrorActionPreference = "Stop"

if (-not $env:PROJECT_ID -or -not $env:REGION -or -not $env:REPO_URL) {
    throw "Set PROJECT_ID, REGION, and REPO_URL before deploying."
}
$zone = if ($env:ZONE) { $env:ZONE } else { "$env:REGION-a" }
$env:ZONE = $zone
$serviceAccountEmail = "fraud-runtime@$env:PROJECT_ID.iam.gserviceaccount.com"

gcloud config set project $env:PROJECT_ID
gcloud compute instances create fraud-engine-vm `
    --zone=$zone `
    --machine-type=e2-standard-4 `
    --image-family=ubuntu-2204-lts `
    --image-project=ubuntu-os-cloud `
    --boot-disk-size=50GB `
    --service-account=$serviceAccountEmail `
    --scopes=https://www.googleapis.com/auth/cloud-platform `
    --tags=fraud-engine `
    --metadata=REPO_URL=$env:REPO_URL `
    --metadata-from-file=startup-script=gcp\startup.sh

gcloud compute firewall-rules create fraud-engine-airflow `
    --allow=tcp:8080 `
    --target-tags=fraud-engine `
    --description="Temporary Airflow demo access; delete after the demo"

Write-Output "VM created in $zone. Check startup progress with:"
Write-Output "gcloud compute ssh fraud-engine-vm --zone=$zone --command='sudo docker compose -f /opt/fraud-engine/docker-compose.yml ps'"
