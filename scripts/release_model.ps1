<#
.SYNOPSIS
Deploys a champion model version to the local Kubernetes cluster, verifies it, and rolls back on failure.

.DESCRIPTION
Called by .github/workflows/release-model.yml (on a self-hosted runner) when Databricks reports a new
champion; can also be run by hand. Steps:
  1. Refuse unless -Version is the current "champion" in the Databricks model registry.
  2. Download that version's ONNX file and build the image emitter-detector-api:v<Version>.
  3. Rolling update of the Deployment, then wait until every pod runs the new image.
  4. Verify through the Service: /model-info reports the version and /predict/test answers with it.
  5. If step 3 or 4 fails, roll back to the previous image and exit with an error.
Needs: the Databricks CLI profile, Docker, and kubectl pointed at the docker-desktop cluster.

.EXAMPLE
./scripts/release_model.ps1 -Version 2
#>
param(
    [Parameter(Mandatory = $true)][int]$Version,
    [string]$DatabricksProfile = "frequency-detector"
)

$ErrorActionPreference = "Stop"

$ModelName = "frequency_detector_8259555092219647.frequency_detector.emitter_detector"
$ModelsVolume = "/Volumes/frequency_detector_8259555092219647/frequency_detector/models"
$Namespace = "frequency-detector"
$Deployment = "emitter-detector-api"
$Container = "api"
$ApiUrl = "http://localhost:8000"
$Image = "emitter-detector-api:v$Version"
$Root = Split-Path $PSScriptRoot -Parent

# Windows PowerShell does not stop on a failing native command, so check every exit code.
function Invoke-Native([string]$What, [scriptblock]$Command) {
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "$What failed (exit code $LASTEXITCODE)" }
}

function Undo-Release([string]$Reason) {
    Write-Host "!! $Reason - rolling back"
    kubectl rollout undo "deployment/$Deployment" -n $Namespace
    kubectl rollout status "deployment/$Deployment" -n $Namespace --timeout=180s
    throw "Release of v$Version failed and was rolled back: $Reason"
}

Write-Host "== 1/5 Checking that v$Version is the champion"
$aliasJson = databricks model-versions get-by-alias $ModelName champion --profile $DatabricksProfile -o json
if ($LASTEXITCODE -ne 0) { throw "Could not read the champion alias of $ModelName" }
$champion = [int](($aliasJson | Out-String | ConvertFrom-Json).version)
if ($champion -ne $Version) { throw "v$Version is not the champion (champion is v$champion); refusing to deploy" }

Write-Host "== 2/5 Downloading the model and building $Image"
Invoke-Native "Model download" {
    databricks fs cp "dbfs:$ModelsVolume/emitter_detector_v$Version.onnx" "$Root/serving/model/model.onnx" --overwrite --profile $DatabricksProfile
}
Invoke-Native "Image build" { docker build --build-arg "MODEL_VERSION=$Version" -t $Image "$Root/serving" }

Write-Host "== 3/5 Rolling update"
$previous = kubectl get "deployment/$Deployment" -n $Namespace -o "jsonpath={.spec.template.spec.containers[0].image}"
if ($LASTEXITCODE -ne 0) { throw "Deployment $Deployment not found - run 'kubectl apply -f k8s/' first" }
Write-Host "   $previous -> $Image"
Invoke-Native "kubectl set image" { kubectl set image "deployment/$Deployment" -n $Namespace "$Container=$Image" }
kubectl rollout status "deployment/$Deployment" -n $Namespace --timeout=180s
if ($LASTEXITCODE -ne 0) { Undo-Release "pods did not become ready within 180s" }

Write-Host "== 4/5 Verifying through the Service"
try {
    $info = Invoke-RestMethod "$ApiUrl/model-info" -TimeoutSec 10
    if ("$($info.model_version)" -ne "$Version") { throw "/model-info reports v$($info.model_version)" }
    foreach ($i in 1..5) {
        $test = Invoke-RestMethod "$ApiUrl/predict/test" -TimeoutSec 10
        if ("$($test.prediction.model_version)" -ne "$Version") { throw "/predict/test answered with v$($test.prediction.model_version)" }
    }
}
catch {
    Undo-Release "smoke test failed: $($_.Exception.Message)"
}

Write-Host "== 5/5 Released v$Version (previously $previous)"
