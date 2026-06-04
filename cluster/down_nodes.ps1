$ErrorActionPreference = "Stop"

$clusterRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $clusterRoot
$composeFile = Join-Path $clusterRoot "docker-compose.generated.yml"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker not found in PATH."
}

if (-not (Test-Path $composeFile)) {
    throw "Generated compose file not found at: $composeFile"
}

& docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker Desktop/Linux Engine is not accessible. Start Docker Desktop and confirm the 'desktop-linux' context is available."
}

Write-Host "Stopping cluster containers..."
Push-Location $projectRoot
try {
    & docker compose -f $composeFile down
}
finally {
    Pop-Location
}
