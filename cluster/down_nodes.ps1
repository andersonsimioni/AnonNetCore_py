$ErrorActionPreference = "Stop"

$clusterRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $clusterRoot
$composeFile = Join-Path $clusterRoot "docker-compose.generated.yml"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker nao encontrado no PATH."
}

if (-not (Test-Path $composeFile)) {
    throw "Compose gerado nao encontrado em: $composeFile"
}

& docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw "O Docker Desktop/Linux Engine nao esta acessivel. Inicie o Docker Desktop e confirme que o contexto 'desktop-linux' esta disponivel."
}

Write-Host "Derrubando containers do cluster..."
Push-Location $projectRoot
try {
    & docker compose -f $composeFile down
}
finally {
    Pop-Location
}
