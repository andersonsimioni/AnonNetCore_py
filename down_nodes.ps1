$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$composeFile = Join-Path $projectRoot "docker\cluster\docker-compose.generated.yml"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker nao encontrado no PATH."
}

if (-not (Test-Path $composeFile)) {
    throw "Compose gerado nao encontrado em: $composeFile"
}

cmd /c "docker info >nul 2>nul"
$dockerInfoExitCode = $LASTEXITCODE

if ($dockerInfoExitCode -ne 0) {
    throw "O Docker Desktop/Linux Engine nao esta acessivel. Inicie o Docker Desktop e confirme que o contexto 'desktop-linux' esta disponivel."
}

Write-Host "Derrubando containers do cluster..."
& docker compose -f $composeFile down
