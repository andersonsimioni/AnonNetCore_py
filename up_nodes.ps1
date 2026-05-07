param(
    [Parameter(Mandatory = $true, Position = 0)]
    [int]$NodeCount,

    [switch]$Detach
)

$ErrorActionPreference = "Stop"

if ($NodeCount -lt 2) {
    throw "Use pelo menos 2 nodes para manter os bootstraps fixos."
}

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$generatorScript = Join-Path $projectRoot "scripts\generate_docker_cluster.py"
$composeFile = Join-Path $projectRoot "docker\cluster\docker-compose.generated.yml"
$clusterStateRoot = Join-Path $projectRoot "docker\cluster\state"

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Python nao encontrado no PATH."
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker nao encontrado no PATH."
}

cmd /c "docker info >nul 2>nul"
$dockerInfoExitCode = $LASTEXITCODE

if ($dockerInfoExitCode -ne 0) {
    throw "O Docker Desktop/Linux Engine nao esta acessivel. Inicie o Docker Desktop e confirme que o contexto 'desktop-linux' esta disponivel."
}

Write-Host "Gerando cluster com $NodeCount nodes..."
& python $generatorScript --nodes $NodeCount

Write-Host "Limpando bancos e logs locais do cluster..."
if (Test-Path $clusterStateRoot) {
    Get-ChildItem -Path $clusterStateRoot -Directory -Filter "node-*" | ForEach-Object {
        $databaseFile = Join-Path $_.FullName "anonnetcore.db"
        $logsDirectory = Join-Path $_.FullName "logs"

        if (Test-Path $databaseFile) {
            Remove-Item -LiteralPath $databaseFile -Force
        }

        if (Test-Path $logsDirectory) {
            Get-ChildItem -Path $logsDirectory -File | Remove-Item -Force
        }
    }
}

$dockerArgs = @(
    "compose"
    "-f"
    $composeFile
    "up"
    "--build"
)

if ($Detach.IsPresent) {
    $dockerArgs += "-d"
}

Write-Host "Subindo containers..."
& docker @dockerArgs
