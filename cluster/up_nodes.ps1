param(
    [Parameter(Mandatory = $true, Position = 0)]
    [int]$NodeCount,

    [switch]$Detach
)

$ErrorActionPreference = "Stop"

if ($NodeCount -lt 2) {
    throw "Use at least 2 nodes to keep fixed bootstrap nodes."
}

$clusterRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $clusterRoot
$generatorScript = Join-Path $clusterRoot "generate_docker_cluster.py"
$composeFile = Join-Path $clusterRoot "docker-compose.generated.yml"
$clusterStateRoot = Join-Path $clusterRoot "state"

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Python not found in PATH."
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker not found in PATH."
}

& docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker Desktop/Linux Engine is not accessible. Start Docker Desktop and confirm the 'desktop-linux' context is available."
}

Write-Host "Generating cluster with $NodeCount nodes..."
Push-Location $projectRoot
try {
    & python $generatorScript --nodes $NodeCount --output-dir $clusterRoot
}
finally {
    Pop-Location
}

Write-Host "Cleaning local cluster databases and logs..."
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

Write-Host "Starting containers..."
Push-Location $projectRoot
try {
    & docker @dockerArgs
}
finally {
    Pop-Location
}
