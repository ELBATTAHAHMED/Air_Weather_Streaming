param(
    [string]$ComposeFile = ".\\docker\\docker-compose.superset.yml",
    [string]$ContainerName = "superset",
    [string]$ContainerExportPath = "/tmp/air_quality_weather_bundle.zip",
    [string]$OutputPath = ".\\superset\\exports\\air_quality_weather_bundle.zip"
)

$ErrorActionPreference = "Stop"

Write-Host "[1/3] Exporting dashboards from Superset container..."
docker compose -f $ComposeFile exec -T $ContainerName superset export-dashboards -f $ContainerExportPath

Write-Host "[2/3] Copying bundle to repository..."
docker cp "$($ContainerName):$ContainerExportPath" $OutputPath

Write-Host "[3/3] Cleaning temporary file in container..."
docker compose -f $ComposeFile exec -T $ContainerName rm -f $ContainerExportPath

Write-Host "Superset bundle exported to: $OutputPath"
