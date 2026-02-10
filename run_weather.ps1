param(
    [string]$TimescaleHost = "127.0.0.1",
    [string]$TimescalePort = "5432",
    [string]$TimescaleDb = "air_quality",
    [string]$TimescaleUser = "air_quality",
    [string]$TimescalePassword = "air_quality",
    [ValidateSet("latest", "earliest")]
    [string]$StartingOffsets = "latest",
    [int]$ShufflePartitions = 8,
    [ValidateSet("stable", "run_id")]
    [string]$CheckpointMode = "stable",
    [string]$LakehouseRoot = "hdfs://namenode:8020/lakehouse",
    [string]$HdfsDefaultFs = "hdfs://namenode:8020",
    [string]$HiveMetastoreUri = "thrift://hive-metastore:9083",
    [string]$SparkSqlWarehouseDir = "hdfs://namenode:8020/warehouse"
)

$ErrorActionPreference = "Stop"

$sparkSubmit = Get-Command spark-submit -ErrorAction SilentlyContinue
if (-not $sparkSubmit) {
    throw "spark-submit introuvable dans PATH."
}

function Test-UriPath {
    param([string]$PathValue)
    return $PathValue -match "^[a-zA-Z][a-zA-Z0-9+.-]*://"
}

function Join-StoragePath {
    param(
        [string]$Root,
        [string]$Child
    )

    if (Test-UriPath $Root) {
        $normalizedChild = $Child.Replace("\\", "/").TrimStart("/")
        return "$($Root.TrimEnd('/'))/$normalizedChild"
    }

    return Join-Path $Root ($Child.Replace("/", "\\"))
}

if ($CheckpointMode -eq "run_id") {
    $runId = Get-Date -Format "yyyyMMdd_HHmmss"
    $root = Join-Path $PSScriptRoot "streaming\checkpoints\weather_run_$runId"
}
else {
    $runId = "stable"
    $root = Join-Path $PSScriptRoot "streaming\checkpoints\weather"
}

$weatherBronzeCheckpointDir = Join-Path $root "bronze"
$weatherCheckpointDir = Join-Path $root "indicators"
$weatherSilverCheckpointDir = Join-Path $root "silver"
$weatherTimescaleCheckpointDir = Join-Path $root "timescale"
$weatherGoldCheckpointDir = Join-Path $root "gold"
$weatherRejectsCheckpointDir = Join-Path $root "rejects"

$lakehouseRoot = $LakehouseRoot
$weatherBronzePath = Join-StoragePath $lakehouseRoot "bronze/weather"
$weatherSilverPath = Join-StoragePath $lakehouseRoot "silver/weather"
$weatherGoldPath = Join-StoragePath $lakehouseRoot "gold/weather_aggregates"
$weatherRejectsPath = Join-StoragePath $lakehouseRoot "rejects/weather"
$weatherIvyCacheDir = Join-Path $root "ivy_cache"
$sparkScriptPath = Join-Path $PSScriptRoot "streaming/spark_weather_stream.py"

New-Item -ItemType Directory -Force -Path @(
    $weatherBronzeCheckpointDir,
    $weatherCheckpointDir,
    $weatherSilverCheckpointDir,
    $weatherTimescaleCheckpointDir,
    $weatherGoldCheckpointDir,
    $weatherRejectsCheckpointDir,
    $weatherIvyCacheDir
) | Out-Null

$localLakehouseDirs = @(
    $weatherBronzePath,
    $weatherSilverPath,
    $weatherGoldPath,
    $weatherRejectsPath
) | Where-Object { -not (Test-UriPath $_) }

if ($localLakehouseDirs.Count -gt 0) {
    New-Item -ItemType Directory -Force -Path $localLakehouseDirs | Out-Null
}

$env:WEATHER_BRONZE_CHECKPOINT_DIR = $weatherBronzeCheckpointDir
$env:WEATHER_CHECKPOINT_DIR = $weatherCheckpointDir
$env:WEATHER_SILVER_CHECKPOINT_DIR = $weatherSilverCheckpointDir
$env:WEATHER_TIMESCALE_CHECKPOINT_DIR = $weatherTimescaleCheckpointDir
$env:WEATHER_GOLD_CHECKPOINT_DIR = $weatherGoldCheckpointDir
$env:WEATHER_REJECTS_CHECKPOINT_DIR = $weatherRejectsCheckpointDir

$env:WEATHER_BRONZE_PATH = $weatherBronzePath
$env:WEATHER_SILVER_PATH = $weatherSilverPath
$env:WEATHER_GOLD_PATH = $weatherGoldPath
$env:WEATHER_REJECTS_PATH = $weatherRejectsPath

$env:LAKEHOUSE_ROOT = $lakehouseRoot
$env:HDFS_DEFAULT_FS = $HdfsDefaultFs
$env:HIVE_METASTORE_URI = $HiveMetastoreUri
$env:SPARK_SQL_WAREHOUSE_DIR = $SparkSqlWarehouseDir

$env:TIMESCALE_HOST = $TimescaleHost
$env:TIMESCALE_PORT = $TimescalePort
$env:TIMESCALE_DB = $TimescaleDb
$env:TIMESCALE_USER = $TimescaleUser
$env:TIMESCALE_PASSWORD = $TimescalePassword
$env:STARTING_OFFSETS = $StartingOffsets

Remove-Item Env:TIMESCALE_JDBC_URL -ErrorAction SilentlyContinue

$packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,org.postgresql:postgresql:42.7.4"
$sparkArgs = @(
    "--conf", "spark.sql.shuffle.partitions=$ShufflePartitions",
    "--conf", "spark.sql.session.timeZone=UTC",
    "--conf", "spark.sql.streaming.stopGracefullyOnShutdown=true",
    "--conf", "spark.hadoop.fs.defaultFS=$HdfsDefaultFs",
    "--conf", "spark.sql.warehouse.dir=$SparkSqlWarehouseDir",
    "--conf", "spark.hadoop.hive.metastore.uris=$HiveMetastoreUri",
    "--conf", "hive.metastore.uris=$HiveMetastoreUri",
    "--conf", "spark.sql.catalogImplementation=hive",
    "--conf", "spark.jars.ivy=$weatherIvyCacheDir",
    "--packages", $packages,
    $sparkScriptPath
)

Write-Host "Run ID: $runId"
Write-Host "Checkpoint Mode: $CheckpointMode"
Write-Host "Checkpoint Root: $root"
Write-Host "Spark Ivy Cache: $weatherIvyCacheDir"
Write-Host "LAKEHOUSE_ROOT=$lakehouseRoot"
Write-Host "HDFS_DEFAULT_FS=$HdfsDefaultFs"
Write-Host "HIVE_METASTORE_URI=$HiveMetastoreUri"
Write-Host "SPARK_SQL_WAREHOUSE_DIR=$SparkSqlWarehouseDir"
Write-Host "WEATHER_BRONZE_CHECKPOINT_DIR=$weatherBronzeCheckpointDir"
Write-Host "WEATHER_CHECKPOINT_DIR=$weatherCheckpointDir"
Write-Host "WEATHER_SILVER_CHECKPOINT_DIR=$weatherSilverCheckpointDir"
Write-Host "WEATHER_TIMESCALE_CHECKPOINT_DIR=$weatherTimescaleCheckpointDir"
Write-Host "WEATHER_GOLD_CHECKPOINT_DIR=$weatherGoldCheckpointDir"
Write-Host "WEATHER_REJECTS_CHECKPOINT_DIR=$weatherRejectsCheckpointDir"
Write-Host "WEATHER_BRONZE_PATH=$weatherBronzePath"
Write-Host "WEATHER_SILVER_PATH=$weatherSilverPath"
Write-Host "WEATHER_GOLD_PATH=$weatherGoldPath"
Write-Host "WEATHER_REJECTS_PATH=$weatherRejectsPath"

& spark-submit @sparkArgs
exit $LASTEXITCODE
