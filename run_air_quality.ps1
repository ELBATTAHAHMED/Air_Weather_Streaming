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
    $root = Join-Path $PSScriptRoot "streaming\checkpoints\air_quality_run_$runId"
}
else {
    $runId = "stable"
    $root = Join-Path $PSScriptRoot "streaming\checkpoints\air_quality"
}

$airQualityBronzeCheckpointDir = Join-Path $root "bronze"
$airQualityCheckpointDir = Join-Path $root "console"
$airQualityTimescaleCheckpointDir = Join-Path $root "timescale"
$airQualityPeaksCheckpointDir = Join-Path $root "peaks"
$airQualitySilverCheckpointDir = Join-Path $root "silver"
$airQualityGoldCheckpointDir = Join-Path $root "gold"
$airQualityRejectsCheckpointDir = Join-Path $root "rejects"

$lakehouseRoot = $LakehouseRoot
$airQualityBronzePath = Join-StoragePath $lakehouseRoot "bronze/air_quality"
$airQualitySilverPath = Join-StoragePath $lakehouseRoot "silver/air_quality"
$airQualityGoldPath = Join-StoragePath $lakehouseRoot "gold/air_quality_aggregates"
$airQualityRejectsPath = Join-StoragePath $lakehouseRoot "rejects/air_quality"
$peaksStatePath = Join-Path $root "peaks_state_store"
$peaksOutputPath = Join-StoragePath $airQualityGoldPath "peaks"
$airQualityIvyCacheDir = Join-Path $root "ivy_cache"
$sparkScriptPath = Join-Path $PSScriptRoot "streaming/spark_air_quality_stream.py"

New-Item -ItemType Directory -Force -Path @(
    $airQualityBronzeCheckpointDir,
    $airQualityCheckpointDir,
    $airQualityTimescaleCheckpointDir,
    $airQualityPeaksCheckpointDir,
    $airQualitySilverCheckpointDir,
    $airQualityGoldCheckpointDir,
    $airQualityRejectsCheckpointDir,
    $peaksStatePath,
    $airQualityIvyCacheDir
) | Out-Null

$localLakehouseDirs = @(
    $airQualityBronzePath,
    $airQualitySilverPath,
    $airQualityGoldPath,
    $airQualityRejectsPath,
    $peaksOutputPath
) | Where-Object { -not (Test-UriPath $_) }

if ($localLakehouseDirs.Count -gt 0) {
    New-Item -ItemType Directory -Force -Path $localLakehouseDirs | Out-Null
}

$env:AIR_QUALITY_BRONZE_CHECKPOINT_DIR = $airQualityBronzeCheckpointDir
$env:AIR_QUALITY_CHECKPOINT_DIR = $airQualityCheckpointDir
$env:AIR_QUALITY_TIMESCALE_CHECKPOINT_DIR = $airQualityTimescaleCheckpointDir
$env:AIR_QUALITY_PEAKS_CHECKPOINT_DIR = $airQualityPeaksCheckpointDir
$env:AIR_QUALITY_SILVER_CHECKPOINT_DIR = $airQualitySilverCheckpointDir
$env:AIR_QUALITY_GOLD_CHECKPOINT_DIR = $airQualityGoldCheckpointDir
$env:AIR_QUALITY_REJECTS_CHECKPOINT_DIR = $airQualityRejectsCheckpointDir

$env:AIR_QUALITY_BRONZE_PATH = $airQualityBronzePath
$env:AIR_QUALITY_SILVER_PATH = $airQualitySilverPath
$env:AIR_QUALITY_GOLD_PATH = $airQualityGoldPath
$env:AIR_QUALITY_REJECTS_PATH = $airQualityRejectsPath
$env:PEAKS_STATE_PATH = $peaksStatePath
$env:PEAKS_OUTPUT_PATH = $peaksOutputPath

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
    "--conf", "spark.jars.ivy=$airQualityIvyCacheDir",
    "--packages", $packages,
    $sparkScriptPath
)

Write-Host "Run ID: $runId"
Write-Host "Checkpoint Mode: $CheckpointMode"
Write-Host "Checkpoint Root: $root"
Write-Host "Spark Ivy Cache: $airQualityIvyCacheDir"
Write-Host "LAKEHOUSE_ROOT=$lakehouseRoot"
Write-Host "HDFS_DEFAULT_FS=$HdfsDefaultFs"
Write-Host "HIVE_METASTORE_URI=$HiveMetastoreUri"
Write-Host "SPARK_SQL_WAREHOUSE_DIR=$SparkSqlWarehouseDir"
Write-Host "AIR_QUALITY_BRONZE_CHECKPOINT_DIR=$airQualityBronzeCheckpointDir"
Write-Host "AIR_QUALITY_CHECKPOINT_DIR=$airQualityCheckpointDir"
Write-Host "AIR_QUALITY_TIMESCALE_CHECKPOINT_DIR=$airQualityTimescaleCheckpointDir"
Write-Host "AIR_QUALITY_PEAKS_CHECKPOINT_DIR=$airQualityPeaksCheckpointDir"
Write-Host "AIR_QUALITY_SILVER_CHECKPOINT_DIR=$airQualitySilverCheckpointDir"
Write-Host "AIR_QUALITY_GOLD_CHECKPOINT_DIR=$airQualityGoldCheckpointDir"
Write-Host "AIR_QUALITY_REJECTS_CHECKPOINT_DIR=$airQualityRejectsCheckpointDir"
Write-Host "AIR_QUALITY_BRONZE_PATH=$airQualityBronzePath"
Write-Host "AIR_QUALITY_SILVER_PATH=$airQualitySilverPath"
Write-Host "AIR_QUALITY_GOLD_PATH=$airQualityGoldPath"
Write-Host "AIR_QUALITY_REJECTS_PATH=$airQualityRejectsPath"
Write-Host "PEAKS_OUTPUT_PATH=$peaksOutputPath"

& spark-submit @sparkArgs
exit $LASTEXITCODE
