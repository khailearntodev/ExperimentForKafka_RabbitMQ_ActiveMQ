param(
    [ValidateSet("rabbitmq", "activemq", "kafka", "sync")]
    [string]$Broker = "rabbitmq",
    
    [ValidateSet("test1", "test2", "test3", "all")]
    [string]$Scenario = "test1",
    
    [int]$Runs = 3
)

$rootDir = (Get-Item $PSScriptRoot).Parent.Parent.FullName
$resultsDir = Join-Path $rootDir "results"
$k6Dir = Join-Path $rootDir "benchmarks\k6"
$runK6Script = Join-Path $PSScriptRoot "run-k6.ps1"

Write-Host "==========================================================" -ForegroundColor Green
Write-Host " TECHLAB MQ EXPERIMENT AUTOMATION RUNNER " -ForegroundColor Green
Write-Host " Target Broker : $Broker" -ForegroundColor Green
Write-Host " Scenario      : $Scenario" -ForegroundColor Green
Write-Host " Repetitions   : $Runs runs" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green

# 1. Health check Order Service
function Test-OrderServiceHealth() {
    $healthUrl = "http://localhost:3000/health"
    try {
        $res = Invoke-RestMethod -Uri $healthUrl -TimeoutSec 3 -ErrorAction Stop
        Write-Host "[HealthCheck] Order Service is UP! (Broker: $($res.broker))" -ForegroundColor Green
        return $true
    } catch {
        Write-Warning "[HealthCheck] Order Service is NOT responding at $healthUrl. Ensure 'npm run start:order' is running."
        return $false
    }
}

# 2. Reset Worker metrics
function Reset-WorkerMetrics() {
    $resetUrl = "http://localhost:3002/reset"
    try {
        Invoke-RestMethod -Uri $resetUrl -Method Get -TimeoutSec 2 -ErrorAction SilentlyContinue | Out-Null
        Write-Host "[Worker] Metrics reset successfully." -ForegroundColor Gray
    } catch {}
}

# 3. Fetch Worker metrics
function Get-WorkerMetrics() {
    $metricsUrl = "http://localhost:3002/metrics"
    try {
        return Invoke-RestMethod -Uri $metricsUrl -Method Get -TimeoutSec 3 -ErrorAction Stop
    } catch {
        return $null
    }
}

function Append-ResultToCsv($targetCsvName, $result, $workerStats = $null) {
    $csvPath = Join-Path $resultsDir $targetCsvName
    $retrySuccess = $result.RetrySuccessRate
    if ($workerStats -ne $null -and $workerStats.successRate -ne $null) {
        $retrySuccess = $workerStats.successRate
    }
    
    $line = "$($result.Broker),$($result.Scenario),$($result.Run),$($result.AvgLatencyMs),$($result.P95LatencyMs),$($result.Throughput),$($result.ErrorRate),$($retrySuccess)"
    Add-Content -Path $csvPath -Value $line
    Write-Host "[CSV] Appended row to ${targetCsvName}: $line" -ForegroundColor Yellow
}

function Execute-Scenario($scName, $k6File, $targetCsv) {
    Write-Host "`n----------------------------------------------------------" -ForegroundColor Cyan
    Write-Host "Starting Scenario: $scName ($Runs iterations for $Broker)" -ForegroundColor Cyan
    Write-Host "----------------------------------------------------------" -ForegroundColor Cyan
    
    for ($i = 1; $i -le $Runs; $i++) {
        Write-Host "`n>>> Iteration $i of $Runs for $Broker - $scName..." -ForegroundColor Magenta
        
        # Reset worker before test
        Reset-WorkerMetrics
        
        $scriptPath = Join-Path $k6Dir $k6File
        $result = & $runK6Script -ScriptPath $scriptPath -ScenarioName $scName -Broker $Broker -RunIndex $i
        
        $workerStats = $null
        if ($scName -eq "test3") {
            # Allow 2 seconds for worker to finalize all in-flight retries
            Start-Sleep -Seconds 2
            $workerStats = Get-WorkerMetrics
            if ($workerStats -ne $null) {
                Write-Host ">>> [Worker Metrics] Received: $($workerStats.totalReceived) | Success: $($workerStats.successfulPayments) | Retries: $($workerStats.retryCount) | Lost: $($workerStats.lostMessages) | SuccessRate: $($workerStats.successRate)%" -ForegroundColor Cyan
            }
        }
        
        if ($result -ne $null) {
            Append-ResultToCsv -targetCsvName $targetCsv -result $result -workerStats $workerStats
        } else {
            Write-Error "Failed to record result for iteration $i"
        }
        
        # Cooldown between runs
        if ($i -lt $Runs) {
            Write-Host "Cooling down 3s before next iteration..." -ForegroundColor Gray
            Start-Sleep -Seconds 3
        }
    }
}

# Run health check
$isHealthy = Test-OrderServiceHealth
if (-not $isHealthy) {
    Write-Warning "Proceeding anyway in case services take another second to bind..."
}

# Scenario routing
if ($Scenario -eq "test1" -or $Scenario -eq "all") {
    Execute-Scenario -scName "test1" -k6File "test1-normal.js" -targetCsv "normal.csv"
}

if ($Scenario -eq "test2" -or $Scenario -eq "all") {
    Execute-Scenario -scName "test2" -k6File "test2-slow-payment.js" -targetCsv "slow-payment.csv"
}

if ($Scenario -eq "test3" -or $Scenario -eq "all") {
    Execute-Scenario -scName "test3" -k6File "test3-retry.js" -targetCsv "retry.csv"
}

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host " Benchmark run for $Broker ($Scenario) completed!" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
