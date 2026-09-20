param(
    [Parameter(Mandatory=$true)]
    [string]$ScriptPath,
    [string]$ScenarioName = "test1",
    [string]$Broker = "rabbitmq",
    [int]$RunIndex = 1
)

$rootDir = (Get-Item $PSScriptRoot).Parent.Parent.FullName
$resultsDir = Join-Path $rootDir "results"
$rawDir = Join-Path $resultsDir "raw"

if (-not (Test-Path $rawDir)) {
    New-Item -ItemType Directory -Path $rawDir -Force | Out-Null
}

$tempSummaryName = "summary_${Broker}_${ScenarioName}_run${RunIndex}.json"
$tempSummaryHostPath = Join-Path $rawDir $tempSummaryName
$tempSummaryContainerPath = "/results/raw/$tempSummaryName"

Write-Host "`n>>> [k6-runner] Executing $ScriptPath (Broker: $Broker, Scenario: $ScenarioName, Run: $RunIndex)..." -ForegroundColor Cyan

# Use absolute path for Docker volume mounting
$resultsMount = $resultsDir.Replace("\", "/")

# Pipe the test script into docker run grafana/k6
Get-Content $ScriptPath | docker run --rm -i --add-host=host.docker.internal:host-gateway -v "${resultsMount}:/results" grafana/k6:latest run --summary-export "$tempSummaryContainerPath" -

if (Test-Path $tempSummaryHostPath) {
    try {
        $json = Get-Content $tempSummaryHostPath -Raw | ConvertFrom-Json
        $metrics = $json.metrics

        $avgVal = if ($metrics.http_req_duration.avg -ne $null) { $metrics.http_req_duration.avg } else { $metrics.http_req_duration.values.avg }
        $p95Val = if ($metrics.http_req_duration.'p(95)' -ne $null) { $metrics.http_req_duration.'p(95)' } else { $metrics.http_req_duration.values.'p(95)' }
        $rateVal = if ($metrics.http_reqs.rate -ne $null) { $metrics.http_reqs.rate } else { $metrics.http_reqs.values.rate }
        $failVal = if ($metrics.http_req_failed.value -ne $null) { $metrics.http_req_failed.value } else { $metrics.http_req_failed.values.rate }

        $avgLatency = [Math]::Round([double]$avgVal, 2)
        $p95Latency = [Math]::Round([double]$p95Val, 2)
        $throughput = [Math]::Round([double]$rateVal, 2)
        $errorRate = [Math]::Round(([double]$failVal) * 100, 2)

        $resultObj = [PSCustomObject]@{
            Broker = $Broker
            Scenario = $ScenarioName
            Run = $RunIndex
            AvgLatencyMs = $avgLatency
            P95LatencyMs = $p95Latency
            Throughput = $throughput
            ErrorRate = $errorRate
            RetrySuccessRate = if ($errorRate -eq 0) { 100.0 } else { [Math]::Round(100.0 - $errorRate, 2) }
        }

        Write-Host ">>> [k6-runner] Results parsed successfully:" -ForegroundColor Green
        Write-Host "    Avg: $($avgLatency)ms | P95: $($p95Latency)ms | Throughput: $($throughput) req/s | Error: $($errorRate)%" -ForegroundColor Green

        return $resultObj
    } catch {
        Write-Error "Failed to parse k6 summary JSON: $_"
        return $null
    }
} else {
    Write-Warning "Summary file not found at $tempSummaryHostPath"
    return $null
}
