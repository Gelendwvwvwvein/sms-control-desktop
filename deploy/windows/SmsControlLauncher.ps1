$ErrorActionPreference = "SilentlyContinue"

$collectorPath = Join-Path $PSScriptRoot "Collector.exe"
$healthUrl = "http://127.0.0.1:5057/health"
$uiUrl = "http://127.0.0.1:5057/"
$isBackendUp = $false

if (Test-Path $collectorPath) {
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $healthUrl -TimeoutSec 2
        if ($response.StatusCode -eq 200) {
            $isBackendUp = $true
        }
    } catch {
        $isBackendUp = $false
    }

    if (-not $isBackendUp) {
        Start-Process -FilePath $collectorPath -ArgumentList "--serve --port 5057" -WindowStyle Hidden
        Start-Sleep -Milliseconds 1200
    }
}

Start-Process $uiUrl
