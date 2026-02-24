param(
    [string]$Configuration = "Release",
    [string]$Runtime = "win-x64"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$projectPath = Join-Path $repoRoot "src\Collector\Collector.csproj"
$publishDir = Join-Path $repoRoot "out\publish\$Runtime"
$issPath = Join-Path $scriptDir "SmsControlSetup.iss"
$collectorExe = Join-Path $publishDir "Collector.exe"
$bundledPlaywrightDir = Join-Path $publishDir "ms-playwright"
$playwrightScriptRid = Join-Path $repoRoot "src\Collector\bin\$Configuration\net8.0\$Runtime\playwright.ps1"
$playwrightScriptDefault = Join-Path $repoRoot "src\Collector\bin\$Configuration\net8.0\playwright.ps1"

Write-Host "Publishing application..."
dotnet publish $projectPath `
  -c $Configuration `
  -r $Runtime `
  --self-contained true `
  -p:PublishSingleFile=true `
  -p:IncludeNativeLibrariesForSelfExtract=true `
  -p:DebugType=None `
  -o $publishDir

if (!(Test-Path $publishDir)) {
    throw "Publish folder not found: $publishDir"
}

if (!(Test-Path $collectorExe)) {
    throw "Collector.exe not found after publish: $collectorExe"
}

if (Test-Path $bundledPlaywrightDir) {
    Remove-Item -Path $bundledPlaywrightDir -Recurse -Force
}

Write-Host "Installing Playwright Chromium into publish folder..."
$prevPlaywrightPath = $env:PLAYWRIGHT_BROWSERS_PATH
$env:PLAYWRIGHT_BROWSERS_PATH = $bundledPlaywrightDir
try {
    $playwrightInstalled = $false

    $playwrightScripts = @($playwrightScriptRid, $playwrightScriptDefault) | Select-Object -Unique
    foreach ($playwrightScript in $playwrightScripts) {
        if (!(Test-Path $playwrightScript)) {
            continue
        }

        Write-Host "Using Playwright script: $playwrightScript"
        & $playwrightScript install chromium
        if ($LASTEXITCODE -eq 0) {
            $playwrightInstalled = $true
            break
        }

        Write-Warning "Playwright script failed with exit code $LASTEXITCODE"
    }

    if (-not $playwrightInstalled) {
        Write-Host "Falling back to Collector.exe --install-playwright"
        & $collectorExe --install-playwright
        if ($LASTEXITCODE -eq 0) {
            $playwrightInstalled = $true
        }
    }

    if (-not $playwrightInstalled) {
        throw "Playwright install failed"
    }
}
finally {
    if ([string]::IsNullOrWhiteSpace($prevPlaywrightPath)) {
        Remove-Item Env:PLAYWRIGHT_BROWSERS_PATH -ErrorAction SilentlyContinue
    }
    else {
        $env:PLAYWRIGHT_BROWSERS_PATH = $prevPlaywrightPath
    }
}

$chromiumFolder = Get-ChildItem -Path $bundledPlaywrightDir -Directory -Filter "chromium-*" -ErrorAction SilentlyContinue | Select-Object -First 1
if ($null -eq $chromiumFolder) {
    throw "Bundled Playwright Chromium not found in: $bundledPlaywrightDir"
}

$iscc = (Get-Command "ISCC.exe" -ErrorAction SilentlyContinue)?.Source
if ([string]::IsNullOrWhiteSpace($iscc)) {
    $defaultIscc = "C:\Program Files (x86)\Inno Setup 6\ISCC.exe"
    if (Test-Path $defaultIscc) {
        $iscc = $defaultIscc
    }
}

if ([string]::IsNullOrWhiteSpace($iscc) -or !(Test-Path $iscc)) {
    throw "ISCC.exe not found. Install Inno Setup 6 and rerun script."
}

Write-Host "Building installer..."
& $iscc $issPath

$installerDir = Join-Path $repoRoot "out\installer"
Write-Host "Done. Installer output: $installerDir"
