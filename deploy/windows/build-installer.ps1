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
    & $collectorExe --install-playwright
    if ($LASTEXITCODE -ne 0) {
        throw "Playwright install failed with exit code $LASTEXITCODE"
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
