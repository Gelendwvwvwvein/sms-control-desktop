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
