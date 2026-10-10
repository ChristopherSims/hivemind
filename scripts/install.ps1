param([switch]$CheckOnly, [switch]$GitOnly)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
if (-not (Get-Command git.exe -ErrorAction SilentlyContinue)) {
    if ($CheckOnly) { throw 'Git is missing. Run the installer without -CheckOnly to install Git.' }
    if (-not (Get-Command winget.exe -ErrorAction SilentlyContinue)) {
        throw 'Git is missing and winget is unavailable. Install standard Git for Windows, then rerun.'
    }
    & winget.exe install --id Git.Git --exact --source winget --silent --accept-package-agreements --accept-source-agreements --disable-interactivity
    if ($LASTEXITCODE -ne 0) { throw 'Standard Git installation failed.' }
    $locations = @(
        (Join-Path $env:ProgramFiles 'Git\cmd'),
        (Join-Path $env:LOCALAPPDATA 'Programs\Git\cmd')
    )
    foreach ($location in $locations) {
        if (Test-Path (Join-Path $location 'git.exe')) { $env:PATH = "$location;$env:PATH" }
    }
}
$git = (Get-Command git.exe -ErrorAction Stop).Source
$version = & $git --version
if ($LASTEXITCODE -ne 0 -or $version -notmatch 'git version (\d+)\.(\d+)') { throw 'Cannot verify Git.' }
if ([int]$Matches[1] -lt 2 -or ([int]$Matches[1] -eq 2 -and [int]$Matches[2] -lt 38)) {
    throw 'Git 2.38 or newer is required. Upgrade standard Git and rerun.'
}
Write-Output $version
if ($CheckOnly -or $GitOnly) { return }
$uv = (Get-Command uv -ErrorAction SilentlyContinue).Source
if (-not $uv) { throw 'Git is ready. Install uv, then rerun to install the service dependencies.' }
& $uv sync --locked --extra embedding --extra mcp --extra runtimes --project $root
if ($LASTEXITCODE -ne 0) { throw 'Service dependency installation failed.' }
Write-Output 'Installed. Configure your existing Git author identity, then initialize Hivemind. No model was downloaded and no service was exposed.'
