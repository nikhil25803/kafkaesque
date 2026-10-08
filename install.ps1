[CmdletBinding()]
param(
    [string]$Version = "latest",
    [string]$InstallDir = (Join-Path $env:LOCALAPPDATA "Programs\kafkaesque")
)

$ErrorActionPreference = "Stop"
$repository = "nikhil25803/kafkaesque"

$architecture = switch ($env:PROCESSOR_ARCHITECTURE.ToLowerInvariant()) {
    "amd64" { "amd64" }
    "arm64" { "arm64" }
    default { throw "Unsupported architecture: $env:PROCESSOR_ARCHITECTURE" }
}

if ($Version -eq "latest") {
    $releaseUrl = "https://github.com/$repository/releases/latest/download"
}
else {
    if (-not $Version.StartsWith("v")) {
        $Version = "v$Version"
    }
    $releaseUrl = "https://github.com/$repository/releases/download/$Version"
}

$archiveName = "kafkaesque_windows_$architecture.zip"
$temporaryDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "kafkaesque-$([guid]::NewGuid())"
$archivePath = Join-Path $temporaryDirectory $archiveName
$checksumsPath = Join-Path $temporaryDirectory "checksums.txt"

New-Item -ItemType Directory -Path $temporaryDirectory | Out-Null

try {
    Write-Host "Downloading Kafkaesque $Version for windows/$architecture..."
    Invoke-WebRequest -Uri "$releaseUrl/$archiveName" -OutFile $archivePath
    Invoke-WebRequest -Uri "$releaseUrl/checksums.txt" -OutFile $checksumsPath

    $checksumLine = Get-Content $checksumsPath |
        Where-Object { ($_ -split '\s+', 2)[1] -eq $archiveName } |
        Select-Object -First 1
    if (-not $checksumLine) {
        throw "No checksum found for $archiveName"
    }

    $expectedChecksum = ($checksumLine -split '\s+')[0].ToLowerInvariant()
    $actualChecksum = (Get-FileHash -Algorithm SHA256 $archivePath).Hash.ToLowerInvariant()
    if ($actualChecksum -ne $expectedChecksum) {
        throw "Checksum verification failed for $archiveName"
    }

    Expand-Archive -Path $archivePath -DestinationPath $temporaryDirectory -Force
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null

    $destination = Join-Path $InstallDir "kafkaesque.exe"
    Copy-Item (Join-Path $temporaryDirectory "kafkaesque.exe") $destination -Force

    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $pathEntries = if ($userPath) { $userPath -split ";" } else { @() }
    if ($pathEntries -notcontains $InstallDir) {
        $newUserPath = if ($userPath) { "$userPath;$InstallDir" } else { $InstallDir }
        [Environment]::SetEnvironmentVariable("Path", $newUserPath, "User")
    }
    $env:Path = "$InstallDir;$env:Path"

    & $destination --help | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Kafkaesque verification failed"
    }

    Write-Host "Kafkaesque installed at $destination"
    Write-Host "Open a new terminal before running kafkaesque."
}
finally {
    Remove-Item $temporaryDirectory -Recurse -Force -ErrorAction SilentlyContinue
}
