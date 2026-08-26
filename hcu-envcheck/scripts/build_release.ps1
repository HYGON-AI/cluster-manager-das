# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
[CmdletBinding()]
param(
    [string]$OutputDirectory,
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

function Set-TarField {
    param([byte[]]$Header, [int]$Offset, [int]$Length, [string]$Value)
    $Bytes = [Text.Encoding]::ASCII.GetBytes($Value)
    if ($Bytes.Length -gt $Length) { throw "Tar field is too long: $Value" }
    [Array]::Copy($Bytes, 0, $Header, $Offset, $Bytes.Length)
}

function New-TarHeader {
    param([string]$Name, [int64]$Size, [int]$Mode, [char]$TypeFlag)
    if ([Text.Encoding]::ASCII.GetByteCount($Name) -gt 100) {
        throw "Tar path exceeds the portable 100-byte limit: $Name"
    }
    [byte[]]$Header = New-Object byte[] 512
    Set-TarField $Header 0 100 $Name
    Set-TarField $Header 100 8 (([Convert]::ToString($Mode, 8)).PadLeft(7, '0') + "`0")
    Set-TarField $Header 108 8 ("0000000`0")
    Set-TarField $Header 116 8 ("0000000`0")
    Set-TarField $Header 124 12 (([Convert]::ToString($Size, 8)).PadLeft(11, '0') + "`0")
    Set-TarField $Header 136 12 ("00000000000`0")
    for ($Index = 148; $Index -lt 156; $Index++) { $Header[$Index] = 32 }
    $Header[156] = [byte][char]$TypeFlag
    Set-TarField $Header 257 6 "ustar`0"
    Set-TarField $Header 263 2 '00'
    Set-TarField $Header 265 32 'root'
    Set-TarField $Header 297 32 'root'
    Set-TarField $Header 329 8 ("0000000`0")
    Set-TarField $Header 337 8 ("0000000`0")
    [int64]$Checksum = 0
    foreach ($Byte in $Header) { $Checksum += $Byte }
    Set-TarField $Header 148 8 (([Convert]::ToString($Checksum, 8)).PadLeft(6, '0') + "`0 ")
    return ,$Header
}

function New-LinuxTarGz {
    param([string]$SourceDirectory, [string]$RootName, [string]$Destination)
    $PlainTar = "$Destination.tar"
    $TarStream = [IO.File]::Create($PlainTar)
    try {
        [byte[]]$Header = New-TarHeader "$RootName/" 0 493 '5'
        $TarStream.Write($Header, 0, $Header.Length)
        $Prefix = $SourceDirectory.TrimEnd('\') + '\'
        $Items = @(Get-ChildItem -LiteralPath $SourceDirectory -Recurse | Sort-Object FullName)
        foreach ($Item in $Items) {
            if (-not $Item.FullName.StartsWith($Prefix, [StringComparison]::OrdinalIgnoreCase)) {
                throw "Staged entry escaped release root: $($Item.FullName)"
            }
            $Relative = $Item.FullName.Substring($Prefix.Length).Replace('\', '/')
            $EntryName = "$RootName/$Relative"
            if ($Item.PSIsContainer) {
                [byte[]]$Header = New-TarHeader ($EntryName + '/') 0 493 '5'
                $TarStream.Write($Header, 0, $Header.Length)
                continue
            }
            $Mode = 420
            if ($Relative -match '(^|/)(hcu-envcheck\.sh|install\.sh|bin/[^/]+|examples/[^/]+\.sh)$') { $Mode = 493 }
            [byte[]]$Header = New-TarHeader $EntryName $Item.Length $Mode '0'
            $TarStream.Write($Header, 0, $Header.Length)
            $InputStream = [IO.File]::OpenRead($Item.FullName)
            try { $InputStream.CopyTo($TarStream) } finally { $InputStream.Dispose() }
            $Padding = (512 - ($Item.Length % 512)) % 512
            if ($Padding -gt 0) {
                [byte[]]$Zeros = New-Object byte[] $Padding
                $TarStream.Write($Zeros, 0, $Zeros.Length)
            }
        }
        [byte[]]$Trailer = New-Object byte[] 1024
        $TarStream.Write($Trailer, 0, $Trailer.Length)
    }
    finally { $TarStream.Dispose() }

    $TarInput = [IO.File]::OpenRead($PlainTar)
    $GzipOutput = [IO.File]::Create($Destination)
    $Gzip = New-Object IO.Compression.GzipStream($GzipOutput, [IO.Compression.CompressionMode]::Compress)
    try { $TarInput.CopyTo($Gzip) }
    finally {
        $Gzip.Dispose()
        $GzipOutput.Dispose()
        $TarInput.Dispose()
        Remove-Item -LiteralPath $PlainTar -Force
    }
}
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $ProjectRoot 'dist'
}

$VersionFile = Join-Path $ProjectRoot 'VERSION'
$ManifestFile = Join-Path $ProjectRoot 'MANIFEST.release'
if (-not (Test-Path -LiteralPath $VersionFile -PathType Leaf)) {
    throw 'VERSION is missing'
}
if (-not (Test-Path -LiteralPath $ManifestFile -PathType Leaf)) {
    throw 'MANIFEST.release is missing'
}

$Version = (Get-Content -LiteralPath $VersionFile -Raw).Trim()
if ($Version -notmatch '^[A-Za-z0-9._-]+$') {
    throw "Invalid VERSION: $Version"
}
$ReleaseName = "hcu-envcheck-$Version"
$PyprojectText = Get-Content -LiteralPath (Join-Path $ProjectRoot 'pyproject.toml') -Raw
$ModuleText = Get-Content -LiteralPath (Join-Path $ProjectRoot 'hcu_envcheck\__init__.py') -Raw
$PyprojectMatch = [regex]::Match($PyprojectText, '(?m)^version\s*=\s*"([^"]+)"\s*$')
$ModuleMatch = [regex]::Match($ModuleText, '(?m)^__version__\s*=\s*"([^"]+)"\s*$')
if (-not $PyprojectMatch.Success -or $PyprojectMatch.Groups[1].Value -ne $Version) {
    $Found = if ($PyprojectMatch.Success) { $PyprojectMatch.Groups[1].Value } else { 'MISSING' }
    throw "Version mismatch: VERSION=$Version pyproject.toml=$Found"
}
if (-not $ModuleMatch.Success -or $ModuleMatch.Groups[1].Value -ne $Version) {
    $Found = if ($ModuleMatch.Success) { $ModuleMatch.Groups[1].Value } else { 'MISSING' }
    throw "Version mismatch: VERSION=$Version hcu_envcheck/__init__.py=$Found"
}
$Archive = Join-Path $OutputDirectory "$ReleaseName.tar.gz"
if ((Test-Path -LiteralPath $Archive -PathType Leaf) -and -not $Force) {
    throw "Release already exists: $Archive (use -Force to replace)"
}
if ((Test-Path -LiteralPath "$Archive.sha256" -PathType Leaf) -and -not $Force) {
    throw "Release checksum already exists: $Archive.sha256 (use -Force to replace)"
}
$TempRoot = Join-Path ([IO.Path]::GetTempPath()) ("hcu-envcheck-release-" + [guid]::NewGuid().ToString('N'))
$Stage = Join-Path $TempRoot $ReleaseName

try {
    New-Item -ItemType Directory -Path $Stage -Force | Out-Null
    New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null

    foreach ($Line in Get-Content -LiteralPath $ManifestFile) {
        $Line = $Line.Trim()
        if (-not $Line -or $Line.StartsWith('#')) { continue }
        if ($Line -notmatch '^(required|optional)\s+(\S+)$') {
            throw "Invalid MANIFEST.release line: $Line"
        }
        $Policy = $Matches[1]
        $RelativePath = $Matches[2]
        $Source = Join-Path $ProjectRoot $RelativePath
        if (-not (Test-Path -LiteralPath $Source)) {
            if ($Policy -eq 'optional') { continue }
            throw "Required release input is missing: $RelativePath"
        }
        Copy-Item -LiteralPath $Source -Destination (Join-Path $Stage $RelativePath) -Recurse -Force
    }

    $CacheFiles = @(Get-ChildItem -LiteralPath $Stage -Recurse -File |
        Where-Object { $_.Extension -eq '.pyc' -or $_.Extension -eq '.pyo' })
    foreach ($CacheFile in $CacheFiles) {
        Remove-Item -LiteralPath $CacheFile.FullName -Force
    }
    $CacheDirectories = @(Get-ChildItem -LiteralPath $Stage -Recurse -Directory -Filter '__pycache__' |
        Sort-Object FullName -Descending)
    foreach ($CacheDirectory in $CacheDirectories) {
        Remove-Item -LiteralPath $CacheDirectory.FullName -Recurse -Force
    }

    $VcsCommit = 'UNAVAILABLE'
    $Git = Get-Command git -ErrorAction SilentlyContinue
    if ($Git) {
        # ProjectRoot may be a subdirectory of a monorepo and therefore have no
        # .git entry of its own. Let Git discover the enclosing work tree.
        $DetectedCommit = (& $Git.Source -C $ProjectRoot rev-parse --verify HEAD 2>$null)
        if ($LASTEXITCODE -eq 0 -and $DetectedCommit) { $VcsCommit = $DetectedCommit.Trim() }
    }
    $ReleaseInfo = @(
        "version=$Version"
        ('build_time_utc=' + [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ'))
        "vcs_commit=$VcsCommit"
    )
    [IO.File]::WriteAllText(
        (Join-Path $Stage 'RELEASE-INFO.txt'),
        (($ReleaseInfo -join "`n") + "`n"),
        [Text.UTF8Encoding]::new($false)
    )

    $ChecksumManifest = Join-Path $Stage 'RELEASE-MANIFEST.sha256'
    $ChecksumLines = @(Get-ChildItem -LiteralPath $Stage -Recurse -File |
        Where-Object FullName -ne $ChecksumManifest |
        Sort-Object FullName |
        ForEach-Object {
            # PS 5.1 has no System.IO.Path.GetRelativePath. All enumerated files
            # are descendants of Stage, so a checked prefix trim is sufficient.
            $Prefix = $Stage.TrimEnd('\') + '\'
            if (-not $_.FullName.StartsWith($Prefix, [StringComparison]::OrdinalIgnoreCase)) {
                throw "Staged file escaped release root: $($_.FullName)"
            }
            $Relative = $_.FullName.Substring($Prefix.Length).Replace('\', '/')
            $Hash = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
            "$Hash  ./$Relative"
        })
    if ($ChecksumLines.Count -eq 0) { throw 'Release staging directory is empty' }
    # Checksum tools on Linux treat CR as part of a path. Always emit LF even
    # when this builder runs under Windows PowerShell 5.1.
    [IO.File]::WriteAllText(
        $ChecksumManifest,
        (($ChecksumLines -join "`n") + "`n"),
        [Text.UTF8Encoding]::new($false)
    )

    $TempArchive = Join-Path $TempRoot "$ReleaseName.tar.gz"
    New-LinuxTarGz -SourceDirectory $Stage -RootName $ReleaseName -Destination $TempArchive

    Move-Item -LiteralPath $TempArchive -Destination $Archive -Force
    $ArchiveHash = (Get-FileHash -LiteralPath $Archive -Algorithm SHA256).Hash.ToLowerInvariant()
    [IO.File]::WriteAllText("$Archive.sha256", "$ArchiveHash  $ReleaseName.tar.gz`n", [Text.UTF8Encoding]::new($false))

    Write-Output "Release: $Archive"
    Write-Output "Checksum: $Archive.sha256"
    Write-Output 'The archive is offline and contains no third-party Python dependencies.'
}
finally {
    if (Test-Path -LiteralPath $TempRoot) {
        Remove-Item -LiteralPath $TempRoot -Recurse -Force
    }
}
