# Installs dh: irm https://github.com/deephaven/deephaven-mcp/releases/latest/download/install.ps1 | iex
# Wrapped in a script block so `iex` doesn't leak variables or exit the caller's shell.
& {
  $ErrorActionPreference = 'Stop'
  # Windows PowerShell 5.1 downloads very slowly with the progress bar on.
  $ProgressPreference = 'SilentlyContinue'
  [Net.ServicePointManager]::SecurityProtocol =
    [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

  $repoUrl = if ($env:DH_INSTALL_REPO_URL) { $env:DH_INSTALL_REPO_URL } else { 'https://github.com/deephaven/deephaven-mcp' }
  # HTTPS only; plain HTTP only for a loopback test server, matching the updater.
  $repo = [Uri]$repoUrl
  if ($repo.Scheme -ne 'https' -and -not ($repo.Scheme -eq 'http' -and $repo.IsLoopback)) {
    throw "Refusing to install over insecure URL $repoUrl"
  }
  # Must be user-writable, or auto-update can't replace the binary.
  $dir = if ($env:DH_INSTALL_DIR) { $env:DH_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA 'Programs\dh' }
  # x64 also runs on Windows on ARM under emulation.
  $target = 'x86_64-pc-windows-msvc'

  $manifestUrl = if ($env:DH_INSTALL_VERSION) {
    "$repoUrl/releases/download/v$($env:DH_INSTALL_VERSION -replace '^v', '')/manifest.json"
  } else {
    "$repoUrl/releases/latest/download/manifest.json"
  }

  $tmp = Join-Path ([IO.Path]::GetTempPath()) "dh-install-$([Guid]::NewGuid())"
  New-Item -ItemType Directory -Force $tmp | Out-Null
  try {
    # The manifest pins a tagged URL and checksum, so this can't mix releases.
    $manifestFile = Join-Path $tmp 'manifest.json'
    Invoke-WebRequest $manifestUrl -OutFile $manifestFile -UseBasicParsing
    $manifest = Get-Content -Raw $manifestFile | ConvertFrom-Json
    $asset = $manifest.binaries.$target
    if (-not $asset) { throw "dh v$($manifest.version) has no binary for $target" }
    $url = [Uri]::new([Uri]$manifestUrl, $asset.url)

    Write-Host "Downloading dh v$($manifest.version) for $target..."
    $exe = Join-Path $tmp 'dh.exe'
    Invoke-WebRequest $url -OutFile $exe -UseBasicParsing
    # Not Get-FileHash: it's missing in PowerShell 5.1 when launched from pwsh 7 (inherited PSModulePath).
    $sha256 = [Security.Cryptography.SHA256]::Create()
    $stream = [IO.File]::OpenRead($exe)
    try {
      $hash = [BitConverter]::ToString($sha256.ComputeHash($stream)) -replace '-', ''
    } finally {
      $stream.Dispose()
      $sha256.Dispose()
    }
    if ($hash -ne $asset.sha256) {
      throw "Checksum mismatch for $url"
    }

    New-Item -ItemType Directory -Force $dir | Out-Null
    Move-Item -Force $exe (Join-Path $dir 'dh.exe')
  } finally {
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $tmp
  }
  Write-Host "Installed dh v$($manifest.version) to $dir\dh.exe"

  $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
  if (($userPath -split ';') -notcontains $dir) {
    if ($env:DH_INSTALL_NO_MODIFY_PATH) {
      Write-Host "Add $dir to your PATH to run dh."
    } else {
      $newPath = (@($userPath, $dir) | Where-Object { $_ }) -join ';'
      [Environment]::SetEnvironmentVariable('Path', $newPath, 'User')
      $env:Path = "$env:Path;$dir"
      Write-Host "Added $dir to your user PATH; other open terminals need a restart."
    }
  }
}
