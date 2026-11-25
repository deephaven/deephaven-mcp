# Installs the Deephaven CLI: irm https://github.com/deephaven/deephaven-mcp/releases/download/cli-channel-release/install.ps1 | iex
# On another release channel: $env:DH_CHANNEL = 'rc'; irm ... | iex
# Wrapped in a script block so `iex` doesn't leak variables or exit the caller's shell.
& {
  $ErrorActionPreference = 'Stop'
  # Rewritten from package.json "name" by scripts/release.ts.
  $bin = 'dh'
  [Net.ServicePointManager]::SecurityProtocol =
    [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

  # HTTPS only; plain HTTP only for a loopback test server, matching the updater.
  function Assert-Trusted([Uri]$uri) {
    $loopback = 'localhost', '127.0.0.1', '[::1]'
    if ($uri.Scheme -ne 'https' -and -not ($uri.Scheme -eq 'http' -and $loopback -contains $uri.Host)) {
      throw "Refusing to install over insecure URL $uri"
    }
  }

  # Follows redirects by hand: Windows PowerShell 5.1 would follow HTTPS -> HTTP unchecked.
  function Save-Trusted([Uri]$uri, [string]$outFile) {
    for ($hop = 0; $hop -le 5; $hop++) {
      Assert-Trusted $uri
      $request = [Net.WebRequest]::Create($uri)
      $request.AllowAutoRedirect = $false
      $response = $request.GetResponse()
      try {
        $status = [int]$response.StatusCode
        if ($status -lt 300 -or $status -ge 400) {
          $out = [IO.File]::Create($outFile)
          try { $response.GetResponseStream().CopyTo($out) } finally { $out.Dispose() }
          return
        }
        $location = $response.Headers['Location']
      } finally {
        $response.Dispose()
      }
      if (-not $location) { throw "HTTP $status without a redirect location from $uri" }
      $uri = [Uri]::new($uri, $location)
    }
    throw "Too many redirects fetching $uri"
  }

  $repoUrl = if ($env:DH_INSTALL_REPO_URL) { $env:DH_INSTALL_REPO_URL } else { 'https://github.com/deephaven/deephaven-mcp' }
  Assert-Trusted $repoUrl
  $channel = if ($env:DH_CHANNEL) { $env:DH_CHANNEL } else { 'release' }
  # Same rules as src/channel.ts; -cmatch is case-sensitive. dev is reserved for builds run from source.
  if ($channel -ceq 'dev') { throw 'Channel dev is reserved for builds run from source' }
  if ($channel -cnotmatch '^[a-z][a-z0-9-]*$' -or $channel.Length -gt 40) {
    throw "Invalid channel ""$channel"": use lowercase letters, digits and hyphens, starting with a letter, at most 40 characters"
  }
  # Must be user-writable, or auto-update can't replace the binary.
  $dir = if ($env:DH_INSTALL_DIR) { $env:DH_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "Programs\$bin" }
  # Absolute against PowerShell's location (not .NET's cwd), since it's persisted in PATH.
  $dir = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($dir)
  # x64 also runs on Windows on ARM under emulation.
  $target = 'windows-x64'

  # DH_INSTALL_VERSION accepts 3.1.1, v3.1.1 or cli-v3.1.1; otherwise the channel release's manifest.
  $manifestUrl = if ($env:DH_INSTALL_VERSION) {
    "$repoUrl/releases/download/cli-v$($env:DH_INSTALL_VERSION -replace '^(cli-)?v', '')/manifest.json"
  } else {
    "$repoUrl/releases/download/cli-channel-$channel/manifest.json"
  }

  $tmp = Join-Path ([IO.Path]::GetTempPath()) "$bin-install-$([Guid]::NewGuid())"
  New-Item -ItemType Directory -Force $tmp | Out-Null
  try {
    # The manifest pins a tagged URL and checksum, so this can't mix releases.
    $manifestFile = Join-Path $tmp 'manifest.json'
    try {
      Save-Trusted $manifestUrl $manifestFile
    } catch {
      # .NET method errors arrive wrapped; find the WebException for its status.
      $e = $_.Exception
      while ($e -and -not ($e -is [Net.WebException])) { $e = $e.InnerException }
      if (-not $env:DH_INSTALL_VERSION -and $e -and $e.Response -and [int]$e.Response.StatusCode -eq 404) {
        throw "no channel $channel at $repoUrl"
      }
      throw
    }
    $manifest = Get-Content -Raw $manifestFile | ConvertFrom-Json
    $asset = $manifest.binaries.$target
    if (-not $asset) { throw "$bin v$($manifest.version) has no binary for $target" }
    $url = [Uri]::new([Uri]$manifestUrl, $asset.url)

    Write-Host "Downloading $bin v$($manifest.version) for $target..."
    $exe = Join-Path $tmp "$bin.exe"
    Save-Trusted $url $exe
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
    Move-Item -Force $exe (Join-Path $dir "$bin.exe")
    # Install metadata the CLI reads (see src/install-meta.ts); written fresh on every install.
    # Not Set-Content: Windows PowerShell 5.1 writes a BOM, which JSON.parse rejects.
    $meta = Join-Path $dir "$bin.exe.meta"
    $metaTmp = "$meta.$PID.tmp"
    [IO.File]::WriteAllText($metaTmp, "{""channel"":""$channel""}`n", [Text.UTF8Encoding]::new($false))
    Move-Item -Force $metaTmp $meta
  } finally {
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $tmp
  }
  Write-Host "Installed $bin v$($manifest.version) ($channel channel) to $dir\$bin.exe"

  # Read and write the raw registry value, so %VAR% entries stay unexpanded.
  $envKey = [Microsoft.Win32.Registry]::CurrentUser.CreateSubKey('Environment')
  try {
    $userPath = [string]$envKey.GetValue('Path', '',
      [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
    # Keep an existing Path's kind (REG_SZ vs REG_EXPAND_SZ); only a new one is expandable.
    $kind = if ($null -ne $envKey.GetValue('Path')) { $envKey.GetValueKind('Path') }
      else { [Microsoft.Win32.RegistryValueKind]::ExpandString }
    $expandable = $kind -eq [Microsoft.Win32.RegistryValueKind]::ExpandString
    $entries = $(if ($expandable) { [Environment]::ExpandEnvironmentVariables($userPath) } else { $userPath }) -split ';'
    if ($entries -notcontains $dir) {
      # ';' would split into bogus entries and '%' would expand in a REG_EXPAND_SZ value.
      if ($env:DH_INSTALL_NO_MODIFY_PATH -or $dir -match '[;%]') {
        Write-Host "Add $dir to your PATH to run $bin."
      } else {
        $newPath = (@($userPath, $dir) | Where-Object { $_ }) -join ';'
        $envKey.SetValue('Path', $newPath, $kind)
        # Changing any user variable this way broadcasts WM_SETTINGCHANGE, so new terminals see the PATH.
        [Environment]::SetEnvironmentVariable('DH_INSTALL_PATH_CHANGED', '1', 'User')
        [Environment]::SetEnvironmentVariable('DH_INSTALL_PATH_CHANGED', $null, 'User')
        $env:Path = "$env:Path;$dir"
        Write-Host "Added $dir to your user PATH; other open terminals need a restart."
      }
    }
  } finally {
    $envKey.Close()
  }
}
