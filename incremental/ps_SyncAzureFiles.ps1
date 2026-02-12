[CmdletBinding()]
param(
  [Parameter(Mandatory)] [string] $SourceRoot,
  [Parameter(Mandatory)] [string] $StorageAccount,
  [Parameter(Mandatory)] [string] $ShareName,
  [Parameter(Mandatory)] [string] $DestBaseSubPath,
  [Parameter(Mandatory)] [string] $Sas,

  [string] $AzCopyPath = "azcopy",
  [string] $LogFile = ".\azcopy-sync.log",

  [switch] $PreservePermissions,

  [int] $AzConcurrency = 16,
  [int] $AzBufferGB = 1,

  [switch] $OpenNewWindows,
  [int] $MaxOpenWindows = 3,
  [int] $LaunchPollSeconds = 10,
  [int] $WindowLaunchDelaySeconds = 15,
  [int] $RamSafeLimit = 65,

  [string] $DoOnly,
  [string] $Exclude,

  [switch] $HoldOnError
)

function Normalize-Sas([string]$s) {
  $t = $s.Trim()
  if (-not $t.StartsWith("?")) { $t = "?" + $t }
  $t
}

function Normalize-SubPath([string]$p) {
  $t = ($p -replace "\\","/").Trim()
  $t.Trim("/")
}

function Escape-SingleQuotes([string]$s) {
  if ($null -eq $s) { return "" }
  $s -replace "'","''"
}

function Ensure-Dir([string]$p) {
  if (-not (Test-Path -LiteralPath $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null }
}

function Parse-NameList([string]$s) {
  $set = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
  if ([string]::IsNullOrWhiteSpace($s)) { return $set }
  foreach ($raw in ($s -split ';')) {
    $n = $raw.Trim()
    if ($n.Length -gt 0) { [void]$set.Add($n) }
  }
  $set
}

function Get-RamUsagePercent {
  $os = Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue
  if (-not $os) { return 0 }
  $used = ($os.TotalVisibleMemorySize - $os.FreePhysicalMemory)
  [int][math]::Round(($used / [double]$os.TotalVisibleMemorySize) * 100, 0)
}

function Get-PwshExe {
  $candidates = @(
    (Get-Command pwsh -ErrorAction SilentlyContinue)?.Source,
    "$env:ProgramFiles\PowerShell\7\pwsh.exe",
    "$env:ProgramFiles\PowerShell\7-preview\pwsh.exe"
  ) | Where-Object { $_ -and (Test-Path $_) } | Select-Object -First 1

  if (-not $candidates) { throw "pwsh.exe no encontrado." }
  $candidates
}

function Get-AliveCount {
  param([System.Collections.Generic.List[int]]$PidList)

  if (-not $PidList) { return 0 }
  $alive = New-Object 'System.Collections.Generic.List[int]'
  foreach ($pid in $PidList) {
    if (Get-Process -Id $pid -ErrorAction SilentlyContinue) { [void]$alive.Add([int]$pid) }
  }
  $PidList.Clear()
  foreach ($pid in $alive) { [void]$PidList.Add([int]$pid) }
  $PidList.Count
}

function Invoke-SyncWorker {
  param(
    [Parameter(Mandatory)] [string] $WorkerSourcePath,
    [Parameter(Mandatory)] [string] $WorkerDestBaseSubPath,
    [Parameter(Mandatory)] [string] $WorkerLogFile
  )

  $src  = (Resolve-Path -LiteralPath $WorkerSourcePath).Path
  $leaf = Split-Path $src -Leaf

  $base = Normalize-SubPath $WorkerDestBaseSubPath

  $destSub = $base
  if ([string]::IsNullOrWhiteSpace($destSub)) {
    $destSub = $leaf
  } else {
    $last = $destSub.Split("/")[-1]
    if ($last -ne $leaf) { $destSub = ($destSub + "/" + $leaf).Trim("/") }
  }

  $sasT = Normalize-Sas $Sas
  $destUrl = "https://$StorageAccount.file.core.windows.net/$ShareName/$destSub$sasT"

  $prevConc = $env:AZCOPY_CONCURRENCY_VALUE
  $prevBuf  = $env:AZCOPY_BUFFER_GB
  try {
    $env:AZCOPY_CONCURRENCY_VALUE = [string]$AzConcurrency
    $env:AZCOPY_BUFFER_GB         = [string]$AzBufferGB

    $args = @(
      "sync", $src, $destUrl,
      "--recursive=true",
      "--delete-destination=false",
      "--output-level=essential",
      "--log-level=ERROR"
    )

    if ($PreservePermissions) {
      $args += @("--preserve-smb-permissions=true","--preserve-smb-info=true")
    }

    Ensure-Dir (Split-Path -Path $WorkerLogFile -Parent)
    & $AzCopyPath @args 2>&1 | Tee-Object -FilePath $WorkerLogFile | Out-Null
    return $LASTEXITCODE
  }
  finally {
    $env:AZCOPY_CONCURRENCY_VALUE = $prevConc
    $env:AZCOPY_BUFFER_GB         = $prevBuf
  }
}

$onlySet = Parse-NameList $DoOnly
$exclSet = Parse-NameList $Exclude

$srcRootResolved = (Resolve-Path -LiteralPath $SourceRoot).Path

# modo directo
if (-not $OpenNewWindows) {
  $code = Invoke-SyncWorker -WorkerSourcePath $srcRootResolved -WorkerDestBaseSubPath $DestBaseSubPath -WorkerLogFile $LogFile
  exit $code
}

# modo orquestador
$pwshExe = Get-PwshExe

$logParent = Split-Path -Path $LogFile -Parent
if ([string]::IsNullOrWhiteSpace($logParent)) { $logParent = "." }
Ensure-Dir $logParent

$rootDirs = Get-ChildItem -LiteralPath $srcRootResolved -Directory -Force

if ($onlySet.Count -gt 0) {
  $rootDirs = $rootDirs | Where-Object { $onlySet.Contains($_.Name) }
}
if ($exclSet.Count -gt 0) {
  $rootDirs = $rootDirs | Where-Object { -not $exclSet.Contains($_.Name) }
}

$launchedPids = New-Object 'System.Collections.Generic.List[int]'

foreach ($dir in $rootDirs) {

  while ($true) {
    $active = Get-AliveCount -PidList $launchedPids
    $ramPct = Get-RamUsagePercent

    $okWindows = ($MaxOpenWindows -le 0) -or ($active -lt $MaxOpenWindows)
    $okRam = ($RamSafeLimit -le 0) -or ($ramPct -lt $RamSafeLimit)

    if ($okWindows -and $okRam) { break }
    Start-Sleep -Seconds ([math]::Max(2, $LaunchPollSeconds))
  }

  $childName = $dir.Name
  $childSrc  = $dir.FullName

  $safeName = ($childName -replace '[<>:"/\\|?*\x00-\x1F]','_')
  $childLog = Join-Path $logParent ("sync-" + $safeName + ".log")

  $srcEsc   = Escape-SingleQuotes $childSrc
  $baseEsc  = Escape-SingleQuotes $DestBaseSubPath
  $logEsc   = Escape-SingleQuotes $childLog
  $accEsc   = Escape-SingleQuotes $StorageAccount
  $shareEsc = Escape-SingleQuotes $ShareName
  $sasEsc   = Escape-SingleQuotes (Normalize-Sas $Sas)
  $azEsc    = Escape-SingleQuotes $AzCopyPath

  $preserveLine = ""
  if ($PreservePermissions) {
    $preserveLine = "`$args += @('--preserve-smb-permissions=true','--preserve-smb-info=true');"
  }

  $holdOnErrLine = ""
  if ($HoldOnError) {
    $holdOnErrLine = "Write-Host 'Presione Enter para cerrar...'; Read-Host | Out-Null;"
  }

  $cmd = @"
`$ErrorActionPreference='Continue';
`$host.ui.rawui.WindowTitle = 'SYNC ($childName)';

function Normalize-SubPath([string]`$p){ (`$p -replace '\\','/').Trim().Trim('/') }
function Normalize-Sas([string]`$s){ if (`$s.Trim().StartsWith('?')) { `$s.Trim() } else { '?' + `$s.Trim() } }

`$env:AZCOPY_CONCURRENCY_VALUE = '$AzConcurrency';
`$env:AZCOPY_BUFFER_GB = '$AzBufferGB';

`$src = '$srcEsc';
`$leaf = Split-Path `$src -Leaf;
`$base = Normalize-SubPath '$baseEsc';

`$destSub = `$base;
if ([string]::IsNullOrWhiteSpace(`$destSub)) {
  `$destSub = `$leaf;
} else {
  `$last = `$destSub.Split('/')[-1];
  if (`$last -ne `$leaf) { `$destSub = (`$destSub + '/' + `$leaf).Trim('/') }
}

`$destUrl = 'https://$accEsc.file.core.windows.net/$shareEsc/' + `$destSub + (Normalize-Sas '$sasEsc');

Write-Host '=== SYNC INICIO ===';
Write-Host "Origen:  `$src";
Write-Host "Destino: `$destUrl";

`$args = @(
  'sync', `$src, `$destUrl,
  '--recursive=true',
  '--delete-destination=false',
  '--output-level=essential',
  '--log-level=ERROR'
);
$preserveLine

& '$azEsc' @args 2>&1 | Tee-Object -FilePath '$logEsc' | Out-Null;

`$code = `$LASTEXITCODE
Write-Host "=== SYNC FIN (ExitCode=`$code) ===";
if (`$code -ne 0) { $holdOnErrLine; exit `$code }
exit 0
"@

  $enc = [Convert]::ToBase64String([System.Text.Encoding]::Unicode.GetBytes($cmd))

  $p = Start-Process -FilePath $pwshExe -PassThru -ArgumentList @(
    '-NoLogo','-NoProfile','-ExecutionPolicy','Bypass',
    '-WindowStyle','Normal',
    '-EncodedCommand', $enc
  )

  if ($p -and $p.Id) { [void]$launchedPids.Add([int]$p.Id) }

  if ($WindowLaunchDelaySeconds -gt 0) {
    Start-Sleep -Seconds $WindowLaunchDelaySeconds
  }
}

exit 0