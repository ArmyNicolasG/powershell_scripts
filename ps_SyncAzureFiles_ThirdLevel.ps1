[CmdletBinding()]
param(
  [Parameter(Mandatory)] [string] $SourceRoot,
  [Parameter(Mandatory)] [string] $StorageAccount,
  [Parameter(Mandatory)] [string] $ShareName,
  [Parameter(Mandatory)] [AllowEmptyString()] [string] $DestBaseSubPath,
  [Parameter(Mandatory)] [string] $Sas,

  [string] $AzCopyPath = "azcopy",
  [string] $LogFile = ".\azcopy-sync-third-level.log",

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
  [switch] $FallbackToSecondLevel,

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
  if ([string]::IsNullOrWhiteSpace($p)) { return }
  if (-not (Test-Path -LiteralPath $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null }
}

function Write-LogLine([string]$message) {
  $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $message
  Write-Host $line

  if ($script:MasterLogPath) {
    try {
      $parent = Split-Path -Path $script:MasterLogPath -Parent
      if (-not [string]::IsNullOrWhiteSpace($parent)) { Ensure-Dir $parent }
      $line | Out-File -LiteralPath $script:MasterLogPath -Append -Encoding utf8
    } catch { }
  }
}

function Write-Info([string]$message) {
  Write-LogLine $message
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
  foreach ($procId in $PidList) {
    if (Get-Process -Id $procId -ErrorAction SilentlyContinue) { [void]$alive.Add([int]$procId) }
  }

  $PidList.Clear()
  foreach ($procId in $alive) { [void]$PidList.Add([int]$procId) }
  $PidList.Count
}

function Get-StampedLogFolder {
  param([Parameter(Mandatory)][string]$InputLogFile)

  $parent = Split-Path -Path $InputLogFile -Parent
  if ([string]::IsNullOrWhiteSpace($parent)) { $parent = "." }

  $stamp = Get-Date -Format "yyyyMMdd-HHmmss"

  if ($parent -eq ".") {
    return (Join-Path "." ("logs-" + $stamp))
  }

  return ($parent.TrimEnd("\") + "-" + $stamp)
}

function Build-FileShareDestUrl {
  param(
    [Parameter(Mandatory)] [string]$Account,
    [Parameter(Mandatory)] [string]$Share,
    [AllowEmptyString()] [string]$DestSubPath,
    [Parameter(Mandatory)] [string]$SasToken
  )

  $sasT = Normalize-Sas $SasToken
  $destSub = Normalize-SubPath $DestSubPath
  $baseUrl = "https://$Account.file.core.windows.net/$Share"

  if ([string]::IsNullOrWhiteSpace($destSub)) {
    return "$baseUrl$sasT"
  }

  return "$baseUrl/$destSub$sasT"
}

function Build-ThirdLevelDestSubPath {
  param(
    [AllowEmptyString()] [string]$BaseSubPath,
    [Parameter(Mandatory)] [string]$SecondLevelName,
    [Parameter(Mandatory)] [string]$ThirdLevelName
  )

  $parts = New-Object System.Collections.Generic.List[string]
  $base = Normalize-SubPath $BaseSubPath
  if (-not [string]::IsNullOrWhiteSpace($base)) { [void]$parts.Add($base) }
  [void]$parts.Add($SecondLevelName)
  [void]$parts.Add($ThirdLevelName)
  ($parts.ToArray() -join "/")
}

function Build-SecondLevelDestSubPath {
  param(
    [AllowEmptyString()] [string]$BaseSubPath,
    [Parameter(Mandatory)] [string]$SecondLevelName
  )

  $parts = New-Object System.Collections.Generic.List[string]
  $base = Normalize-SubPath $BaseSubPath
  if (-not [string]::IsNullOrWhiteSpace($base)) { [void]$parts.Add($base) }
  [void]$parts.Add($SecondLevelName)
  ($parts.ToArray() -join "/")
}

function Sanitize-LogName([string]$name) {
  if ([string]::IsNullOrWhiteSpace($name)) { return "unnamed" }
  ($name -replace '[<>:"/\\|?*\x00-\x1F]','_').Trim()
}

function Get-ThirdLevelWorkItems {
  param(
    [Parameter(Mandatory)] [string]$ResolvedSourceRoot,
    [System.Collections.Generic.HashSet[string]]$OnlySet,
    [System.Collections.Generic.HashSet[string]]$ExcludeSet,
    [switch]$AllowSecondLevelFallback
  )

  $items = New-Object System.Collections.Generic.List[object]
  Write-Info ("Enumerando carpetas de segundo nivel en '{0}'..." -f $ResolvedSourceRoot)
  $secondLevelDirs = @(Get-ChildItem -LiteralPath $ResolvedSourceRoot -Directory -Force -ErrorAction SilentlyContinue)
  Write-Info ("Segundo nivel detectado antes de filtros: {0}" -f $secondLevelDirs.Count)

  if ($OnlySet -and $OnlySet.Count -gt 0) {
    $secondLevelDirs = $secondLevelDirs | Where-Object { $OnlySet.Contains($_.Name) }
    Write-Info ("Aplicado DoOnly. Segundo nivel restante: {0}" -f @($secondLevelDirs).Count)
  }
  if ($ExcludeSet -and $ExcludeSet.Count -gt 0) {
    $secondLevelDirs = $secondLevelDirs | Where-Object { -not $ExcludeSet.Contains($_.Name) }
    Write-Info ("Aplicado Exclude. Segundo nivel restante: {0}" -f @($secondLevelDirs).Count)
  }

  foreach ($secondDir in $secondLevelDirs) {
    Write-Info ("Revisando segundo nivel '{0}'..." -f $secondDir.Name)
    $thirdLevelDirs = @(Get-ChildItem -LiteralPath $secondDir.FullName -Directory -Force -ErrorAction SilentlyContinue)
    Write-Info ("'{0}' tiene {1} subcarpetas directas." -f $secondDir.Name, $thirdLevelDirs.Count)

    if ($thirdLevelDirs.Count -eq 0) {
      if ($AllowSecondLevelFallback) {
        $fallbackDest = Build-SecondLevelDestSubPath -BaseSubPath $DestBaseSubPath -SecondLevelName $secondDir.Name
        Write-Info ("Fallback a segundo nivel para '{0}': no tiene carpetas de tercer nivel. Destino='{1}'." -f $secondDir.Name, $fallbackDest)
        $items.Add([pscustomobject]@{
          WorkLevel       = "SecondLevel"
          SecondLevelName = $secondDir.Name
          ThirdLevelName  = $null
          SourcePath      = $secondDir.FullName
          DestSubPath     = $fallbackDest
        }) | Out-Null
      } else {
        Write-Info ("Omitiendo '{0}': no tiene carpetas de tercer nivel." -f $secondDir.Name)
      }
      continue
    }

    foreach ($thirdDir in $thirdLevelDirs) {
      $destSub = Build-ThirdLevelDestSubPath -BaseSubPath $DestBaseSubPath -SecondLevelName $secondDir.Name -ThirdLevelName $thirdDir.Name
      Write-Info ("Unidad detectada: '{0}' / '{1}' -> '{2}'." -f $secondDir.Name, $thirdDir.Name, $destSub)
      $items.Add([pscustomobject]@{
        WorkLevel       = "ThirdLevel"
        SecondLevelName = $secondDir.Name
        ThirdLevelName  = $thirdDir.Name
        SourcePath      = $thirdDir.FullName
        DestSubPath     = $destSub
      }) | Out-Null
    }
  }

  Write-Info ("Enumeracion finalizada. Unidades acumuladas: {0}" -f $items.Count)
  $items
}

function Invoke-SyncWorker {
  param(
    [Parameter(Mandatory)] [string] $WorkerSourcePath,
    [Parameter(Mandatory)] [AllowEmptyString()] [string] $WorkerDestSubPath,
    [Parameter(Mandatory)] [string] $WorkerLogFile
  )

  $src = (Resolve-Path -LiteralPath $WorkerSourcePath).Path
  $destUrl = Build-FileShareDestUrl -Account $StorageAccount -Share $ShareName -DestSubPath $WorkerDestSubPath -SasToken $Sas

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

    $lp = Split-Path -Path $WorkerLogFile -Parent
    if ([string]::IsNullOrWhiteSpace($lp)) { $lp = "." }
    Ensure-Dir $lp

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

$logLeaf = Split-Path -Path $LogFile -Leaf
if ([string]::IsNullOrWhiteSpace($logLeaf)) { $logLeaf = "azcopy-sync-third-level.log" }

$stampedFolder = Get-StampedLogFolder -InputLogFile $LogFile
Ensure-Dir $stampedFolder

$LogFile = Join-Path $stampedFolder $logLeaf
$logParent = $stampedFolder
$script:MasterLogPath = $LogFile

Write-Info ("Inicio de corrida third-level sync. SourceRoot='{0}' Share='{1}' DestBaseSubPath='{2}'" -f $srcRootResolved, $ShareName, $DestBaseSubPath)
if ($FallbackToSecondLevel) {
  Write-Info "Fallback a segundo nivel: ACTIVADO"
} else {
  Write-Info "Fallback a segundo nivel: DESACTIVADO"
}

if (-not $OpenNewWindows) {
  Write-Info "Modo directo: se sincroniza toda la raiz."
  $code = Invoke-SyncWorker -WorkerSourcePath $srcRootResolved -WorkerDestSubPath $DestBaseSubPath -WorkerLogFile $LogFile
  exit $code
}

Write-Info "Resolviendo pwsh para modo ventanas..."
$pwshExe = Get-PwshExe
Write-Info ("pwsh detectado en '{0}'." -f $pwshExe)
$workItems = @(Get-ThirdLevelWorkItems -ResolvedSourceRoot $srcRootResolved -OnlySet $onlySet -ExcludeSet $exclSet -AllowSecondLevelFallback:$FallbackToSecondLevel)

if ($workItems.Count -eq 0) {
  Write-Info "No hay carpetas de tercer nivel para procesar."
  exit 0
}

Write-Info ("Unidades de trabajo de tercer nivel: {0}" -f $workItems.Count)

$launchedPids = New-Object 'System.Collections.Generic.List[int]'

foreach ($item in $workItems) {
  while ($true) {
    $active = Get-AliveCount -PidList $launchedPids
    $ramPct = Get-RamUsagePercent

    $okWindows = ($MaxOpenWindows -le 0) -or ($active -lt $MaxOpenWindows)
    $okRam = ($RamSafeLimit -le 0) -or ($ramPct -lt $RamSafeLimit)

    if ($okWindows -and $okRam) { break }
    Write-Info ("Esperando recursos para lanzar siguiente unidad. Ventanas activas={0}, RAM={1}%, MaxOpenWindows={2}, RamSafeLimit={3}%." -f $active, $ramPct, $MaxOpenWindows, $RamSafeLimit)
    Start-Sleep -Seconds ([math]::Max(2, $LaunchPollSeconds))
  }

  $secondName = $item.SecondLevelName
  $thirdName  = $item.ThirdLevelName
  $childSrc   = $item.SourcePath
  $destSub    = $item.DestSubPath
  $workLevel  = $item.WorkLevel

  $safeSecond = Sanitize-LogName $secondName
  $safeThird  = Sanitize-LogName $thirdName
  $childLog   = if ($workLevel -eq "SecondLevel") {
    Join-Path $logParent ("sync-" + $safeSecond + ".log")
  } else {
    Join-Path $logParent ("sync-" + $safeSecond + "--" + $safeThird + ".log")
  }

  $srcEsc     = Escape-SingleQuotes $childSrc
  $destEsc    = Escape-SingleQuotes $destSub
  $logEsc     = Escape-SingleQuotes $childLog
  $accEsc     = Escape-SingleQuotes $StorageAccount
  $shareEsc   = Escape-SingleQuotes $ShareName
  $sasEsc     = Escape-SingleQuotes (Normalize-Sas $Sas)
  $azEsc      = Escape-SingleQuotes $AzCopyPath
  $windowTitle = if ($workLevel -eq "SecondLevel") { "SYNC ($secondName)" } else { "SYNC ($secondName -> $thirdName)" }
  $titleEsc   = Escape-SingleQuotes $windowTitle

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
`$host.ui.rawui.WindowTitle = '$titleEsc';

function Normalize-SubPath([string]`$p){ (`$p -replace '\\','/').Trim().Trim('/') }
function Normalize-Sas([string]`$s){ if (`$s.Trim().StartsWith('?')) { `$s.Trim() } else { '?' + `$s.Trim() } }
function Build-FileShareDestUrl([string]`$account, [string]`$share, [string]`$destSubPath, [string]`$sasToken) {
  `$sasT = Normalize-Sas `$sasToken;
  `$destSub = Normalize-SubPath `$destSubPath;
  `$baseUrl = 'https://' + `$account + '.file.core.windows.net/' + `$share;
  if ([string]::IsNullOrWhiteSpace(`$destSub)) {
    return `$baseUrl + `$sasT;
  }
  return `$baseUrl + '/' + `$destSub + `$sasT;
}

`$env:AZCOPY_CONCURRENCY_VALUE = '$AzConcurrency';
`$env:AZCOPY_BUFFER_GB = '$AzBufferGB';

`$src = '$srcEsc';
`$destUrl = Build-FileShareDestUrl '$accEsc' '$shareEsc' '$destEsc' '$sasEsc';

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

  if ($workLevel -eq "SecondLevel") {
    Write-Info ("Lanzado fallback segundo nivel: {0} -> {1}" -f $secondName, $destSub)
  } else {
    Write-Info ("Lanzado tercer nivel: {0} / {1} -> {2}" -f $secondName, $thirdName, $destSub)
  }

  if ($WindowLaunchDelaySeconds -gt 0) {
    Start-Sleep -Seconds $WindowLaunchDelaySeconds
  }
}

Write-Info "Orquestacion finalizada."
exit 0
