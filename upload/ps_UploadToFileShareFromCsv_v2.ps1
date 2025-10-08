<#
.SYNOPSIS
  Subida a Azure (File Share o Blob) con logs rotativos y resumen legible de AzCopy.
  Mantiene la forma de conexión por partes: -StorageAccount, -ShareName, -DestSubPath, -Sas.

.PARAMETER SourceRoot
  Carpeta local origen (puede ser UNC).

.PARAMETER StorageAccount
  Nombre de la cuenta (ej. itvstoragediscoxprd001).

.PARAMETER ShareName
  Nombre del File Share o del contenedor (según ServiceType).

.PARAMETER DestSubPath
  Subcarpeta/ruta destino dentro del share o contenedor (con o sin '/' inicial).

.PARAMETER Sas
  Token SAS (con o sin '?').

.PARAMETER ServiceType
  'FileShare' (default) o 'Blob'. Ajusta flags como preserve-smb-*.

.PARAMETER LogDir
  Carpeta única para TODOS los outputs. Se crea si no existe.
  - upload-logs-#.txt (logs del wrapper con rotación)
  - azcopy\* (logs nativos de AzCopy vía AZCOPY_LOG_LOCATION)

.PARAMETER AzCopyPath
  Ruta a azcopy.exe (si no, usa 'azcopy' del PATH).

.PARAMETER Overwrite
  'ifSourceNewer' (default), 'true', 'false' (append-only) o 'prompt'.

.PARAMETER IncludePaths
  Lista de rutas relativas a incluir (mapeadas a --include-path "a;b;c").

.PARAMETER PreservePermissions
  Solo aplica a FileShare: añade --preserve-smb-permissions=true --preserve-smb-info=true

.PARAMETER MaxLogSizeMB
  Tamaño máximo por archivo de log rotativo (default 8 MB).

.PARAMETER AzCopyPath
  Ruta a azcopy.exe (si no, usa 'azcopy' del PATH).
.PARAMETER NativeLogLevel
  Nivel de logs nativos de AzCopy (ERROR por defecto) para evitar ruido. Valores comunes: ERROR, INFO, WARNING, PANIC.

.PARAMETER ConsoleOutputLevel
  Nivel de salida en consola (AzCopy): essential (default), quiet, info.

.EXAMPLE
  .\ps_UploadToFileShareFromCsv_vNext.ps1 `
    -SourceRoot "\\192.168.98.19\UnidadX\1CONTABILIDAD\ESTADOS FINANCIEROS" `
    -StorageAccount "itvstoragediscoxprd001" -ShareName "disco-x" -DestSubPath "/1CONTABILIDAD" `
    -Sas "?sv=..." -ServiceType FileShare -PreservePermissions `
    -AzCopyPath "C:\Tools\azcopy.exe" -LogDir "D:\logs\migracion\contabilidad"

.EXAMPLE
  .\ps_UploadToFileShareFromCsv_vNext.ps1 `
    -SourceRoot "D:\export" `
    -StorageAccount "miacct" -ShareName "backups" -DestSubPath "/2025" `
    -Sas "sv=..." -ServiceType Blob `
    -LogDir "D:\logs\migracion\backup-blob"
    .PARAMETER GenerateStatusReports
  Si se indica, genera failed.txt, skipped.txt, completed.txt y summary.txt en -LogDir.

#>

[CmdletBinding()]
param(
  [Parameter(Mandatory)][string]$SourceRoot,
  [Parameter(Mandatory)][string]$StorageAccount,
  [Parameter(Mandatory)][string]$ShareName,
  [Parameter(Mandatory)][string]$DestSubPath,
  [Parameter(Mandatory)][string]$Sas,

  [ValidateSet('FileShare','Blob')][string]$ServiceType = 'FileShare',
  [ValidateSet('ifSourceNewer','true','false','prompt')][string]$Overwrite = 'ifSourceNewer',
  [string[]]$IncludePaths,
  [string]$AzCopyPath = 'azcopy',
  [switch]$PreservePermissions,
  [Parameter(Mandatory)][string]$LogDir,
  [int]$MaxLogSizeMB = 8,

  [switch]$GenerateStatusReports,
  [ValidateSet('ERROR','INFO','WARNING','PANIC')][string]$NativeLogLevel = 'ERROR',
  [ValidateSet('essential','quiet','info')][string]$ConsoleOutputLevel = 'essential'
)

# ---------- Helpers base ----------
function Convert-ToSystemPath {
  param([string]$AnyPath)
  try { $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($AnyPath) }
  catch { $AnyPath -replace '^Microsoft\.PowerShell\.Core\\FileSystem::','' }
}
function Add-LongPathPrefix {
  param([string]$SystemPath)
  if ($SystemPath -like '\\?\*') { return $SystemPath }
  if (-not $IsWindows) { return $SystemPath }
  if ($SystemPath -match '^[A-Za-z]:\\') { return "\\?\$SystemPath" }
  if ($SystemPath -like '\\*') { return "\\?\UNC\{0}" -f $SystemPath.TrimStart('\') }
  $SystemPath
}
function Ensure-Directory { param([string]$Dir)
  $lp = Add-LongPathPrefix (Convert-ToSystemPath $Dir)
  [void][System.IO.Directory]::CreateDirectory($lp)
}

# ---------- Logger rotativo (consola + archivo) ----------
Ensure-Directory -Dir $LogDir
$LogPrefix = Join-Path $LogDir 'upload-logs'
$script:LogIndex = 1
$script:LogPath  = "{0}-{1}.txt" -f $LogPrefix, $script:LogIndex
$MaxBytes = [int64]$MaxLogSizeMB * 1MB

function Open-NewLog { $script:LogIndex++; $script:LogPath = "{0}-{1}.txt" -f $LogPrefix, $script:LogIndex }
function Write-Log {
  param([string]$Message,[string]$Level='INFO')
  $ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss.fff')
  $line = "[$ts][$Level] $Message"
  Write-Host $line
  try {
    if (Test-Path -LiteralPath $script:LogPath) {
      $len = (Get-Item -LiteralPath $script:LogPath).Length
      if ($len -ge $MaxBytes) { Open-NewLog }
    }
    $lp = Add-LongPathPrefix (Convert-ToSystemPath $script:LogPath)
    $fs = [System.IO.File]::Open($lp, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
    $sw = New-Object System.IO.StreamWriter($fs, [System.Text.UTF8Encoding]::new($true))
    $sw.WriteLine($line); $sw.Dispose(); $fs.Dispose()
  } catch {}
}

# ---------- Build Dest URL ----------
function Build-DestUrl {
  param([string]$StorageAccount,[string]$ShareName,[string]$DestSubPath,[string]$Sas,[string]$ServiceType)
  $sub = $DestSubPath
  if ([string]::IsNullOrWhiteSpace($sub)) { $sub = "" }
  $sub = $sub -replace '\\','/'
  if ($sub -and -not $sub.StartsWith('/')) { $sub = '/' + $sub }
  $sasT = $Sas.Trim(); if (-not $sasT.StartsWith('?')) { $sasT = '?' + $sasT }
  if ($ServiceType -eq 'Blob') { "https://$StorageAccount.blob.core.windows.net/$ShareName$sub$sasT" }
  else                         { "https://$StorageAccount.file.core.windows.net/$ShareName$sub$sasT" }
}

# preparación comando
$AzNative = Join-Path $LogDir 'azcopy'
Ensure-Directory -Dir $AzNative
$env:AZCOPY_LOG_LOCATION = $AzNative

$src = Convert-ToSystemPath $SourceRoot
$destUrl = Build-DestUrl -StorageAccount $StorageAccount -ShareName $ShareName -DestSubPath $DestSubPath -Sas $Sas -ServiceType $ServiceType

$az = $AzCopyPath
$args = @('copy', $src, $destUrl, '--recursive=true',
          "--overwrite=$Overwrite",
          '--output-level', $ConsoleOutputLevel,
          '--log-level', $NativeLogLevel)

if ($IncludePaths -and $IncludePaths.Count -gt 0) {
  $args += @('--include-path', ($IncludePaths -join ';'))
}
if ($ServiceType -eq 'FileShare' -and $PreservePermissions) {
  $args += @('--preserve-smb-permissions=true','--preserve-smb-info=true')
}

Write-Log "Destino: $destUrl"
Write-Log ("Ejecutando: {0} {1}" -f $az, ($args -join ' '))

#  capturar salida 
$jobStart = Get-Date
$outLines = @()
& $az @args 2>&1 | Tee-Object -Variable outLines | ForEach-Object {
  Write-Log $_ 'AZCOPY'
} | Out-Null

if ($LASTEXITCODE -ne 0) { Write-Log "AzCopy devolvió código $LASTEXITCODE." 'WARN' }

# resumen
$summary = @{
  JobID=$null; Status=$null; TotalTransfers=$null; Completed=$null; Failed=$null; Skipped=$null;
  BytesTransferred=$null; Elapsed=$null
}
foreach ($ln in $outLines) {
  if ($ln -match 'Job\s+([0-9a-fA-F-]{8,})\s+has started') { $summary.JobID = $Matches[1] }
  if ($ln -match '^Final Job Status:\s*(.+)$')             { $summary.Status = $Matches[1].Trim() }
  if ($ln -match '^Total Number of Transfers:\s*(\d+)')    { $summary.TotalTransfers = [int]$Matches[1] }
  if ($ln -match '^Number of File Transfers:\s*(\d+)')     { $summary.TotalTransfers = [int]$Matches[1] }
  if ($ln -match '^Number of Transfers Completed:\s*(\d+)'){ $summary.Completed = [int]$Matches[1] }
  if ($ln -match '^Number of Transfers Failed:\s*(\d+)')   { $summary.Failed = [int]$Matches[1] }
  if ($ln -match '^Number of Transfers Skipped:\s*(\d+)')  { $summary.Skipped = [int]$Matches[1] }
  if ($ln -match '^Total Bytes Transferred:\s*(\d+)')      { $summary.BytesTransferred = [int64]$Matches[1] }
  if ($ln -match '^Elapsed Time:\s*(.+)$')                 { $summary.Elapsed = $Matches[1].Trim() }
}

Write-Log "===================== RESUMEN DE AZCOPY ====================="
if ($summary.JobID)           { Write-Log ("JobID:               {0}" -f $summary.JobID) }
if ($summary.Status)          { Write-Log ("Estado:              {0}" -f $summary.Status) }
if ($summary.TotalTransfers)  { Write-Log ("Total transfers:     {0}" -f $summary.TotalTransfers) }
if ($summary.Completed)       { Write-Log ("Completados:         {0}" -f $summary.Completed) }
if ($summary.Failed -ne $null){ Write-Log ("Fallidos:            {0}" -f $summary.Failed) }
if ($summary.Skipped -ne $null){Write-Log ("Saltados:            {0}" -f $summary.Skipped) }
if ($summary.BytesTransferred){ Write-Log ("Bytes transferidos:  {0}" -f $summary.BytesTransferred) }
if ($summary.Elapsed)         { Write-Log ("Duración:            {0}" -f $summary.Elapsed) }
Write-Log "============================================================="

# ---------- Reportes por estado (TXT) usando jobs show JSON (solo para procesar) ----------
if ($GenerateStatusReports -and $summary.JobID) {
  Write-Log "Generando reportes de estado (failed/skipped/completed) …"
  $jobId = $summary.JobID
  $json = & $az jobs show $jobId --with-status=All --output-type json 2>$null
  $failed = New-Object System.Collections.Generic.List[object]
  $skipped= New-Object System.Collections.Generic.List[object]
  $done   = New-Object System.Collections.Generic.List[object]

  foreach ($ln in $json) {
    try {
      if ([string]::IsNullOrWhiteSpace($ln)) { continue }
      $o = $ln | ConvertFrom-Json -ErrorAction Stop
      $path = $o.path ?? $o.Path ?? $o.Destination ?? $o.Source ?? $null
      $status = $o.status ?? $o.Status ?? $o.TransferStatus ?? $null
      $err = $o.errorMsg ?? $o.ErrorMsg ?? $o.Error ?? $null

      switch ($status) {
        'Success'      { $done.Add($path)    | Out-Null }
        'Completed'    { $done.Add($path)    | Out-Null }
        'Skipped'      { $skipped.Add($path) | Out-Null }
        'Failed'       { $failed.Add(@("$path | $err")) | Out-Null }
      }
    } catch {}
  }

  $failedPath  = Join-Path $LogDir 'failed.txt'
  $skippedPath = Join-Path $LogDir 'skipped.txt'
  $donePath    = Join-Path $LogDir 'completed.txt'
  $sumPath     = Join-Path $LogDir 'summary.txt'

  if ($failed.Count  -gt 0) { $failed  | Set-Content -LiteralPath $failedPath  -Encoding UTF8; Write-Log "failed.txt -> $failedPath" }
  if ($skipped.Count -gt 0) { $skipped | Set-Content -LiteralPath $skippedPath -Encoding UTF8; Write-Log "skipped.txt -> $skippedPath" }
  if ($done.Count    -gt 0) { $done    | Set-Content -LiteralPath $donePath    -Encoding UTF8; Write-Log "completed.txt -> $donePath" }

  $summaryLines = @(
    "JobID:               $($summary.JobID)",
    "Estado:              $($summary.Status)",
    "Total transfers:     $($summary.TotalTransfers)",
    "Completados:         $($summary.Completed)",
    "Fallidos:            $($summary.Failed)",
    "Saltados:            $($summary.Skipped)",
    "Bytes transferidos:  $($summary.BytesTransferred)",
    "Duración:            $($summary.Elapsed)"
  )
  $summaryLines | Set-Content -LiteralPath $sumPath -Encoding UTF8
  Write-Log "summary.txt -> $sumPath"
}

Write-Log "Logs nativos de AzCopy -> $AzNative"
Write-Log ("Logs wrapper -> {0}-{1}.txt (rotación por {2} MB)" -f $LogPrefix,$script:LogIndex,$MaxLogSizeMB)