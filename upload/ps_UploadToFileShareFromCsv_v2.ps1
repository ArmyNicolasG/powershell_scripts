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
  [int]$MaxLogSizeMB = 8
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
function Ensure-Directory {
  param([string]$Dir)
  $lp = Add-LongPathPrefix (Convert-ToSystemPath $Dir)
  [void][System.IO.Directory]::CreateDirectory($lp)
}

# ---------- Logger rotativo (consola + archivo) ----------
$LogPrefix = Join-Path $LogDir 'upload-logs'
$script:LogIndex = 1
$script:LogPath  = "{0}-{1}.txt" -f $LogPrefix, $script:LogIndex
$MaxBytes = [int64]$MaxLogSizeMB * 1MB
Ensure-Directory -Dir $LogDir

function Open-NewLog {
  $script:LogIndex++
  $script:LogPath = "{0}-{1}.txt" -f $LogPrefix, $script:LogIndex
}
function Write-Log {
  param([string]$Message,[string]$Level='INFO')
  $ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss.fff')
  $line = "[$ts][$Level] $Message"
  # consola
  Write-Host $line
  # archivo con rotación por tamaño y FileShare RW
  $lp = Add-LongPathPrefix (Convert-ToSystemPath $script:LogPath)
  [void][System.IO.Directory]::CreateDirectory([System.IO.Path]::GetDirectoryName($lp))
  $max = 4
  for($i=1;$i -le $max;$i++){
    try {
      if (Test-Path -LiteralPath $script:LogPath) {
        $len = (Get-Item -LiteralPath $script:LogPath).Length
        if ($len -ge $MaxBytes) { Open-NewLog }
      }
      $lp = Add-LongPathPrefix (Convert-ToSystemPath $script:LogPath)
      $fs = [System.IO.File]::Open($lp, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
      $sw = New-Object System.IO.StreamWriter($fs, [System.Text.UTF8Encoding]::new($true))
      $sw.WriteLine($line); $sw.Dispose(); $fs.Dispose(); break
    } catch {
      if ($i -eq $max) { Write-Host "[$ts][WARN] No se pudo escribir log: $($_.Exception.Message)" }
      else { Start-Sleep -Milliseconds (100 * $i * $i) }
    }
  }
}

# ---------- Construcción de URL destino manteniendo tu estilo ----------
function Build-DestUrl {
  param([string]$StorageAccount,[string]$ShareName,[string]$DestSubPath,[string]$Sas,[string]$ServiceType)
  $sub = $DestSubPath
  if ([string]::IsNullOrWhiteSpace($sub)) { $sub = "" }
  $sub = $sub -replace '\\','/'
  if ($sub -and -not $sub.StartsWith('/')) { $sub = '/' + $sub }
  $sasT = $Sas.Trim()
  if (-not $sasT.StartsWith('?')) { $sasT = '?' + $sasT }

  if ($ServiceType -eq 'Blob') {
    # El nombre "ShareName" se usa como contenedor
    return "https://$StorageAccount.blob.core.windows.net/$ShareName$sub$sasT"
  } else {
    return "https://$StorageAccount.file.core.windows.net/$ShareName$sub$sasT"
  }
}

# ---------- Preparación de entorno ----------
Ensure-Directory -Dir $LogDir
$AzNative = Join-Path $LogDir 'azcopy'
Ensure-Directory -Dir $AzNative
$env:AZCOPY_LOG_LOCATION = $AzNative

$src = Convert-ToSystemPath $SourceRoot
$destUrl = Build-DestUrl -StorageAccount $StorageAccount -ShareName $ShareName -DestSubPath $DestSubPath -Sas $Sas -ServiceType $ServiceType

# ---------- Comando AzCopy ----------
$az = $AzCopyPath
$args = @('copy', $src, $destUrl, '--recursive=true', "--overwrite=$Overwrite",'--output-level','essential','--log-level','INFO')

if ($IncludePaths -and $IncludePaths.Count -gt 0) {
  $inc = ($IncludePaths -join ';')
  $args += @('--include-path', $inc)
}

if ($ServiceType -eq 'FileShare' -and $PreservePermissions) {
  $args += @('--preserve-smb-permissions=true','--preserve-smb-info=true')
}

Write-Log "Destino: $destUrl"
Write-Log ("Ejecutando: {0} {1}" -f $az, ($args -join ' '))

# ---------- Ejecución y captura ----------
$outLines = @()
& $az @args 2>&1 | Tee-Object -Variable outLines | ForEach-Object {
  # Mostrar cada línea cruda también en log para auditoría
  try { Write-Log $_ 'AZCOPY' } catch {}
} | Out-Null

if ($LASTEXITCODE -ne 0) {
  Write-Log "AzCopy devolvió código $LASTEXITCODE." 'WARN'
}

# ---------- Resumen legible (a partir del JSON de azcopy) ----------
# Buscar el bloque "EndOfJob" / "CompleteJobOrdered" en la salida JSONL
$summary = @{
  JobID = $null; Status=$null; TotalTransfers=$null; TransfersCompleted=$null; TransfersFailed=$null; TransfersSkipped=$null;
  BytesEnumerated=$null; BytesTransferred=$null; ElapsedSeconds=$null; FilesScanned=$null; FoldersScanned=$null
}

foreach ($line in $outLines) {
  try {
    if ([string]::IsNullOrWhiteSpace($line)) { continue }
    $o = $line | ConvertFrom-Json -ErrorAction Stop

    if ($null -ne $o.JobID) { $summary.JobID = $o.JobID }
    if ($o.MessageType -eq 'EndOfJob' -or $o.MessageType -eq 'CompleteJobOrdered') {
      $m = $o.MessageContent
      if ($m) {
        # AzCopy suele devolver propiedades con estos nombres (varían por versión)
        $summary.Status              = $m.JobStatus            ?? $summary.Status
        $summary.TotalTransfers      = $m.TotalTransfers       ?? $summary.TotalTransfers
        $summary.TransfersCompleted  = $m.TransfersCompleted   ?? $summary.TransfersCompleted
        $summary.TransfersFailed     = $m.TransfersFailed      ?? $summary.TransfersFailed
        $summary.TransfersSkipped    = $m.TransfersSkipped     ?? $summary.TransfersSkipped
        $summary.BytesEnumerated     = $m.TotalBytesEnumerated ?? $m.BytesEnumerated ?? $summary.BytesEnumerated
        $summary.BytesTransferred    = $m.BytesOverWire        ?? $m.TotalBytesTransferred ?? $summary.BytesTransferred
        $summary.ElapsedSeconds      = $m.ElapsedTimeInMs      ? [math]::Round($m.ElapsedTimeInMs/1000,2) : $summary.ElapsedSeconds
        $summary.FilesScanned        = $m.FileTransfers        ?? $summary.FilesScanned
        $summary.FoldersScanned      = $m.FolderPropertyTransfers ?? $summary.FoldersScanned
      }
    }
  } catch { continue }
}

Write-Log "===================== RESUMEN DE AZCOPY =====================" 'INFO'
if ($summary.JobID)              { Write-Log ("JobID:             {0}" -f $summary.JobID) }
if ($summary.Status)             { Write-Log ("Estado:            {0}" -f $summary.Status) }
if ($summary.TotalTransfers)     { Write-Log ("Total transfers:   {0}" -f $summary.TotalTransfers) }
if ($summary.TransfersCompleted) { Write-Log ("Completados:       {0}" -f $summary.TransfersCompleted) }
if ($summary.TransfersFailed)    { Write-Log ("Fallidos:          {0}" -f $summary.TransfersFailed) }
if ($summary.TransfersSkipped)   { Write-Log ("Saltados:          {0}" -f $summary.TransfersSkipped) }
if ($summary.FilesScanned)       { Write-Log ("Archivos tratados: {0}" -f $summary.FilesScanned) }
if ($summary.FoldersScanned)     { Write-Log ("Carpetas tratadas: {0}" -f $summary.FoldersScanned) }
if ($summary.BytesEnumerated)    { Write-Log ("Bytes enumerados:  {0}" -f $summary.BytesEnumerated) }
if ($summary.BytesTransferred)   { Write-Log ("Bytes enviados:    {0}" -f $summary.BytesTransferred) }
if ($summary.ElapsedSeconds)     { Write-Log ("Duración (s):      {0}" -f $summary.ElapsedSeconds) }
Write-Log "==============================================================" 'INFO'

Write-Log "Logs nativos de AzCopy -> $AzNative"
Write-Log ("Logs wrapper -> {0}-{1}.txt (rotación por {2} MB)" -f $LogPrefix,$script:LogIndex,$MaxLogSizeMB)
