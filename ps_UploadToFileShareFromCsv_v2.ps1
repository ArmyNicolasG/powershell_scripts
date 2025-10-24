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

.PARAMETER GenerateStatusReports
  Si se indica, genera CSVs separados por tipo/estado leyendo SOLO los logs nativos.

.PARAMETER UploadSummaryCsv
  Ruta al archivo CSV de resumen de carga centralizado (opcional).
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
  [string]$UploadSummaryCsv

)

# ---------- Helpers base ----------
function New-NamedMutex { param([string]$Name) [System.Threading.Mutex]::new($false, "Global\$Name") }

function Ensure-CsvHeaderUtf8Bom {
  param([Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$HeaderLine)
  if (-not (Test-Path -LiteralPath $Path)) {
    $lp = Add-LongPathPrefix (Convert-ToSystemPath $Path)
    [void][System.IO.Directory]::CreateDirectory([System.IO.Path]::GetDirectoryName($lp))
    $fs = [System.IO.File]::Open($lp, [System.IO.FileMode]::CreateNew,
                                 [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
    $sw = New-Object System.IO.StreamWriter($fs, [System.Text.UTF8Encoding]::new($true)) # BOM
    $sw.WriteLine($HeaderLine); $sw.Dispose(); $fs.Dispose()
  }
}

function Append-LinesUtf8Retry {
  param([Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string[]]$Lines,
        [int]$MaxRetries = 40, [int]$InitialDelayMs = 120)
  $lp = Add-LongPathPrefix (Convert-ToSystemPath $Path)
  [void][System.IO.Directory]::CreateDirectory([System.IO.Path]::GetDirectoryName($lp))
  $attempt = 0
  while ($true) {
    try {
      $fs = [System.IO.File]::Open($lp, [System.IO.FileMode]::Append,
                                   [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
      $sw = New-Object System.IO.StreamWriter($fs, [System.Text.UTF8Encoding]::new($false))
      foreach ($l in $Lines) { $sw.WriteLine($l) }
      $sw.Dispose(); $fs.Dispose()
      break
    } catch [System.IO.IOException],[System.UnauthorizedAccessException] {
      if ($attempt -ge $MaxRetries) { throw }
      Start-Sleep -Milliseconds ([int]($InitialDelayMs * [math]::Pow(1.25, $attempt)))
      $attempt++
    }
  }
}

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

# ---------- Entorno AzCopy ----------
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

# ---------- Ejecución ----------
$outLines = @()
& $az @args 2>&1 | Tee-Object -Variable outLines | ForEach-Object { Write-Log $_ 'AZCOPY' } | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Log "AzCopy devolvió código $LASTEXITCODE." 'WARN' }

# ---------- Resumen legible (totales combinados) ----------
$summary = @{
  JobID                   = $null
  Status                  = $null
  TotalTransfers          = $null
  FileTransfers           = $null
  FolderPropertyTransfers = $null
  SymlinkTransfers        = $null
  TransfersCompleted      = $null
  TransfersFailed         = $null
  TransfersSkipped        = $null
  FoldersCompleted        = $null
  BytesTransferred        = $null
  BytesOverWire           = $null
  BytesExpected           = $null
  PercentComplete         = $null
  Elapsed                 = $null
}

[int]$filesCompleted=0;   [int]$foldersCompleted=0
[int]$filesFailed=0;      [int]$foldersFailed=0
[int]$filesSkipped=0;     [int]$foldersSkipped=0

foreach ($ln in $outLines) {
  # Job y estado
  if ($ln -match 'Job\s+([0-9a-fA-F-]{8,})\s+has started') { $summary.JobID = $Matches[1] }
  if ($ln -match '^\s*Final Job Status:\s*(.+)$')          { $summary.Status = $Matches[1].Trim() }

  # Totales
  if ($ln -match '^\s*Total Number of Transfers:\s*(\d+)') { $summary.TotalTransfers          = [int]$Matches[1] }
  if ($ln -match '^\s*Number of File Transfers:\s*(\d+)')  { $summary.FileTransfers           = [int]$Matches[1] }
  if ($ln -match '^\s*Number of Folder Property Transfers:\s*(\d+)') { $summary.FolderPropertyTransfers = [int]$Matches[1] }
  if ($ln -match '^\s*Number of Symlink Transfers:\s*(\d+)'){ $summary.SymlinkTransfers        = [int]$Matches[1] }

  # Resultados por estado (sumamos File+Folder para el total combinado)
  if ($ln -match '^\s*Number of File Transfers Completed:\s*(\d+)')   { $filesCompleted    = [int]$Matches[1] }
  if ($ln -match '^\s*Number of Folder Transfers Completed:\s*(\d+)') { $foldersCompleted  = [int]$Matches[1] }
  if ($ln -match '^\s*Number of File Transfers Failed:\s*(\d+)')      { $filesFailed       = [int]$Matches[1] }
  if ($ln -match '^\s*Number of Folder Transfers Failed:\s*(\d+)')    { $foldersFailed     = [int]$Matches[1] }
  if ($ln -match '^\s*Number of File Transfers Skipped:\s*(\d+)')     { $filesSkipped      = [int]$Matches[1] }
  if ($ln -match '^\s*Number of Folder Transfers Skipped:\s*(\d+)')   { $foldersSkipped    = [int]$Matches[1] }

  # Bytes (aceptamos con o sin “Number of”)
  if ($ln -match '^\s*Total (Number of )?Bytes Transferred:\s*(\d+)') { $summary.BytesTransferred = [int64]$Matches[2] }

  # % (si aparece)
  if ($ln -match '^\s*Percent Complete:\s*(\d+)%')                    { $summary.PercentComplete  = [int]$Matches[1] }

  # Duración (con o sin paréntesis, guardamos tal cual)
  if ($ln -match '^\s*Elapsed Time(?:\s*\([^)]+\))?:\s*(.+)$')        { $summary.Elapsed          = $Matches[1].Trim() }
}

# Combina para llenar las columnas del CSV
$summary.TransfersCompleted = $filesCompleted  + $foldersCompleted
$summary.TransfersFailed    = $filesFailed     + $foldersFailed
$summary.TransfersSkipped   = $filesSkipped    + $foldersSkipped

# Coherencia básica (solo aviso en log, no falla el run)
$checkOk = $false
if ($summary.TotalTransfers -ne $null -and
    $summary.TransfersCompleted -ne $null -and
    $summary.TransfersFailed -ne $null -and
    $summary.TransfersSkipped -ne $null) {
  $sumStates = $summary.TransfersCompleted + $summary.TransfersFailed + $summary.TransfersSkipped
  $checkOk = ($sumStates -eq $summary.TotalTransfers)
  if (-not $checkOk) {
    Write-Log ("ADVERTENCIA: Completed+Failed+Skipped ({0}) != TotalTransfers ({1})" -f $sumStates, $summary.TotalTransfers) 'WARN'
  }
}

Write-Log "===================== RESUMEN DE AZCOPY ====================="
if ($summary.JobID -ne $null)            { Write-Log ("JobID:                 {0}" -f $summary.JobID) }
if ($summary.Status)                      { Write-Log ("Estado:                {0}" -f $summary.Status) }
if ($summary.TotalTransfers -ne $null)    { Write-Log ("Total transfers:       {0}" -f $summary.TotalTransfers) }
if ($summary.FileTransfers -ne $null)     { Write-Log ("  ├─ Files:            {0}" -f $summary.FileTransfers) }
if ($summary.FolderPropertyTransfers -ne $null) { Write-Log ("  ├─ Folder props:     {0}" -f $summary.FolderPropertyTransfers) }
if ($summary.SymlinkTransfers -ne $null)  { Write-Log ("  └─ Symlinks:         {0}" -f $summary.SymlinkTransfers) }

if ($summary.TransfersCompleted -ne $null){ Write-Log ("Completados:           {0}" -f $summary.TransfersCompleted) }
if ($summary.TransfersFailed -ne $null)   { Write-Log ("Fallidos:              {0}" -f $summary.TransfersFailed) }
if ($summary.TransfersSkipped -ne $null)  { Write-Log ("Saltados:              {0}" -f $summary.TransfersSkipped) }

if ($summary.BytesTransferred -ne $null)  { Write-Log ("Bytes transferidos:    {0}" -f $summary.BytesTransferred) }
if ($summary.BytesOverWire   -ne $null)   { Write-Log ("Bytes sobre la red:    {0}" -f $summary.BytesOverWire) }
if ($summary.BytesExpected   -ne $null)   { Write-Log ("Bytes esperados:       {0}" -f $summary.BytesExpected) }
if ($summary.PercentComplete -ne $null)   { Write-Log ("Porcentaje completado: {0}%" -f $summary.PercentComplete) }
if ($summary.Elapsed)                     { Write-Log ("Duración:              {0}" -f $summary.Elapsed) }
Write-Log "============================================================="


# ---------- TXT opcionales (compat) ----------
if ($GenerateStatusReports) {
  $sumPath = Join-Path $LogDir 'summary.txt'
  $summaryLines = @(
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

# ---------- CSV centralizado de subidas (robusto + Excel) ----------
try {
  if ($UploadSummaryCsv) {
    $folderName = Split-Path -Path $SourceRoot -Leaf

    # Encabezado amigable para Excel (sin 'ñ'), separador ';'
    $header = 'Subcarpeta;JobID;Estado;TotalTransfers;Completados;Fallidos;Saltados;BytesTransferidos;Duracion;FechaHora;LogWrapper'

    # Línea de datos, siempre entrecomillada por campo
    $sep = ';'
    $vals = @(
      $folderName
      $summary.JobID
      $summary.Status
      $summary.TotalTransfers
      $summary.TransfersCompleted
      $summary.TransfersFailed
      $summary.TransfersSkipped
      $summary.BytesTransferred
      $summary.Elapsed
      (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
      $script:LogPath
    ) | ForEach-Object { "$_" -replace '"','""' }
    $dataLine = '"' + ($vals -join ('"' + $sep + '"')) + '"'

    # Mutex global para escrituras atómicas entre procesos/ventanas
    $mutex = New-NamedMutex -Name 'upload_summary_mutex'
    $null  = $mutex.WaitOne()

    # Fase de reparación: fusiona huérfanos antiguos si existieran
    $sumDir = Split-Path -Parent (Convert-ToSystemPath $UploadSummaryCsv)
    $orphans = Get-ChildItem -LiteralPath $sumDir -File -Filter '.__tmp_up_*.csv' -ErrorAction SilentlyContinue
    if ($orphans) {
      Ensure-CsvHeaderUtf8Bom -Path $UploadSummaryCsv -HeaderLine $header
      foreach ($t in $orphans) {
        $lines = Get-Content -LiteralPath $t -ErrorAction SilentlyContinue
        if ($lines.Count -gt 1) {
          Append-LinesUtf8Retry -Path $UploadSummaryCsv -Lines $lines[1..($lines.Count-1)]
        }
        Remove-Item -LiteralPath $t -Force -ErrorAction SilentlyContinue
      }
    }

    # Escritura normal
    Ensure-CsvHeaderUtf8Bom -Path $UploadSummaryCsv -HeaderLine $header
    Append-LinesUtf8Retry   -Path $UploadSummaryCsv -Lines @($dataLine)

    Write-Log "UploadSummaryCsv actualizado -> $UploadSummaryCsv"
  }
  else {
    Write-Log "UploadSummaryCsv no especificado: se omite CSV centralizado (modo standalone)."
  }
}
catch {
  Write-Log "WARN: No se pudo actualizar UploadSummaryCsv -> $($_.Exception.Message)" 'WARN'
}
finally {
  if ($mutex) { $mutex.ReleaseMutex(); $mutex.Dispose() }
}



Write-Log "Logs nativos de AzCopy -> $AzNative"
Write-Log ("Logs wrapper -> {0}-{1}.txt (rotación por {2} MB)" -f $LogPrefix,$script:LogIndex,$MaxLogSizeMB)
