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
  JobID=$null; Status=$null; TotalTransfers=$null; Completed=$null; Failed=$null; Skipped=$null;
  BytesTransferred=$null; Elapsed=$null
}
foreach ($ln in $outLines) {
  if ($ln -match 'Job\s+([0-9a-fA-F-]{8,})\s+has started') { $summary.JobID = $Matches[1] }
  if ($ln -match '^\s*Final Job Status:\s*(.+)$')          { $summary.Status = $Matches[1].Trim() }
  if ($ln -match '^\s*Total Number of Transfers:\s*(\d+)') { $summary.TotalTransfers = [int]$Matches[1] }
  if ($ln -match '^\s*Number of File Transfers:\s*(\d+)')  { $summary.TotalTransfers = [int]$Matches[1] }
  if ($ln -match '^\s*Number of Transfers Completed:\s*(\d+)') { $summary.Completed = [int]$Matches[1] }
  if ($ln -match '^\s*Number of Transfers Failed:\s*(\d+)')    { $summary.Failed = [int]$Matches[1] }
  if ($ln -match '^\s*Number of Transfers Skipped:\s*(\d+)')   { $summary.Skipped = [int]$Matches[1] }
  if ($ln -match '^\s*Total Bytes Transferred:\s*(\d+)')       { $summary.BytesTransferred = [int64]$Matches[1] }
  if ($ln -match '^\s*Elapsed Time:\s*(.+)$')                  { $summary.Elapsed = $Matches[1].Trim() }
}

Write-Log "===================== RESUMEN DE AZCOPY ====================="
if ($summary.JobID)            { Write-Log ("JobID:               {0}" -f $summary.JobID) }
if ($summary.Status)           { Write-Log ("Estado:              {0}" -f $summary.Status) }
if ($summary.TotalTransfers)   { Write-Log ("Total transfers:     {0}" -f $summary.TotalTransfers) }
if ($summary.Completed)        { Write-Log ("Completados:         {0}" -f $summary.Completed) }
if ($summary.Failed -ne $null) { Write-Log ("Fallidos:            {0}" -f $summary.Failed) }
if ($summary.Skipped -ne $null){ Write-Log ("Saltados:            {0}" -f $summary.Skipped) }
if ($summary.BytesTransferred) { Write-Log ("Bytes transferidos:  {0}" -f $summary.BytesTransferred) }
if ($summary.Elapsed)          { Write-Log ("Duración:            {0}" -f $summary.Elapsed) }
Write-Log "============================================================="

# ---------- Reportes por tipo/estado desde logs nativos (sin jobs/list) ----------
if ($GenerateStatusReports) {
  # Aviso: para obtener eventos Completed/Skipped por elemento, usa -NativeLogLevel INFO
  if ($NativeLogLevel -ne 'INFO') {
    Write-Log "Advertencia: NativeLogLevel=$NativeLogLevel. Para CSV detallados por elemento, usa -NativeLogLevel INFO." 'WARN'
  }

  # Encuentra el log nativo más reciente tras esta ejecución
  $latestNative = Get-ChildItem -LiteralPath $AzNative -File -ErrorAction SilentlyContinue |
                  Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if (-not $latestNative) {
    Write-Log "No se encontró log nativo en '$AzNative'." 'WARN'
  }
  else {
    Write-Log "Analizando log nativo: $($latestNative.FullName)"

    $csvMap = @{
      'File|Completed'   = (Join-Path $LogDir 'files_completed.csv')
      'File|Skipped'     = (Join-Path $LogDir 'files_skipped.csv')
      'File|Failed'      = (Join-Path $LogDir 'files_failed.csv')
      'Folder|Completed' = (Join-Path $LogDir 'folders_completed.csv')
      'Folder|Skipped'   = (Join-Path $LogDir 'folders_skipped.csv')
      'Folder|Failed'    = (Join-Path $LogDir 'folders_failed.csv')
    }
    foreach ($p in $csvMap.Values) {
      if (Test-Path -LiteralPath $p) { Remove-Item -LiteralPath $p -Force -ErrorAction SilentlyContinue }
      'Path,Status,Error' | Out-File -LiteralPath $p -Encoding UTF8
    }

    # Procesamiento streaming, baja memoria
    $writeLine = {
      param($key,$path,$status,$err)
      $csv = $csvMap[$key]; if (-not $csv) { return }
      $safePath = ($path -replace '"','""')
      $safeErr  = ($err  -replace '"','""')
      Add-Content -LiteralPath $csv -Value ('"{0}","{1}","{2}"' -f $safePath,$status,$safeErr)
    }

    # Patrones tolerantes (AzCopy cambia formatos entre versiones)
    $reStatus = @(
      'status="?([A-Za-z]+)"?',                  # status="Success"
      'transferStatus="?([A-Za-z]+)"?',          # transferStatus=Failed
      '"Status"\s*:\s*"([A-Za-z]+)"'             # "Status":"Skipped"
    )
    $reIsDir = @(
      'isDir=(true|false)',                      # isDir=true
      'isFolder=(true|false)',                   # isFolder=false
      '"isDir"\s*:\s*(true|false)',
      '"entityType"\s*:\s*"(File|Folder)"'
    )
    $rePath = @(
      'source="([^"]+)"',                        # source="C:\..."
      'path="([^"]+)"',
      '"path"\s*:\s*"([^"]+)"',
      '"source"\s*:\s*"([^"]+)"',
      '"relativePath"\s*:\s*"([^"]+)"'
    )
    $reError = @(
      'error(Msg|Message)?="?([^"]+)"?',         # error="The system cannot find..."
      '"error(Msg|Message)"\s*:\s*"([^"]+)"'
    )

    $chunkSize = 2000
    $buf = New-Object System.Text.StringBuilder

    Get-Content -LiteralPath $latestNative.FullName -ReadCount $chunkSize -ErrorAction SilentlyContinue | ForEach-Object {
      foreach ($line in $_) {
        # Extrae status
        $status = $null
        foreach ($rx in $reStatus) { if ($line -match $rx) { $status = $matches[1]; break } }
        if (-not $status) { continue }

        # Normaliza status
        switch -Regex ($status) {
          '^(Success|Completed)$' { $status = 'Completed' }
          '^Skipped$'             { }
          '^Failed$'              { }
          default                 { continue } # ignorar otros niveles
        }

        # Path
        $path = $null
        foreach ($rx in $rePath) { if ($line -match $rx) { $path = $matches[1]; break } }
        if (-not $path) { continue }

        # isDir / entityType
        $entity = 'File'
        foreach ($rx in $reIsDir) {
          if ($line -match $rx) {
            $val = $matches[1]
            if ($val -match '^(true|Folder)$') { $entity = 'Folder' } else { $entity = 'File' }
            break
          }
        }

        # Error (si existe)
        $err = ''
        foreach ($rx in $reError) { if ($line -match $rx) { $err = $matches[-1]; break } }

        # Ruta CSV destino
        $key = '{0}|{1}' -f $entity,$status
        & $writeLine $key $path $status $err
      }
    }

    Write-Log "CSV detallados generados (si el nivel de log lo permitió)."
  }
}

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

Write-Log "Logs nativos de AzCopy -> $AzNative"
Write-Log ("Logs wrapper -> {0}-{1}.txt (rotación por {2} MB)" -f $LogPrefix,$script:LogIndex,$MaxLogSizeMB)
