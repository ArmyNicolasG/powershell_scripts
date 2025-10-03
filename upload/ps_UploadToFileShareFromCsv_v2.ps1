<#
.SYNOPSIS
  AzCopy wrapper (compat) que mantiene parámetros separados:
  -StorageAccount, -ShareName, -DestSubPath, -Sas
  y añade logging a consola/archivo + CSV por-archivo + inventario remoto opcional.

.DESCRIPTION
  - Construye internamente el DestUrl conservando tu forma actual de invocación.
  - Muestra la salida de AzCopy en consola **y** la captura para procesar.
  - Genera CSV local con los estados de cada transferencia (jobs show).
  - (Opcional) Genera inventario remoto del File Share (azcopy list).
  - Soporta -AzCopyPath para usar una ruta específica del ejecutable.
  - Soporta -PreservePermissions (mapea a --preserve-smb-permissions y --preserve-smb-info).

.PARAMETER SourceRoot
  Carpeta local origen.

.PARAMETER StorageAccount
  Nombre de la cuenta de almacenamiento (ej. itvstoragedisc...prd001).

.PARAMETER ShareName
  Nombre del File Share (ej. 'disco-x').

.PARAMETER DestSubPath
  Subcarpeta destino dentro del share (ej. '/1CONTABILIDAD/ESTADOS FINANCIEROS'). Puede ir con o sin '/' inicial.

.PARAMETER Sas
  Token SAS que inicia con '?' o sin él (se ajusta automáticamente).

.PARAMETER OutCsv
  CSV por-archivo (local). Default: .\resultado_azcopy.csv

.PARAMETER LogDir
  Carpeta para logs **nativos** de AzCopy (AZCOPY_LOG_LOCATION).

.PARAMETER WrapperLog
  Archivo .log adicional del wrapper (mensajes de alto nivel).

.PARAMETER Overwrite
  'ifSourceNewer' (default), 'true', 'false' (append-only: sólo nuevos).

.PARAMETER IncludePaths
  Subconjunto de rutas relativas a incluir (usa --include-path).

.PARAMETER AzCopyPath
  Ruta a azcopy.exe si no está en PATH.

.PARAMETER PreservePermissions
  Si se indica, añade --preserve-smb-permissions=true --preserve-smb-info=true

.PARAMETER GenerateRemoteInventory
  Genera *.remote.csv con `azcopy list --recursive --output-type json`.

.EXAMPLE
  .\ps_UploadToFileShareFromCsv_v2.4_compat.ps1 `
    -SourceRoot "\\192.168.98.19\UnidadX\1CONTABILIDAD\ESTADOS FINANCIEROS" `
    -StorageAccount "itvstoragediscoxprd001" -ShareName "disco-x" -DestSubPath "/1CONTABILIDAD" `
    -Sas "?sv=..." -AzCopyPath "C:\Tools\azcopy.exe" -PreservePermissions `
    -OutCsv "D:\Migracion\resultado.csv" -LogDir "D:\Migracion\logs\AzCopy" -WrapperLog "D:\Migracion\logs\wrapper.log" `
    -Overwrite ifSourceNewer -GenerateRemoteInventory
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory)] [string]$SourceRoot,
  [Parameter(Mandatory)] [string]$StorageAccount,
  [Parameter(Mandatory)] [string]$ShareName,
  [Parameter(Mandatory)] [string]$DestSubPath,
  [Parameter(Mandatory)] [string]$Sas,
  [string]$OutCsv = ".\resultado_azcopy.csv",
  [string]$LogDir,
  [string]$WrapperLog,
  [ValidateSet('ifSourceNewer','true','false')][string]$Overwrite = 'ifSourceNewer',
  [string[]]$IncludePaths,
  [string]$AzCopyPath,
  [switch]$PreservePermissions,
  [switch]$GenerateRemoteInventory
)

# ---------- Helpers ----------
function Write-Log {
  param([string]$Message,[string]$Level='INFO')
  $ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss.fff')
  $line = "[$ts][$Level] $Message"
  Write-Host $line
  if ($WrapperLog) { Add-Content -LiteralPath $WrapperLog -Value $line -Encoding UTF8 }
}

function Build-DestUrl {
  param([string]$StorageAccount,[string]$ShareName,[string]$DestSubPath,[string]$Sas)
  $sub = $DestSubPath
  if ([string]::IsNullOrWhiteSpace($sub)) { $sub = "" }
  $sub = $sub -replace '\\','/'
  if ($sub -and -not $sub.StartsWith('/')) { $sub = '/' + $sub }
  $sasT = $Sas.Trim()
  if (-not $sasT.StartsWith('?')) { $sasT = '?' + $sasT }
  return "https://$StorageAccount.file.core.windows.net/$ShareName$sub$sasT"
}

function Get-AzCopyExe {
  param([string]$AzCopyPath)
  if ($AzCopyPath) {
    return $AzCopyPath
  }
  return "azcopy"
}

# CSV schema
$csvColumns = @('JobId','RelativePath','EntityType','Status','Bytes','LastModified','Error')
if (Test-Path -LiteralPath $OutCsv) {
  try {
    $header = Get-Content -LiteralPath $OutCsv -First 1 -ErrorAction Stop
    if ($header -and ($header -notmatch '^#TYPE')) {
      $existing = $header -split ',' | ForEach-Object { $_.Trim('"') }
      if ($existing.Count -gt 1) { $csvColumns = $existing }
    }
  } catch { }
}

function Export-Transfers {
  param([System.Collections.Generic.List[object]]$Rows,[string]$OutCsv)
  $Rows | Select-Object -Property $csvColumns | Export-Csv -LiteralPath $OutCsv -NoTypeInformation -Encoding UTF8
  Write-Log "CSV de transfers -> $OutCsv"
}

function Export-RemoteInventory {
  param([string]$DestUrl,[string]$OutCsv,[string]$AzCopyExe)
  Write-Log "Generando inventario remoto (azcopy list)."
  $lines = & $AzCopyExe list $DestUrl --recursive --output-type json 2>&1 | Tee-Object -Variable listRaw
  $rows = New-Object System.Collections.Generic.List[object]
  foreach ($ln in $listRaw) {
    try {
      if ([string]::IsNullOrWhiteSpace($ln)) { continue }
      $o = $ln | ConvertFrom-Json -ErrorAction Stop
      $rows.Add([pscustomobject]@{
        Path         = $o.path ?? $o.name ?? $o.Path ?? $null
        EntityType   = $o.entityType ?? $o.EntityType ?? $null
        Bytes        = $o.contentLength ?? $o.ContentLength ?? $null
        LastModified = $o.lastModified ?? $o.LastModified ?? $null
      }) | Out-Null
    } catch { }
  }
  $rows | Export-Csv -LiteralPath $OutCsv -NoTypeInformation -Encoding UTF8
  Write-Log "Inventario remoto -> $OutCsv"
}

# ---------- Main ----------
$destUrl = Build-DestUrl -StorageAccount $StorageAccount -ShareName $ShareName -DestSubPath $DestSubPath -Sas $Sas
$az = Get-AzCopyExe -AzCopyPath $AzCopyPath

if ($LogDir) { $env:AZCOPY_LOG_LOCATION = $LogDir }

$args = @('copy', $SourceRoot, $destUrl, '--recursive=true', "--overwrite=$Overwrite", '--output-type', 'json', '--output-level','essential','--log-level','INFO')
if ($IncludePaths -and $IncludePaths.Count -gt 0) {
  $inc = ($IncludePaths -join ';')
  $args += @('--include-path', $inc)
}
if ($PreservePermissions) {
  $args += @('--preserve-smb-permissions=true','--preserve-smb-info=true')
}

Write-Log ("Ejecutando: {0} {1}" -f $az, ($args -join ' '))

# Mostrar en consola y capturar salida
$outLines = @()
& $az @args 2>&1 | Tee-Object -Variable outLines | ForEach-Object { $_ } | Out-Host
if ($LASTEXITCODE -ne 0) {
  Write-Log "AzCopy devolvió código $LASTEXITCODE." 'WARN'
}

# Guardar salida cruda para auditoría (local)
$jsonlPath = Join-Path -Path (Resolve-Path '.\').Path -ChildPath ("azcopy_{0:yyyyMMddHHmmss}.jsonl" -f (Get-Date))
$outLines | Set-Content -LiteralPath $jsonlPath -Encoding UTF8
Write-Log "Salida JSONL -> $jsonlPath"

# Extraer JobId
$jobId = $null
foreach ($line in $outLines) {
  try {
    if ([string]::IsNullOrWhiteSpace($line)) { continue }
    $obj = $line | ConvertFrom-Json -ErrorAction Stop
    if ($obj.jobId) { $jobId = $obj.jobId; break }
    if ($obj.JobId) { $jobId = $obj.JobId; break }
  } catch { continue }
}
if (-not $jobId) {
  Write-Log "No se detectó JobId en salida; intentaré 'azcopy jobs list'." 'WARN'
  $jobsLines = & $az jobs list --output-type json 2>&1 | Tee-Object -Variable jobsRaw
  foreach ($j in $jobsRaw) {
    try { $o = $j | ConvertFrom-Json -ErrorAction Stop; if ($o.jobId) { $jobId = $o.jobId; break } } catch {}
  }
}

if ($jobId) {
  Write-Log "JobId detectado: $jobId"
  $lines = & $az jobs show $jobId --with-status=All --output-type json 2>&1 | Tee-Object -Variable jobShowRaw
  $rows = New-Object System.Collections.Generic.List[object]

  foreach ($ln in $jobShowRaw) {
    try {
      if ([string]::IsNullOrWhiteSpace($ln)) { continue }
      $o = $ln | ConvertFrom-Json -ErrorAction Stop

      $path   = $o.path   ?? $o.Path   ?? $o.Destination ?? $o.Source ?? $null
      $status = $o.status ?? $o.Status ?? $o.TransferStatus ?? $null
      $etype  = $o.entityType ?? $o.EntityType ?? $null
      $bytes  = $o.contentLength ?? $o.Size ?? $null
      $lm     = $o.lastModified ?? $null
      $err    = $o.errorMsg ?? $o.ErrorMsg ?? $o.Error ?? $null

      $rows.Add([pscustomobject]@{
        JobId        = $jobId
        RelativePath = $path
        EntityType   = $etype
        Status       = $status
        Bytes        = $bytes
        LastModified = $lm
        Error        = $err
      }) | Out-Null
    } catch { }
  }

  Export-Transfers -Rows $rows -OutCsv $OutCsv
} else {
  Write-Log "No se pudo detectar JobId; no generaré CSV por-archivo." 'WARN'
}

if ($GenerateRemoteInventory) {
  $remoteCsv = [IO.Path]::ChangeExtension($OutCsv, '.remote.csv')
  Export-RemoteInventory -DestUrl $destUrl -OutCsv $remoteCsv -AzCopyExe $az
}

Write-Log "Proceso finalizado."
if ($LogDir)     { Write-Log "Logs nativos de AzCopy en: $LogDir" }
if ($WrapperLog) { Write-Log "Wrapper log en: $WrapperLog" }
