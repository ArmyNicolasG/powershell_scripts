<#
.SYNOPSIS
  Wrapper para AzCopy que:
   1) Ejecuta la copia (desde un folder local o lista de rutas) hacia Azure Files.
   2) Muestra progreso/essentials en consola.
   3) Captura el JobId y genera un CSV por-archivo con estado (Success/Failed/Started).
   4) (Opcional) Genera inventario remoto usando `azcopy list` para comparación.

.DESCRIPTION
  - Requiere AzCopy v10 en PATH.
  - Usa `--output-type json` (líneas JSON) y `azcopy jobs show --with-status=All --output-type json` para obtener detalle por archivo.
  - Crea logs en %USERPROFILE%\.azcopy o en -LogDir si se define.

.PARAMETER SourceRoot
  Carpeta local raíz que se copiará.

.PARAMETER DestUrl
  URL del file share o carpeta destino (con SAS o identidad que ya funcione). Ej:
  https://<account>.file.core.windows.net/<share>/<carpeta>?<sas>

.PARAMETER IncludePaths
  Lista de rutas RELATIVAS (desde SourceRoot) a incluir. Si se omite, se copia todo (--recursive).

.PARAMETER OutCsv
  CSV por-archivo con columnas: RelativePath, EntityType, Status, Bytes, LastModified, Error.

.PARAMETER LogDir
  Carpeta para logs de AzCopy. Si se omite usa la predeterminada.

.PARAMETER Overwrite
  'ifSourceNewer' (default), 'true' o 'false'.

.PARAMETER GenerateRemoteInventory
  Si se indica, llama `azcopy list` recursivo sobre DestUrl y genera un CSV remoto.

.EXAMPLE
  .\ps_UploadToFileShareFromCsv_v2.ps1 -SourceRoot 'D:\Datos' -DestUrl 'https://acct.file.core.windows.net/share/carpeta?<sas>' `
     -OutCsv .\resultado_azcopy.csv -LogDir 'D:\Logs\AzCopy' -GenerateRemoteInventory
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory)] [string]$SourceRoot,
  [Parameter(Mandatory)] [string]$DestUrl,
  [string[]]$IncludePaths,
  [string]$OutCsv = ".\resultado_azcopy.csv",
  [string]$LogDir,
  [ValidateSet('ifSourceNewer','true','false')][string]$Overwrite = 'ifSourceNewer',
  [switch]$GenerateRemoteInventory
)

function Invoke-AzCopyCopy {
  param(
    [string]$SourceRoot,
    [string]$DestUrl,
    [string[]]$IncludePaths,
    [string]$Overwrite,
    [string]$LogDir
  )
  if ($LogDir) { $env:AZCOPY_LOG_LOCATION = $LogDir }

  $args = @('copy', $SourceRoot, $DestUrl, '--recursive=true', "--overwrite=$Overwrite", '--output-type', 'json', '--output-level','essential','--log-level','INFO')
  if ($IncludePaths -and $IncludePaths.Count -gt 0) {
    $inc = ($IncludePaths -join ';')
    $args += @('--include-path', $inc)
  }

  Write-Host ">>> azcopy $($args -join ' ')" -ForegroundColor Cyan
  $outLines = & azcopy @args 2>&1
  if ($LASTEXITCODE -ne 0) {
    Write-Warning "AzCopy devolvió código $LASTEXITCODE. Revisa los logs en $($env:AZCOPY_LOG_LOCATION)"
  }

  # Guardar salida cruda para auditoría
  $jsonlPath = Join-Path -Path (Resolve-Path '.\').Path -ChildPath ("azcopy_{0:yyyyMMddHHmmss}.jsonl" -f (Get-Date))
  $outLines | Set-Content -LiteralPath $jsonlPath -Encoding UTF8
  Write-Host "Salida JSONL guardada en: $jsonlPath"

  # Extraer JobId (buscamos clave jobId en alguna línea JSON)
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
    Write-Warning "No pude detectar JobId en la salida. Intentando listar el ultimo job"
    # fallback: tomar el primer job de 'azcopy jobs list'
    $jobs = (& azcopy jobs list --output-type json) | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_ | ConvertFrom-Json } 
    $jobId = $jobs[0].jobId
  }
  return $jobId
}

function Export-JobTransfersToCsv {
  param([string]$JobId,[string]$OutCsv)

  $lines = & azcopy jobs show $JobId --with-status=All --output-type json 2>&1
  $rows = New-Object System.Collections.Generic.List[object]

  foreach ($ln in $lines) {
    try {
      if ([string]::IsNullOrWhiteSpace($ln)) { continue }
      $o = $ln | ConvertFrom-Json -ErrorAction Stop

      # Intentar mapear propiedades típicas;  fallback genérico si cambian
      $path   = $o.path   ?? $o.Path   ?? $o.Destination ?? $o.Source ?? $null
      $status = $o.status ?? $o.Status ?? $o.TransferStatus ?? $null
      $etype  = $o.entityType ?? $o.EntityType ?? $null
      $bytes  = $o.contentLength ?? $o.Size ?? $null
      $lm     = $o.lastModified ?? $null
      $err    = $o.errorMsg ?? $o.ErrorMsg ?? $o.Error ?? $null

      if (-not $path -and $o -is [string]) { $path = $o } # ultra-fallback

      $rows.Add([pscustomobject]@{
        JobId        = $JobId
        RelativePath = $path
        EntityType   = $etype
        Status       = $status
        Bytes        = $bytes
        LastModified = $lm
        Error        = $err
      }) | Out-Null
    } catch {
      # ignora líneas no-JSON
    }
  }

  if ($rows.Count -eq 0) {
    Write-Warning "No se detectaron transfers en el job $JobId. ¿Se ejecutó correctamente?"
  }

  $rows | Export-Csv -LiteralPath $OutCsv -NoTypeInformation -Encoding UTF8
  Write-Host "✅ CSV de transfers: $OutCsv"
}

function Export-RemoteInventory {
  param([string]$DestUrl,[string]$OutCsv)
  # Usamos azcopy list recursivo con JSON para obtener arbol remoto
  $lines = & azcopy list $DestUrl --recursive --output-type json 2>&1
  $rows = New-Object System.Collections.Generic.List[object]
  foreach ($ln in $lines) {
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
  Write-Host "✅ Inventario remoto: $OutCsv"
}

# -------- Execute --------

$jobId = Invoke-AzCopyCopy -SourceRoot $SourceRoot -DestUrl $DestUrl -IncludePaths $IncludePaths -Overwrite $Overwrite -LogDir $LogDir
if ($jobId) {
  Write-Host "JobId: $jobId"
  Export-JobTransfersToCsv -JobId $jobId -OutCsv $OutCsv
} else {
  Write-Warning "No se pudo obtener JobId; no generaré CSV por-archivo."
}

if ($GenerateRemoteInventory) {
  $remoteCsv = [IO.Path]::ChangeExtension($OutCsv, '.remote.csv')
  Export-RemoteInventory -DestUrl $DestUrl -OutCsv $remoteCsv
}
