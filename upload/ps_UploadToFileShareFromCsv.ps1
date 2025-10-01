<#
.SYNOPSIS
  Migra archivos/carpetas a Azure File Share preservando ACLs NTFS.
  Copia todo el árbol o solo rutas listadas en un CSV (por lotes) con trazabilidad.

.PARAMETER AzCopyPath
  Ruta absoluta a azcopy.exe. Si no se especifica, se busca en PATH.
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)] [string] $SourceRoot,
  [string] $CsvStructurePath,
  [Parameter(Mandatory=$true)] [string] $StorageAccount,
  [Parameter(Mandatory=$true)] [string] $ShareName,
  [string] $DestSubPath,
  [string] $Sas,
  [string] $AccountKey,
  [switch] $FromCsvOnly,
  [int] $BatchSize = 2000,
  [bool] $PreservePermissions = $true,
  [string] $LogDir = $(Join-Path -Path (Join-Path -Path (Get-Location) -ChildPath "logs") -ChildPath (Get-Date -Format "yyyyMMdd-HHmmss")),
  [string] $AzCopyPath
)

# ---------- utilidades ----------
function Resolve-AzCopyPath {
  param([string]$PathHint)
  if ($PathHint) {
    if (!(Test-Path -LiteralPath $PathHint)) { throw "AzCopyPath no existe: $PathHint" }
    return (Resolve-Path -LiteralPath $PathHint).Path
  }
  $cmd = Get-Command azcopy.exe -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }
  return $null
}
function Mask-Sas {
  param([string]$url)
  if (-not $url) { return $null }
  # enmascara 'sig=' y recorta un poco los otros parámetros
  $masked = $url -replace '(sig=)[^&]+', '${1}***'
  $masked = $masked -replace '(se=)[^&]+','${1}***'
  $masked = $masked -replace '(st=)[^&]+','${1}***'
  return $masked
}
function Write-Section { param([string]$title) Write-Host "`n=== $title ===" -ForegroundColor Cyan }

# ---------- validaciones básicas ----------
if (!(Test-Path -LiteralPath $SourceRoot)) { throw "SourceRoot no existe o no es accesible: $SourceRoot" }
$azcopy = Resolve-AzCopyPath -PathHint $AzCopyPath
if (-not $azcopy) { throw "AzCopy no está instalado/en PATH. Pasa -AzCopyPath con la ruta a azcopy.exe (v10+)." }

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
$runIdDir = Join-Path $LogDir ("run-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
New-Item -ItemType Directory -Path $runIdDir -Force | Out-Null
$stdOutPath = Join-Path $runIdDir 'azcopy-stdout.log'
$stdErrPath = Join-Path $runIdDir 'azcopy-stderr.log'

# ---------- construir destino ----------
$baseUrl = "https://$StorageAccount.file.core.windows.net/$ShareName"
if ($DestSubPath) { 
  $DestSubPath = $DestSubPath.Trim('\','/')
  $baseUrl = "$baseUrl/$DestSubPath"
}
$destUrl  = if ($Sas) { "$baseUrl$Sas" } else { $baseUrl }
$destUrlMasked = if ($Sas) { Mask-Sas $destUrl } else { $destUrl }

# ---------- preparar entorno AzCopy ----------
$env:AZCOPY_LOG_LOCATION      = $runIdDir
$env:AZCOPY_JOB_PLAN_LOCATION = $runIdDir
$env:AZCOPY_CONCURRENCY_VALUE = "AUTO"
if ($AccountKey -and -not $Sas) { $env:AZCOPY_ACCOUNT_KEY = $AccountKey }

# ---------- args comunes ----------
$permArgs = @()
if ($PreservePermissions) {
  $permArgs += "--preserve-smb-permissions=true"
  $permArgs += "--preserve-smb-info=true"
  $permArgs += "--backup"
}
$commonArgs = @(
  "--recursive=true",
  "--overwrite=ifSourceNewer",
  "--check-length=true",
  "--output-level=Essential"
) + $permArgs

# ---------- pre-flight ----------
Write-Section "Pre-flight"
try { 
  $ver = & "$azcopy" --version 2>$null
  Write-Host ("AzCopy: {0}" -f ($ver -join ' ')) 
} catch { Write-Host "AzCopy: (no se pudo leer versión)" }
Write-Host ("Ejecutable : {0}" -f $azcopy)
Write-Host ("Origen     : {0}" -f $SourceRoot)
Write-Host ("Destino    : {0}" -f $destUrlMasked)
Write-Host ("Share      : {0}/{1}" -f $StorageAccount, $ShareName)
Write-Host ("Subcarpeta : {0}" -f ($DestSubPath ? $DestSubPath : '/'))
Write-Host ("Permisos   : {0}" -f ($PreservePermissions ? 'preserve SMB perms + info + backup' : 'NO preservar permisos'))
Write-Host ("LogDir     : {0}" -f $runIdDir)

# ---------- funciones de copia ----------
function Invoke-AzCopy {
  param([string[]]$ArgsToUse)
  $cmdMasked = @("`"$azcopy`"", 'copy', "`"$SourceRoot`"", "`"$destUrlMasked`"") + $ArgsToUse
  Write-Section "Comando AzCopy (SAS enmascarado)"
  Write-Host ($cmdMasked -join ' ')

  Write-Section "Ejecución"
  # Ejecutar, capturando stdout/stderr
  & "$azcopy" copy "$SourceRoot" "$destUrl" @ArgsToUse 2> "$stdErrPath" | Tee-Object -FilePath "$stdOutPath"
  $exit = $LASTEXITCODE
  Write-Host "`nExitCode: $exit  (stdout: $stdOutPath  / stderr: $stdErrPath)"

  # Intentar hallar el log interno de AzCopy y mostrar tail si hubo error
  $azLog = Select-String -Path $stdOutPath -Pattern 'Log file is located at:\s*(.*)$' | Select-Object -Last 1
  if ($azLog) {
    $logPath = $azLog.Matches[0].Groups[1].Value.Trim()
    Write-Host "AzCopy log: $logPath"
    if ($exit -ne 0 -and (Test-Path -LiteralPath $logPath)) {
      Write-Section "Últimas 60 líneas del log de AzCopy"
      Get-Content -LiteralPath $logPath -Tail 60
    }
  } else {
    Write-Host "No se detectó ruta del log interno en stdout."
  }

  if ($exit -ne 0) { throw "AzCopy retornó código $exit" }
}

function Start-FullTreeCopy {
  Write-Section "Copia completa"
  Invoke-AzCopy -ArgsToUse $commonArgs
}

function Start-CsvSelectiveCopy {
  if (-not (Test-Path -LiteralPath $CsvStructurePath)) { throw "CSV no encontrado: $CsvStructurePath" }
  Write-Section "CSV selectivo"
  Write-Host "[Cargando] $CsvStructurePath"
  $rows = Import-Csv -LiteralPath $CsvStructurePath
  if (-not $rows -or $rows.Count -eq 0) { Write-Warning "CSV vacío: $CsvStructurePath"; return }

  $candidateCols = @('FilePath','Path','Ruta','FullName','FolderPath')
  $first = $rows[0]
  $col = $candidateCols | Where-Object { $_ -in $first.PSObject.Properties.Name }
  if (-not $col) { throw "No se detectó columna de ruta en CSV. Esperaba: $($candidateCols -join ', ')" }
  $col = $col[0]

  # Normalizar a rutas relativas
  $allPaths = foreach ($r in $rows) {
    $p = $r.$col; if ([string]::IsNullOrWhiteSpace($p)) { continue }
    $p = $p -replace '[\\/]+','\'
    if ($p.StartsWith($SourceRoot, [StringComparison]::OrdinalIgnoreCase)) {
      $rel = $p.Substring($SourceRoot.Length).TrimStart('\','/')
      if ($rel) { $rel }
    }
  }
  $relPaths = $allPaths | Sort-Object -Unique
  if (-not $relPaths) { Write-Warning "El CSV no contiene rutas bajo $SourceRoot"; return }

  # Batching
  $batches = [System.Collections.Generic.List[Object]]::new()
  $current = New-Object System.Collections.Generic.List[string]
  foreach ($item in $relPaths) {
    $current.Add($item)
    if ($current.Count -ge $BatchSize) { $batches.Add($current); $current = New-Object System.Collections.Generic.List[string] }
  }
  if ($current.Count -gt 0) { $batches.Add($current) }

  Write-Host ("Rutas únicas: {0} | Tamaño lote: {1} | Nº de lotes: {2}" -f $relPaths.Count, $BatchSize, $batches.Count)

  $i = 0
  foreach ($batch in $batches) {
    $i++
    $inc = ($batch -join ';')
    Write-Section ("Lote {0}/{1} — Rutas: {2}" -f $i, $batches.Count, $batch.Count)
    Invoke-AzCopy -ArgsToUse ($commonArgs + "--include-path=$inc")
  }
}

# ---------- ejecución ----------
try {
  if ($FromCsvOnly) { Start-CsvSelectiveCopy } else { Start-FullTreeCopy }
  Write-Section "OK"
  Write-Host "Migración completada. Carpeta de ejecución/logs: $runIdDir"
}
catch {
  Write-Section "ERROR"
  Write-Error $_.Exception.Message
  Write-Host "Revisa: $stdOutPath  y  $stdErrPath  (y el log interno que se imprimió arriba)."
  throw
}
finally {
  if ($env:AZCOPY_ACCOUNT_KEY) { Remove-Item Env:\AZCOPY_ACCOUNT_KEY -ErrorAction SilentlyContinue }
}
