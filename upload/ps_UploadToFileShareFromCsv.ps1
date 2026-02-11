<#
.SYNOPSIS
  Migra archivos/carpetas a Azure File Share preservando ACLs NTFS.
  Copia todo el árbol o solo rutas del CSV (por lotes) con trazabilidad.

.PARAMETER SourceRoot
  Raíz local o UNC (D:\Datos o \\FS01\Share\Area)

.PARAMETER CsvStructurePath
  CSV para copia selectiva (si se usa -FromCsvOnly)

.PARAMETER StorageAccount
  Nombre de la cuenta (sin FQDN)

.PARAMETER ShareName
  Nombre del Azure File Share

.PARAMETER DestSubPath
  Subcarpeta en el share (opcional)

.PARAMETER Sas
  SAS del destino (incluye ?sv=...)

.PARAMETER AccountKey
  Clave de la Storage Account (si no usas SAS)

.PARAMETER FromCsvOnly
  Copia solo las rutas del CSV

.PARAMETER BatchSize
  Nº de rutas por lote (include-path)

.PARAMETER PreservePermissions
  Activa --preserve-smb-permissions/--preserve-smb-info (sin --backup)

.PARAMETER LogDir
  Carpeta base de logs (se crea subcarpeta por corrida)

.PARAMETER AzCopyPath
  Ruta a azcopy.exe (si no está en PATH)

.PARAMETER OverwriteMode
  IfSourceNewer (default) | True | False | Prompt
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
  [string] $AzCopyPath,
  [ValidateSet('IfSourceNewer','True','False','Prompt')]
  [string] $OverwriteMode = 'IfSourceNewer'
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
  ($url -replace '(sig=)[^&]+','${1}***' -replace '(se=)[^&]+','${1}***' -replace '(st=)[^&]+','${1}***')
}

function Parse-SasWindow {
  param([string]$sas)
  if (-not $sas) { return $null }

  $q = $sas.TrimStart('?')
  $map = @{ }
  foreach ($kv in $q -split '&') {
    $parts = $kv -split '=',2
    if ($parts.Count -eq 2) { $map[$parts[0]] = [System.Uri]::UnescapeDataString($parts[1]) }
  }

  $stUtc = $null; $seUtc = $null
  if ($map['st']) { try { $stUtc = ([datetimeoffset]::Parse($map['st'], [Globalization.CultureInfo]::InvariantCulture)).UtcDateTime } catch { $stUtc = $null } }
  if ($map['se']) { try { $seUtc = ([datetimeoffset]::Parse($map['se'], [Globalization.CultureInfo]::InvariantCulture)).UtcDateTime } catch { $seUtc = $null } }

  [pscustomobject]@{
    StartUtc = $stUtc
    ExpiryUtc = $seUtc
    Services = $map['ss']
    Types    = $map['srt']
    Perms    = $map['sp']
  }
}

function Write-Section { param([string]$title) Write-Host "`n=== $title ===" -ForegroundColor Cyan }

# ---------- validaciones básicas ----------
if (!(Test-Path -LiteralPath $SourceRoot)) { throw "SourceRoot no existe o no es accesible: $SourceRoot" }
$azcopy = Resolve-AzCopyPath -PathHint $AzCopyPath
if (-not $azcopy) { throw "AzCopy no está disponible. Pasa -AzCopyPath con la ruta a azcopy.exe (v10+)." }

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
$runDir = Join-Path $LogDir ("run-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
New-Item -ItemType Directory -Path $runDir -Force | Out-Null
$stdOutPath = Join-Path $runDir 'azcopy-stdout.log'

# ---------- construir destino ----------
$baseUrl = "https://$StorageAccount.file.core.windows.net/$ShareName"
if ($DestSubPath) { 
  $DestSubPath = $DestSubPath.Trim('\','/')
  $baseUrl = "$baseUrl/$DestSubPath"
}
$destUrl    = if ($Sas) { "$baseUrl$Sas" } else { $baseUrl }
$destMasked = if ($Sas) { Mask-Sas $destUrl } else { $destUrl }

# ---------- entorno AzCopy ----------
$env:AZCOPY_LOG_LOCATION      = $runDir
$env:AZCOPY_JOB_PLAN_LOCATION = $runDir
$env:AZCOPY_CONCURRENCY_VALUE = "AUTO"
$env:AZCOPY_LOG_LEVEL         = "INFO"      # para logs internos más detallados
$env:AZCOPY_REDACT_SAS        = "true"
if ($AccountKey -and -not $Sas) { $env:AZCOPY_ACCOUNT_KEY = $AccountKey }

# ---------- args comunes ----------
$permArgs = @()
if ($PreservePermissions) {
  $permArgs += "--preserve-smb-permissions=true"
  $permArgs += "--preserve-smb-info=true"
  # (sin --backup, según lo solicitado)
}

$ow = switch ($OverwriteMode) {
  'True'          { 'true' }
  'False'         { 'false' }
  'Prompt'        { 'prompt' }
  default         { 'ifSourceNewer' }
}

$commonArgs = @(
  "--recursive=true",
  "--overwrite=$ow",
  "--check-length=false",
  "--output-level=Essential",   # muestra cada transferencia en consola
  "--log-level=INFO"       # y en los logs internos
) + $permArgs

# ---------- pre-flight ----------
Write-Section "Pre-flight"
try { $ver = & "$azcopy" --version 2>$null; Write-Host ("AzCopy: {0}" -f ($ver -join ' ')) } catch { Write-Host "AzCopy: (no se pudo leer versión)" }
Write-Host ("Ejecutable : {0}" -f $azcopy)
Write-Host ("Origen     : {0}" -f $SourceRoot)
Write-Host ("Destino    : {0}" -f $destMasked)
Write-Host ("Overwrite  : {0}" -f $OverwriteMode)
Write-Host ("Permisos   : {0}" -f ($PreservePermissions ? 'preserve SMB perms + info' : 'NO preservar permisos'))
Write-Host ("Logs en    : {0}" -f $runDir)

# Mostrar ventana del SAS y alertar por reloj
if ($Sas) {
  $sw = Parse-SasWindow -sas $Sas
  if ($sw) {
    $nowUtc = (Get-Date).ToUniversalTime()
    Write-Host ("SAS: ss={0} srt={1} sp={2}" -f $sw.Services, $sw.Types, $sw.Perms)
    Write-Host ("SAS Start (UTC):  {0:yyyy-MM-ddTHH:mm:ssZ}" -f $sw.StartUtc)
    Write-Host ("SAS Expiry (UTC): {0:yyyy-MM-ddTHH:mm:ssZ}" -f $sw.ExpiryUtc)
    Write-Host ("Now (UTC):        {0:yyyy-MM-ddTHH:mm:ssZ}" -f $nowUtc)
    if ($sw.StartUtc -and $nowUtc -lt $sw.StartUtc.AddMinutes(-5)) {
      Write-Warning "El SAS aún no está 'activo' según tu reloj local (st > ahora)."
    }
    if ($sw.ExpiryUtc -and $nowUtc -gt $sw.ExpiryUtc) {
      Write-Warning "El SAS está expirado."
    }
  }
}

# Test rápido de autenticación (SIN enumerar con 'ls')
function Test-AzCopyAuth {
  param([string]$Az, [string]$UrlReal, [string]$UrlMask)
  Write-Section "Test de autenticación (sin listar contenido)"
  Write-Host ("Destino: {0}" -f $UrlMask)
  Write-Host "Se omite 'azcopy ls' para no disparar una enumeración masiva. Los errores de autenticación aparecerán durante la copia."
}
Test-AzCopyAuth -Az $azcopy -UrlReal $destUrl -UrlMask $destMasked

# ---------- ejecución AzCopy ----------
function Invoke-AzCopy {
  param([string[]]$ArgsToUse, [string]$Src, [string]$Dst, [string]$DstMask)
  Write-Section "Comando AzCopy (SAS enmascarado)"
  $cmdMasked = @("`"$azcopy`"",'copy',"`"$Src`"", "`"$DstMask`"") + $ArgsToUse
  Write-Host ($cmdMasked -join ' ')

  Write-Section "Ejecución"
  # Muestra TODO (stdout+stderr) en consola y guarda log combinado
  & "$azcopy" copy "$Src" "$Dst" @ArgsToUse 2>&1 | Tee-Object -FilePath "$stdOutPath"
  $exit = $LASTEXITCODE
  Write-Host "`nExitCode: $exit  (log combinado: $stdOutPath)"

  # Si falló, muestra un extracto útil
  if ($exit -ne 0 -and (Test-Path -LiteralPath $stdOutPath)) {
    Write-Section "Últimas 80 líneas del log combinado"
    Get-Content -LiteralPath $stdOutPath -Tail 80
  }

  if ($exit -ne 0) { throw "AzCopy retornó código $exit" }
}

# --- Modo 1: Tree completo ---
function Start-FullTreeCopy {
  Write-Section "Copia completa"
  Invoke-AzCopy -ArgsToUse $commonArgs -Src $SourceRoot -Dst $destUrl -DstMask $destMasked
}

# --- Modo 2: CSV selectivo ---
function Start-CsvSelectiveCopy {
  if (-not (Test-Path -LiteralPath $CsvStructurePath)) { throw "CSV no encontrado: $CsvStructurePath" }
  Write-Section "CSV selectivo"
  Write-Host "[Cargando] $CsvStructurePath"
  $rows = Import-Csv -LiteralPath $CsvStructurePath
  if (-not $rows -or $rows.Count -eq 0) { Write-Warning "CSV vacío: $CsvStructurePath"; return }

  $candidateCols = @('FilePath','Path','Ruta','FullName','FolderPath')
  $first = $rows[0]
  $col = $candidateCols | Where-Object { $_ -in $first.PSObject.Properties.Name } | Select-Object -First 1
  if (-not $col) { throw "No se detectó columna de ruta en CSV. Esperaba: $($candidateCols -join ', ')" }

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
    Invoke-AzCopy -ArgsToUse ($commonArgs + "--include-path=$inc") -Src $SourceRoot -Dst $destUrl -DstMask $destMasked
  }
}

# ---------- run ----------
try {
  if ($FromCsvOnly) { Start-CsvSelectiveCopy } else { Start-FullTreeCopy }
  Write-Section "OK"
  Write-Host "Migración completada. Logs: $runDir"
}
catch {
  Write-Section "ERROR"
  Write-Error $_.Exception.Message
  Write-Host "Revisa: $stdOutPath"
  throw
}
finally {
  if ($env:AZCOPY_ACCOUNT_KEY) { Remove-Item Env:\AZCOPY_ACCOUNT_KEY -ErrorAction SilentlyContinue }
}
