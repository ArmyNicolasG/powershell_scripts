<#
.SYNOPSIS
  Migra archivos/carpetas a Azure File Share preservando ACLs NTFS.
  Puede copiar todo el árbol (más eficiente) o solo las rutas listadas en un CSV.

.PARAMETER SourceRoot
  Carpeta raíz local o UNC del origen (ej. D:\Datos o \\FS01\Compartido\Area).

.PARAMETER CsvStructurePath
  (Opcional) CSV salido de ps_GetFilesParameters.ps1. Si se usa -FromCsvOnly, filtra qué copiar.

.PARAMETER StorageAccount
  Nombre de la Storage Account de Azure (sin FQDN).

.PARAMETER ShareName
  Nombre del Azure File Share de destino.

.PARAMETER DestSubPath
  (Opcional) Subcarpeta en el share (ej. Proyectos2025). Si no se da, copia a la raíz del share.

.PARAMETER Sas
  (Opcional) SAS del share (recomendado). Formato ?sv=...; no incluyas la URL.

.PARAMETER AccountKey
  (Opcional) Clave de la Storage Account. Úsala si no pasas SAS.

.PARAMETER FromCsvOnly
  Si se especifica, copia únicamente las rutas que aparezcan en el CSV (archivos y/o carpetas).

.PARAMETER BatchSize
  Cantidad de rutas a incluir por lote cuando se usa -FromCsvOnly (por defecto 2000).

.PARAMETER PreservePermissions
  Por defecto $true. Controla --preserve-smb-permissions/--preserve-smb-info.

.PARAMETER LogDir
  Carpeta para logs de AzCopy y journal. Por defecto .\logs\{fecha-hora}.

.PARAMETER AzCopyPath
  (Opcional) Ruta absoluta a azcopy.exe. Si no se especifica, se buscará en PATH.

.EXAMPLE
  .\Invoke-AzureFilesMigration.ps1 `
    -SourceRoot "D:\Datos" `
    -StorageAccount "contosofiles01" -ShareName "proyectos" `
    -Sas "?sv=2024-08-04&ss=f&srt=sco&sp=rwdlcx..." `
    -AzCopyPath "C:\Tools\azcopy\azcopy.exe" `
    -LogDir "D:\MigraLogs"

.EXAMPLE
  # Copiar solo lo que está listado en el CSV
  .\Invoke-AzureFilesMigration.ps1 `
    -SourceRoot "\\FS01\Compartido\Contabilidad" `
    -CsvStructurePath "C:\out\estructura.csv" -FromCsvOnly `
    -StorageAccount "contosofiles01" -ShareName "proyectos" -Sas "?sv=..."
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)] [string] $SourceRoot,
  [Parameter(Mandatory=$false)] [string] $CsvStructurePath,
  [Parameter(Mandatory=$true)] [string] $StorageAccount,
  [Parameter(Mandatory=$true)] [string] $ShareName,
  [Parameter(Mandatory=$false)] [string] $DestSubPath,
  [Parameter(Mandatory=$false)] [string] $Sas,
  [Parameter(Mandatory=$false)] [string] $AccountKey,
  [switch] $FromCsvOnly,
  [int] $BatchSize = 2000,
  [bool] $PreservePermissions = $true,
  [string] $LogDir = $(Join-Path -Path (Join-Path -Path (Get-Location) -ChildPath "logs") -ChildPath (Get-Date -Format "yyyyMMdd-HHmmss")),
  [string] $AzCopyPath   # <-- NUEVO
)

# --- Validaciones ------------------------------------------------------------
if (!(Test-Path -LiteralPath $SourceRoot)) { throw "SourceRoot no existe: $SourceRoot" }

# Resolver ruta de azcopy.exe (usar parámetro si viene)
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

$azcopy = Resolve-AzCopyPath -PathHint $AzCopyPath
if (-not $azcopy) { throw "AzCopy no está instalado/en PATH. Pasa -AzCopyPath con la ruta a azcopy.exe (v10+)." }

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

# Construir URL destino
$baseUrl = "https://$StorageAccount.file.core.windows.net/$ShareName"
if ($DestSubPath) { 
  $DestSubPath = $DestSubPath.Trim('\','/')
  $baseUrl = "$baseUrl/$DestSubPath"
}

# Autenticación
$env:AZCOPY_LOG_LOCATION = $LogDir
$env:AZCOPY_JOB_PLAN_LOCATION = $LogDir
$env:AZCOPY_CONCURRENCY_VALUE = "AUTO"  # AzCopy decide; puedes fijar p.ej. 32

if ($AccountKey -and -not $Sas) {
  # Con Account Key
  $env:AZCOPY_ACCOUNT_KEY = $AccountKey
}

# Parámetros comunes AzCopy
$permArgs = @()
if ($PreservePermissions) {
  $permArgs += "--preserve-smb-permissions=true"
  $permArgs += "--preserve-smb-info=true"
  # Permite copiar archivos con atributos de sistema/ocultos/backup
  $permArgs += "--backup"
}

$commonArgs = @(
  "--recursive=true",
  "--overwrite=ifSourceNewer",
  "--check-length=true",
  "--output-level=Essential"
) + $permArgs

# Destino final (SAS o no)
$destUrl = if ($Sas) { "$baseUrl$Sas" } else { $baseUrl }

# --- Modo 1: Copiar todo el árbol (más eficiente) ----------------------------
function Start-FullTreeCopy {
  Write-Host "[AzCopy] Copiando TODO el árbol desde $SourceRoot -> $destUrl"
  & "$azcopy" copy $SourceRoot $destUrl @commonArgs
  if ($LASTEXITCODE -ne 0) { throw "AzCopy retornó código $LASTEXITCODE" }
}

# --- Modo 2: Copiar sólo rutas del CSV (batches) ----------------------------
function Start-CsvSelectiveCopy {
  if (-not (Test-Path -LiteralPath $CsvStructurePath)) { throw "CSV no encontrado: $CsvStructurePath" }

  Write-Host "[AzCopy] Cargando CSV de estructura: $CsvStructurePath"
  $rows = Import-Csv -LiteralPath $CsvStructurePath

  # Las columnas pueden variar. Intentamos detectar columna de ruta:
  $candidateCols = @('FilePath','Path','Ruta','FullName','FolderPath')
  $col = $candidateCols | Where-Object { $_ -in $rows[0].PSObject.Properties.Name }
  if (-not $col) { throw "No se detectó columna de ruta en CSV. Esperaba una de: $($candidateCols -join ', ')" }
  $col = $col[0]

  # Normalizar a rutas relativas respecto a SourceRoot
  $allPaths = foreach ($r in $rows) {
    $p = $r.$col
    if ([string]::IsNullOrWhiteSpace($p)) { continue }
    # Convertir slash invertido duplicado, etc.
    $p = $p -replace '[\\/]+','\'
    if ($p.StartsWith($SourceRoot, [StringComparison]::OrdinalIgnoreCase)) {
      $rel = $p.Substring($SourceRoot.Length).TrimStart('\','/')
      if ($rel) { $rel }
    }
  }

  # Limpiar duplicados y dividir en lotes
  $relPaths = $allPaths | Sort-Object -Unique
  if (-not $relPaths) { Write-Warning "El CSV no contiene rutas bajo $SourceRoot"; return }

  # AzCopy puede incluir varias rutas con --include-path separadas por ';'
  $batches = [System.Collections.Generic.List[Object]]::new()
  $current = New-Object System.Collections.Generic.List[String]
  foreach ($item in $relPaths) {
    $current.Add($item)
    if ($current.Count -ge $BatchSize) {
      $batches.Add($current)
      $current = New-Object System.Collections.Generic.List[String]
    }
  }
  if ($current.Count -gt 0) { $batches.Add($current) }

  $i = 0
  foreach ($batch in $batches) {
    $i++
    $inc = ($batch -join ';')
    Write-Host "[AzCopy] Lote $i/$($batches.Count) - Rutas: $($batch.Count)"
    & "$azcopy" copy $SourceRoot $destUrl @commonArgs --include-path="$inc"
    if ($LASTEXITCODE -ne 0) { throw "AzCopy (lote $i) retornó código $LASTEXITCODE" }
  }
}

# --- Ejecución ---------------------------------------------------------------
try {
  if ($FromCsvOnly) { Start-CsvSelectiveCopy } else { Start-FullTreeCopy }
  Write-Host "`n[OK] Migración completada. Logs: $LogDir"
}
catch {
  Write-Error $_.Exception.Message
  throw
}
finally {
  # Limpieza sensible de credenciales en variables de entorno
  if ($env:AZCOPY_ACCOUNT_KEY) { Remove-Item Env:\AZCOPY_ACCOUNT_KEY -ErrorAction SilentlyContinue }
}
