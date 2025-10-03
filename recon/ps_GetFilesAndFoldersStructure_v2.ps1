<#
.SYNOPSIS
  Inventario rápido de archivos y carpetas con verificación de accesibilidad (sin tamaños).
  v2.2:
    - Corrige "Cannot append CSV ... mismatched properties" ajustando al header existente.
    - Opción de logging a archivo + consola.
    - Progreso en consola. Verbose por elemento.

.DESCRIPTION
  - Recorre el árbol en BFS (cola), sin recursion profunda.
  - Para FOLDERS: comprueba si se pueden listar elementos inmediatos (permiso LIST) con enumeración .NET (1 paso).
  - Para FILES: intenta leer atributos (prueba liviana).
  - Omite puntos de reanálisis (symlinks/junctions) por defecto para evitar loops.
  - CSV en streaming por lotes (memoria acotada). Si el CSV existe, respeta sus columnas.

.PARAMETER Path
  Ruta local o UNC.

.PARAMETER OutCsv
  Ruta del CSV (UTF-8). Si se omite, emite al pipeline.

.PARAMETER LogPath
  Archivo .log para registrar eventos (además de la consola).

.PARAMETER Depth
  Profundidad máxima (-1 = ilimitado).

.PARAMETER IncludeFiles
  Incluir archivos (default True).

.PARAMETER IncludeFolders
  Incluir carpetas (default True).

.PARAMETER SkipReparsePoints
  Saltar reparse points (default True).

.PARAMETER Utc
  Fechas en UTC.

.EXAMPLE
  .\ps_GetFilesAndFoldersStructure_v2.2.ps1 -Path "D:\Datos" -OutCsv .\inventario_local.csv -LogPath .\inventario.log -Utc -Verbose
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory)]
  [string]$Path,
  [string]$OutCsv,
  [string]$LogPath,
  [int]$Depth = -1,
  [switch]$IncludeFiles = $true,
  [switch]$IncludeFolders = $true,
  [switch]$SkipReparsePoints = $true,
  [switch]$Utc
)

# ---------- Helpers ----------
function Write-Log {
  param([string]$Message,[string]$Level='INFO')
  $ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss.fff')
  $line = "[$ts][$Level] $Message"
  Write-Host $line
  if ($LogPath) { Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8 }
}

function Resolve-LongPath {
  param([string]$InputPath)
  if ($IsWindows) {
    if ($InputPath -match '^[A-Za-z]:\\') { return "\\?\$InputPath" }
    if ($InputPath -like '\\*') { return "\\?\UNC\{0}" -f $InputPath.TrimStart('\') }
  }
  return $InputPath
}

function Get-EnumOptions {
  $opts = [System.IO.EnumerationOptions]::new()
  $opts.RecurseSubdirectories     = $false
  $opts.ReturnSpecialDirectories  = $false
  $opts.IgnoreInaccessible        = $false   # queremos capturar errores
  $opts.AttributesToSkip          = [System.IO.FileAttributes]::Offline -bor [System.IO.FileAttributes]::Temporary -bor [System.IO.FileAttributes]::Device
  return $opts
}

function Test-DirReadable {
  param([string]$DirPath)
  try {
    $enum = [System.IO.Directory]::EnumerateFileSystemEntries($DirPath, '*', (Get-EnumOptions))
    $e = $enum.GetEnumerator()
    $null = $e.MoveNext()   # valida LIST en la carpeta
    $e.Dispose()
    return @{ OK = $true; Error = $null }
  } catch {
    return @{ OK = $false; Error = $_.Exception.Message }
  }
}

function Get-AttributesSafe {
  param([string]$AnyPath)
  try {
    return @{ OK = $true; Attr = [System.IO.File]::GetAttributes($AnyPath); Error = $null }
  } catch {
    return @{ OK = $false; Attr = $null; Error = $_.Exception.Message }
  }
}

function Format-DateUtcOpt {
  param([DateTime]$dt,[switch]$Utc)
  if ($Utc) { return $dt.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss.fffZ') }
  else      { return $dt.ToString('yyyy-MM-ddTHH:mm:ss.fffK') }
}

# CSV schema handling
$defaultColumns = @('Type','Path','Name','Parent','LastWriteTime','UserHasAccess','AccessStatus','AccessError')
$csvColumns = $defaultColumns.Clone()

if ($OutCsv -and (Test-Path -LiteralPath $OutCsv)) {
  try {
    $header = Get-Content -LiteralPath $OutCsv -First 1 -ErrorAction Stop
    if ($header -and ($header -notmatch '^#TYPE')) {
      # columnas existentes (sin comillas en nuestro caso)
      $existing = $header -split ',' | ForEach-Object { $_.Trim('"') }
      if ($existing.Count -gt 1) { $csvColumns = $existing }
    }
    Write-Log "CSV existente detectado. Usaré columnas: $([string]::Join(',', $csvColumns))"
  } catch {
    Write-Log "No pude leer header de CSV existente, usaré columnas por defecto." 'WARN'
  }
} else {
  if ($OutCsv) {
    # crear/limpiar log
    if ($LogPath) { Remove-Item -LiteralPath $LogPath -ErrorAction SilentlyContinue; New-Item -ItemType File -Path $LogPath -Force | Out-Null }
  }
}

$batch   = New-Object System.Collections.Generic.List[object]
$BATCH_SIZE = 1000

function Flush-Batch {
  if ($batch.Count -eq 0) { return }
  if ($OutCsv) {
    $exists = Test-Path -LiteralPath $OutCsv
    # Forzar orden/shape de columnas para evitar "mismatched properties"
    $batch | Select-Object -Property $csvColumns | Export-Csv -LiteralPath $OutCsv -Append:$exists -NoTypeInformation -Encoding utf8
  } else {
    $batch
  }
  $batch.Clear()
}

function Add-Row {
  param([hashtable]$Row)
  # Asegura que el objeto tenga todas las columnas (faltantes -> $null); extra -> se ignoran en Select-Object
  $ordered = [ordered]@{}
  foreach ($c in $csvColumns) { $ordered[$c] = $(if ($Row.ContainsKey($c)) { $Row[$c] } else { $null }) }
  $obj = [pscustomobject]$ordered
  $batch.Add($obj) | Out-Null
  if ($batch.Count -ge $BATCH_SIZE) { Flush-Batch }
  if ($PSBoundParameters['Verbose']) {
    Write-Verbose ("{0}: {1} [{2}]" -f $obj.Type, $obj.Path, $obj.AccessStatus)
  }
}

# ---------- Main ----------
try {
  $resolved = Resolve-Path -LiteralPath $Path -ErrorAction Stop | Select-Object -ExpandProperty Path
} catch {
  Write-Log "Ruta no encontrada: $Path" 'ERROR'
  throw
}
$rootLP = Resolve-LongPath -InputPath $resolved
Write-Log "Inicio inventario en: $resolved"

$queue = [System.Collections.Generic.Queue[string]]::new()
$queue.Enqueue($rootLP)

$visited = 0
$rootDepth = ($rootLP -split '[\\/]').Length

while ($queue.Count -gt 0) {
  $dir = $queue.Dequeue()
  $visited++

  # Progreso
  Write-Progress -Activity "Inventariando..." -Status $dir -PercentComplete -1

  # Depth control
  $depthNow = ($dir -split '[\\/]').Length - $rootDepth
  if ($Depth -ge 0 -and $depthNow -gt $Depth) { continue }

  $attrInfo = Get-AttributesSafe -AnyPath $dir
  $isReparse = $false
  if ($attrInfo.OK -and ($attrInfo.Attr -band [System.IO.FileAttributes]::ReparsePoint)) { $isReparse = $true }

  if ($IncludeFolders) {
    if ($SkipReparsePoints -and $isReparse) {
      Add-Row @{
        Type='Folder'; Path=$dir; Name=[System.IO.Path]::GetFileName($dir.TrimEnd('\'));
        Parent=[System.IO.Path]::GetDirectoryName($dir);
        LastWriteTime = (Format-DateUtcOpt (Get-Item -LiteralPath $dir -Force).LastWriteTime -Utc:$Utc);
        UserHasAccess=$false; AccessStatus='SKIPPED_REPARSE'; AccessError=$null
      }
      continue
    }

    $check = Test-DirReadable -DirPath $dir
    Add-Row @{
      Type='Folder'; Path=$dir; Name=[System.IO.Path]::GetFileName($dir.TrimEnd('\'));
      Parent=[System.IO.Path]::GetDirectoryName($dir);
      LastWriteTime = (Format-DateUtcOpt (Get-Item -LiteralPath $dir -Force).LastWriteTime -Utc:$Utc);
      UserHasAccess=$check.OK; AccessStatus=($check.OK ? 'OK' : 'DENIED'); AccessError=$check.Error
    }
    if (-not $check.OK) {
      Write-Log "DENIED: $dir - $($check.Error)" 'WARN'
      continue
    }
  }

  # Enumerar hijos inmediatos solo si pudimos leer el folder
  try {
    $entries = [System.IO.Directory]::EnumerateFileSystemEntries($dir, '*', (Get-EnumOptions))
    foreach ($entry in $entries) {
      $attr = Get-AttributesSafe -AnyPath $entry
      if (-not $attr.OK) {
        if ($IncludeFiles) {
          Add-Row @{
            Type='File'; Path=$entry; Name=[System.IO.Path]::GetFileName($entry);
            Parent=[System.IO.Path]::GetDirectoryName($entry);
            LastWriteTime = $null;
            UserHasAccess=$false; AccessStatus='ATTR_DENIED'; AccessError=$attr.Error
          }
          Write-Log "ATTR_DENIED: $entry - $($attr.Error)" 'WARN'
        }
        continue
      }

      $isDir = ($attr.Attr -band [System.IO.FileAttributes]::Directory)
      $isReparseChild = ($attr.Attr -band [System.IO.FileAttributes]::ReparsePoint)

      if ($isDir) {
        if ($SkipReparsePoints -and $isReparseChild) {
          if ($IncludeFolders) {
            Add-Row @{
              Type='Folder'; Path=$entry; Name=[System.IO.Path]::GetFileName($entry.TrimEnd('\'));
              Parent=[System.IO.Path]::GetDirectoryName($entry);
              LastWriteTime = (Format-DateUtcOpt (Get-Item -LiteralPath $entry -Force).LastWriteTime -Utc:$Utc);
              UserHasAccess=$false; AccessStatus='SKIPPED_REPARSE'; AccessError=$null
            }
          }
          continue
        }
        # Enqueue para inspeccionar su accesibilidad (no contamos elementos aquí).
        $queue.Enqueue($entry)
      } else {
        if ($IncludeFiles) {
          $fi = Get-Item -LiteralPath $entry -Force
          Add-Row @{
            Type='File'; Path=$entry; Name=$fi.Name; Parent=$fi.DirectoryName;
            LastWriteTime = (Format-DateUtcOpt $fi.LastWriteTime -Utc:$Utc);
            UserHasAccess=$true; AccessStatus='OK'; AccessError=$null
          }
        }
      }
    }
  } catch {
    if ($IncludeFolders) {
      Add-Row @{
        Type='Folder'; Path=$dir; Name=[System.IO.Path]::GetFileName($dir.TrimEnd('\'));
        Parent=[System.IO.Path]::GetDirectoryName($dir);
        LastWriteTime = (Format-DateUtcOpt (Get-Item -LiteralPath $dir -Force).LastWriteTime -Utc:$Utc);
        UserHasAccess=$false; AccessStatus='ENUMERATION_ERROR'; AccessError=$_.Exception.Message
      }
      Write-Log "ENUMERATION_ERROR: $dir - $($_.Exception.Message)" 'WARN'
    }
  }
}

Flush-Batch
Write-Log "Inventario completado. Visitados: $visited"
if ($OutCsv) { Write-Log "CSV -> $OutCsv" }
if ($LogPath) { Write-Log "LOG -> $LogPath" }
