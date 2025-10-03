<#
.SYNOPSIS
  Inventario rápido de archivos y carpetas con verificación de accesibilidad (sin tamaños).

.DESCRIPTION
  - Recorre el árbol en BFS (cola) para evitar recursion profunda.
  - Lista TODOS los folders y archivos debajo de -Path.
  - Para cada FOLDER intenta enumerar su contenido para decidir si es accesible (UserHasAccess=True/False).
  - Para cada FILE marca accesible si se pueden leer sus atributos (prueba liviana).
  - Omite puntos de reanálisis (symlinks/junctions) por defecto para evitar bucles y errores.
  - Escribe CSV en streaming para alto volumen.

.PARAMETER Path
  Ruta local o UNC (ej. D:\Datos o \\Servidor\Share\Carpeta).

.PARAMETER OutCsv
  Ruta del CSV de salida (UTF-8). Si se omite, emite objetos al pipeline.

.PARAMETER Depth
  Profundidad máxima (-1 = ilimitado, 0 = solo la raíz).

.PARAMETER IncludeFiles
  Incluir archivos en el inventario (default: True).

.PARAMETER IncludeFolders
  Incluir carpetas en el inventario (default: True).

.PARAMETER SkipReparsePoints
  Saltar puntos de reanálisis (symlinks/junctions).

.PARAMETER Utc
  Fechas en UTC (si no, hora local).

.EXAMPLE
  .\ps_GetFilesAndFoldersStructure_v2.ps1 -Path "D:\Datos" -OutCsv .\inventario_local.csv -Utc

.NOTES
  Requiere PowerShell 7+. Probado en 7.5+
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory)]
  [string]$Path,
  [string]$OutCsv,
  [int]$Depth = -1,
  [switch]$IncludeFiles = $true,
  [switch]$IncludeFolders = $true,
  [switch]$SkipReparsePoints = $true,
  [switch]$Utc
)

# ---------- Helpers ----------

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
    $null = $e.MoveNext()   # esto valida permisos de LIST en la carpeta
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

# CSV streaming helpers
$columns = @('Type','Path','Name','Parent','LastWriteTime','UserHasAccess','AccessStatus','AccessError')
$batch   = New-Object System.Collections.Generic.List[object]
$BATCH_SIZE = 1000

function Flush-Batch {
  if ($batch.Count -eq 0) { return }
  if ($OutCsv) {
    $exists = Test-Path -LiteralPath $OutCsv
    $batch | Export-Csv -LiteralPath $OutCsv -Append:($exists) -NoTypeInformation -Encoding utf8
  } else {
    $batch
  }
  $batch.Clear()
}

function Add-Row {
  param([hashtable]$Row)
  $obj = [pscustomobject]$Row
  $batch.Add($obj) | Out-Null
  if ($batch.Count -ge $BATCH_SIZE) { Flush-Batch }
}

# ---------- Main ----------

$root = Resolve-Path -LiteralPath $Path -ErrorAction Stop | Select-Object -ExpandProperty Path
$rootLP = Resolve-LongPath -InputPath $root

$queue = [System.Collections.Generic.Queue[string]]::new()
$queue.Enqueue($rootLP)

$visited = 0
$rootDepth = ($rootLP -split '[\\/]').Length

while ($queue.Count -gt 0) {
  $dir = $queue.Dequeue()
  $visited++

  # Depth control
  $depthNow = ($dir -split '[\\/]').Length - $rootDepth
  if ($Depth -ge 0 -and $depthNow -gt $Depth) { continue }

  # Detect reparse points for the current directory
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
    if (-not $check.OK) { continue }
  }

  # Enumerate children only if we could read the folder
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
        # enqueue for later processing (its own access will be evaluated)
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
    # Should be rare because we already tested readability
    if ($IncludeFolders) {
      Add-Row @{
        Type='Folder'; Path=$dir; Name=[System.IO.Path]::GetFileName($dir.TrimEnd('\'));
        Parent=[System.IO.Path]::GetDirectoryName($dir);
        LastWriteTime = (Format-DateUtcOpt (Get-Item -LiteralPath $dir -Force).LastWriteTime -Utc:$Utc);
        UserHasAccess=$false; AccessStatus='ENUMERATION_ERROR'; AccessError=$_.Exception.Message
      }
    }
  }
}

Flush-Batch

if ($OutCsv) {
  Write-Host "✅ Inventario completado -> $OutCsv"
}