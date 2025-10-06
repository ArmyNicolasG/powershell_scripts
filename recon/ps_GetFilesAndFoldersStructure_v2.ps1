<#
.SYNOPSIS
  Inventario rápido con verificación de accesibilidad inmediata, salida unificada y saneo opcional de nombres
    - Nuevo: -LogDir (único directorio de salida) -> inventory.csv, inventory.log, folder-info.txt
    - Nuevo: -ComputeRootSize (opcional) suma de bytes del árbol raíz (una sola cifra)
    - Nuevo: -SanitizeNames (opcional) renombra archivos/carpetas para cumplir reglas (caracteres inválidos, longitud, espacios/puntos finales, reservados)
    - CSV agrega columnas OlderName y NewName (Name = nombre final actual).

.DESCRIPTION
  - Recorre el árbol en BFS. Para cada carpeta valida si se puede listar hijos inmediatos.
  - Para archivos intenta leer atributos
  - Sanea nombres en caliente si se indica (-SanitizeNames), evitando colisiones con sufijos ~1, ~2...
  - Escribe todo en -LogDir:
        inventory.csv
        inventory.log
        folder-info.txt (si -ComputeRootSize)
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory)]
  [string]$Path,

  [Parameter(Mandatory)]
  [string]$LogDir,

  [int]$Depth = -1,
  [switch]$IncludeFiles = $true,
  [switch]$IncludeFolders = $true,
  [switch]$SkipReparsePoints = $true,
  [switch]$Utc,

  # Opcionales
  [switch]$ComputeRootSize,
  [switch]$SanitizeNames,
  [int]$MaxNameLength = 255,
  [string]$ReplacementChar = "_"
)

# ---------- Salidas ----------
if (-not (Test-Path -LiteralPath $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }
$OutCsv  = Join-Path $LogDir 'inventory.csv'
$LogPath = Join-Path $LogDir 'inventory.log'
$InfoTxt = Join-Path $LogDir 'folder-info.txt'

# ---------- Helpers ----------
function Write-Log {
  param([string]$Message,[string]$Level='INFO')
  $ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss.fff')
  $line = "[$ts][$Level] $Message"
  Write-Host $line
  Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8
}

# Convierte PSPath -> ruta sistema (sin prefijo de proveedor)
function Convert-ToSystemPath {
  param([string]$AnyPath)
  try { $sys = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($AnyPath) }
  catch { $sys = $AnyPath -replace '^Microsoft\.PowerShell\.Core\\FileSystem::','' }
  return $sys
}

# Añade prefijo long-path para .NET
function Add-LongPathPrefix {
  param([string]$SystemPath)
  if (-not $IsWindows) { return $SystemPath }
  if ($SystemPath -match '^[A-Za-z]:\\') { return "\\?\$SystemPath" }
  if ($SystemPath -like '\\*') { return "\\?\UNC\{0}" -f $SystemPath.TrimStart('\') }
  return $SystemPath
}

function Get-EnumOptions {
  $opts = [System.IO.EnumerationOptions]::new()
  $opts.RecurseSubdirectories     = $false
  $opts.ReturnSpecialDirectories  = $false
  $opts.IgnoreInaccessible        = $false
  $opts.AttributesToSkip          = [System.IO.FileAttributes]::Offline -bor [System.IO.FileAttributes]::Temporary -bor [System.IO.FileAttributes]::Device
  return $opts
}

function Test-DirReadable {
  param([string]$DirPathForDotNet)
  try {
    $enum = [System.IO.Directory]::EnumerateFileSystemEntries($DirPathForDotNet, '*', (Get-EnumOptions))
    $e = $enum.GetEnumerator(); $null = $e.MoveNext(); $e.Dispose()
    return @{ OK = $true; Error = $null }
  } catch { return @{ OK = $false; Error = $_.Exception.Message } }
}

function Get-AttrSafe {
  param([string]$AnySystemPath)
  try { return @{ OK = $true; Attr = [System.IO.File]::GetAttributes($AnySystemPath); Error = $null } }
  catch { return @{ OK = $false; Attr = $null; Error = $_.Exception.Message } }
}

function Get-DirInfoSafe { param([string]$AnySystemPath) try { [System.IO.DirectoryInfo]::new($AnySystemPath) } catch { $null } }
function Get-FileInfoSafe { param([string]$AnySystemPath) try { [System.IO.FileInfo]::new($AnySystemPath) } catch { $null } }

function Format-DateUtcOpt {
  param($dt,[switch]$Utc)
  if ($null -eq $dt) { return $null }
  try {
    $d = [datetime]$dt
    if ($Utc) { return $d.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss.fffZ') }
    else      { return $d.ToString('yyyy-MM-ddTHH:mm:ss.fffK') }
  } catch { return $null }
}

# Reglas de saneo (Windows/Azure Files): caracteres inválidos y nombres reservados
$InvalidCharsPattern = '[<>:"/\\|?*\x00-\x1F]'
$ReservedNames = @('CON','PRN','AUX','NUL','COM1','COM2','COM3','COM4','COM5','COM6','COM7','COM8','COM9','LPT1','LPT2','LPT3','LPT4','LPT5','LPT6','LPT7','LPT8','LPT9')

function Sanitize-BaseName {
  param([string]$Name,[int]$MaxLen,[string]$ReplacementChar)
  if ([string]::IsNullOrWhiteSpace($Name)) { return 'unnamed' }
  $new = $Name -replace $InvalidCharsPattern, [Regex]::Escape($ReplacementChar)
  # quitar espacios/puntos finales
  $new = $new.TrimEnd('.',' ')
  $new = $new.TrimStart(' ') # leading spaces generan problemas
  if ([string]::IsNullOrWhiteSpace($new)) { $new = 'unnamed' }

  # Si contiene extensión, intenta conservarla al truncar
  $ext = [System.IO.Path]::GetExtension($new)
  if ($new.Length -gt $MaxLen) {
    if ($ext -and $ext.Length -lt $MaxLen) {
      $base = $new.Substring(0, [Math]::Max(1, $MaxLen - $ext.Length))
      $new = $base + $ext
    } else {
      $new = $new.Substring(0, $MaxLen)
    }
  }

  # Reservados (sin extensión y todo en mayúsculas)
  $baseNoExt = [System.IO.Path]::GetFileNameWithoutExtension($new)
  if ($ReservedNames -contains $baseNoExt.ToUpper()) {
    $new = "${baseNoExt}_$($ext.TrimStart('.'))"
  }

  if ([string]::IsNullOrWhiteSpace($new)) { $new = 'unnamed' }
  return $new
}

function Ensure-UniqueName {
  param(
    [string]$DirectoryPath, # ruta sistema normal (sin long prefix)
    [string]$Candidate,
    [bool]$IsDirectory
  )
  $dirLP = Add-LongPathPrefix -SystemPath $DirectoryPath
  $ext = [System.IO.Path]::GetExtension($Candidate)
  $stem = [System.IO.Path]::GetFileNameWithoutExtension($Candidate)
  $i = 1
  $final = $Candidate
  while ($true) {
    $target = Join-Path $DirectoryPath $final
    $exists = if ($IsDirectory) { [System.IO.Directory]::Exists((Add-LongPathPrefix $target)) } else { [System.IO.File]::Exists((Add-LongPathPrefix $target)) }
    if (-not $exists) { return $final }
    $final = "{0}~{1}{2}" -f $stem, $i, $ext
    $i++
  }
}

function Try-RenameItem {
  param([string]$CurrentPath,[string]$NewName,[bool]$IsDirectory)
  $directory = [System.IO.Path]::GetDirectoryName($CurrentPath)
  $targetPath = Join-Path $directory $NewName
  try {
    if ($IsDirectory) { [System.IO.Directory]::Move((Add-LongPathPrefix $CurrentPath), (Add-LongPathPrefix $targetPath)) }
    else { [System.IO.File]::Move((Add-LongPathPrefix $CurrentPath), (Add-LongPathPrefix $targetPath)) }
    return @{ OK = $true; NewPath = $targetPath; Error = $null }
  } catch {
    return @{ OK = $false; NewPath = $CurrentPath; Error = $_.Exception.Message }
  }
}

# CSV schema (fijo con nuevas columnas)
$csvColumns = @('Type','Name','OlderName','NewName','Path','Parent','LastWriteTime','UserHasAccess','AccessStatus','AccessError')

$batch   = New-Object System.Collections.Generic.List[object]
$BATCH_SIZE = 1000
function Flush-Batch {
  if ($batch.Count -eq 0) { return }
  $exists = Test-Path -LiteralPath $OutCsv
  $batch | Select-Object -Property $csvColumns | Export-Csv -LiteralPath $OutCsv -Append:$exists -NoTypeInformation -Encoding utf8
  $batch.Clear()
}

function Add-Row {
  param([hashtable]$Row)
  $ordered = [ordered]@{}
  foreach ($c in $csvColumns) { $ordered[$c] = $(if ($Row.ContainsKey($c)) { $Row[$c] } else { $null }) }
  $obj = [pscustomobject]$ordered
  $batch.Add($obj) | Out-Null
  if ($batch.Count -ge $BATCH_SIZE) { Flush-Batch }
}

# ---------- Main ----------
# Preparar archivos de salida
Remove-Item -LiteralPath $OutCsv -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $LogPath -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $InfoTxt -ErrorAction SilentlyContinue
# Crear cabecera CSV
Export-Csv -InputObject ([pscustomobject]@{}) -LiteralPath $OutCsv -NoTypeInformation -Force | Out-Null
# Reescribir cabecera correcta
$null = Set-Content -LiteralPath $OutCsv -Value ($csvColumns -join ',') -Encoding UTF8

$friendlyRoot = Convert-ToSystemPath -AnyPath $Path
if (-not (Test-Path -LiteralPath $friendlyRoot)) { Write-Log "Ruta no encontrada: $Path" 'ERROR'; throw "No such path: $Path" }
Write-Log "Inicio inventario en: $friendlyRoot"

$queue = [System.Collections.Generic.Queue[string]]::new()
$queue.Enqueue($friendlyRoot)

$visited = 0
$rootDepth = ($friendlyRoot -split '[\\/]').Length

# Para ComputeRootSize
[long]$totalBytes = 0

while ($queue.Count -gt 0) {
  $dirFriendly = $queue.Dequeue(); $visited++
  Write-Progress -Activity "Inventariando..." -Status $dirFriendly -PercentComplete -1

  $depthNow = ($dirFriendly -split '[\\/]').Length - $rootDepth
  if ($Depth -ge 0 -and $depthNow -gt $Depth) { continue }

  $dirSys = Convert-ToSystemPath $dirFriendly
  $dirLP  = Add-LongPathPrefix $dirSys

  $attrInfo = Get-AttrSafe -AnySystemPath $dirLP
  $isReparse = $false
  if ($attrInfo.OK -and ($attrInfo.Attr -band [System.IO.FileAttributes]::ReparsePoint)) { $isReparse = $true }

  # Saneamos el propio directorio si aplica (excepto la raíz pasada)
  if ($SanitizeNames -and $dirFriendly -ne $friendlyRoot) {
    $di = Get-DirInfoSafe -AnySystemPath $dirLP
    if ($di) {
      $oldName = $di.Name
      $newName = Sanitize-BaseName -Name $oldName -MaxLen $MaxNameLength -ReplacementChar $ReplacementChar
      if ($newName -ne $oldName) {
        $unique = Ensure-UniqueName -DirectoryPath $di.Parent.FullName -Candidate $newName -IsDirectory $true
        $ren = Try-RenameItem -CurrentPath $di.FullName -NewName $unique -IsDirectory $true
        if ($ren.OK) {
          Write-Log "Renombrado carpeta: '$oldName' -> '$unique' en '$($di.Parent.FullName)'"
          $dirFriendly = $ren.NewPath
          $dirSys = Convert-ToSystemPath $dirFriendly
          $dirLP  = Add-LongPathPrefix $dirSys
        } else {
          Write-Log "No se pudo renombrar carpeta '$oldName': $($ren.Error)" 'WARN'
        }
      }
    }
  }

  if ($IncludeFolders) {
    if ($SkipReparsePoints -and $isReparse) {
      $di = Get-DirInfoSafe -AnySystemPath $dirLP
      Add-Row @{
        Type='Folder'; Name=$di?.Name; OlderName=$null; NewName=$di?.Name;
        Path=$dirFriendly; Parent=$di?.Parent?.FullName;
        LastWriteTime = (Format-DateUtcOpt $di?.LastWriteTime -Utc:$Utc);
        UserHasAccess=$false; AccessStatus='SKIPPED_REPARSE'; AccessError=$null
      }
      continue
    }

    $check = Test-DirReadable -DirPathForDotNet $dirLP
    $di = Get-DirInfoSafe -AnySystemPath $dirLP
    Add-Row @{
      Type='Folder'; Name=$di?.Name; OlderName=$null; NewName=$di?.Name;
      Path=$dirFriendly; Parent=$di?.Parent?.FullName;
      LastWriteTime = (Format-DateUtcOpt $di?.LastWriteTime -Utc:$Utc);
      UserHasAccess=$check.OK; AccessStatus=($check.OK ? 'OK' : 'DENIED'); AccessError=$check.Error
    }
    if (-not $check.OK) { Write-Log "DENIED: $dirFriendly - $($check.Error)" 'WARN'; continue }
  }

  try {
    $entries = [System.IO.Directory]::EnumerateFileSystemEntries($dirLP, '*', (Get-EnumOptions))
    foreach ($entryLP in $entries) {
      # friendly path
      $entryFriendly = $entryLP
      if ($entryFriendly -like '\\?\*') {
        if ($entryFriendly -like '\\?\UNC\*') { $entryFriendly = '\' + $entryFriendly.Substring(7) } else { $entryFriendly = $entryFriendly.Substring(4) }
      }
      $entrySys = Convert-ToSystemPath $entryFriendly

      $attr = Get-AttrSafe -AnySystemPath $entryLP
      if (-not $attr.OK) {
        if ($IncludeFiles) {
          Add-Row @{
            Type='File'; Name=[System.IO.Path]::GetFileName($entryFriendly);
            OlderName=$null; NewName=[System.IO.Path]::GetFileName($entryFriendly);
            Path=$entryFriendly; Parent=[System.IO.Path]::GetDirectoryName($entryFriendly);
            LastWriteTime = $null; UserHasAccess=$false; AccessStatus='ATTR_DENIED'; AccessError=$attr.Error
          }
          Write-Log "ATTR_DENIED: $entryFriendly - $($attr.Error)" 'WARN'
        }
        continue
      }

      $isDir = ($attr.Attr -band [System.IO.FileAttributes]::Directory)
      $isReparseChild = ($attr.Attr -band [System.IO.FileAttributes]::ReparsePoint)

      if ($isDir) {
        # Saneo de carpeta hija (si aplica)
        if ($SanitizeNames) {
          $cd = Get-DirInfoSafe -AnySystemPath (Add-LongPathPrefix $entrySys)
          if ($cd) {
            $old = $cd.Name
            $suggest = Sanitize-BaseName -Name $old -MaxLen $MaxNameLength -ReplacementChar $ReplacementChar
            if ($suggest -ne $old) {
              $unique = Ensure-UniqueName -DirectoryPath $cd.Parent.FullName -Candidate $suggest -IsDirectory $true
              $ren = Try-RenameItem -CurrentPath $cd.FullName -NewName $unique -IsDirectory $true
              if ($ren.OK) {
                Write-Log "Renombrado carpeta: '$old' -> '$unique' en '$($cd.Parent.FullName)'"
                $entryFriendly = $ren.NewPath
                $entrySys = Convert-ToSystemPath $entryFriendly
              } else {
                Write-Log "No se pudo renombrar carpeta '$old': $($ren.Error)" 'WARN'
              }
            }
          }
        }

        if ($SkipReparsePoints -and $isReparseChild) {
          if ($IncludeFolders) {
            $childDi = Get-DirInfoSafe -AnySystemPath (Add-LongPathPrefix $entrySys)
            Add-Row @{
              Type='Folder'; Name=$childDi?.Name; OlderName=$null; NewName=$childDi?.Name;
              Path=$entryFriendly; Parent=$childDi?.Parent?.FullName;
              LastWriteTime = (Format-DateUtcOpt $childDi?.LastWriteTime -Utc:$Utc);
              UserHasAccess=$false; AccessStatus='SKIPPED_REPARSE'; AccessError=$null
            }
          }
          continue
        }
        $queue.Enqueue($entryFriendly)
      } else {
        if ($IncludeFiles) {
          $fi = Get-FileInfoSafe -AnySystemPath (Add-LongPathPrefix $entrySys)
          $oldName = $fi?.Name
          $newName = $oldName
          if ($SanitizeNames -and $fi) {
            $suggest = Sanitize-BaseName -Name $oldName -MaxLen $MaxNameLength -ReplacementChar $ReplacementChar
            if ($suggest -ne $oldName) {
              $unique = Ensure-UniqueName -DirectoryPath $fi.DirectoryName -Candidate $suggest -IsDirectory $false
              $ren = Try-RenameItem -CurrentPath $fi.FullName -NewName $unique -IsDirectory $false
              if ($ren.OK) {
                Write-Log "Renombrado archivo: '$oldName' -> '$unique' en '$($fi.DirectoryName)'"
                $entryFriendly = $ren.NewPath
                $fi = Get-FileInfoSafe -AnySystemPath (Add-LongPathPrefix (Convert-ToSystemPath $entryFriendly))
                $newName = $fi?.Name
              } else {
                Write-Log "No se pudo renombrar archivo '$oldName': $($ren.Error)" 'WARN'
              }
            }
          }

          if ($ComputeRootSize -and $fi) {
            $totalBytes += [int64]$fi.Length
          }

          Add-Row @{
            Type='File'; Name=$fi?.Name; OlderName=($(if ($newName -ne $oldName) { $oldName } else { $null }));
            NewName=$newName; Path=$entryFriendly; Parent=$fi?.DirectoryName;
            LastWriteTime = (Format-DateUtcOpt $fi?.LastWriteTime -Utc:$Utc);
            UserHasAccess=$true; AccessStatus='OK'; AccessError=$null
          }
        }
      }
    }
  } catch {
    if ($IncludeFolders) {
      $di = Get-DirInfoSafe -AnySystemPath $dirLP
      Add-Row @{
        Type='Folder'; Name=$di?.Name; OlderName=$null; NewName=$di?.Name;
        Path=$dirFriendly; Parent=$di?.Parent?.FullName;
        LastWriteTime = (Format-DateUtcOpt $di?.LastWriteTime -Utc:$Utc);
        UserHasAccess=$false; AccessStatus='ENUMERATION_ERROR'; AccessError=$_.Exception.Message
      }
      Write-Log "ENUMERATION_ERROR: $dirFriendly - $($_.Exception.Message)" 'WARN'
    }
  }
}

Flush-Batch

if ($ComputeRootSize) {
  $info = @(
    "RootPath: $friendlyRoot",
    "TotalBytes: $totalBytes",
    "Timestamp: $((Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))"
  )
  $info | Set-Content -LiteralPath $InfoTxt -Encoding UTF8
  Write-Log "folder-info.txt -> $InfoTxt (TotalBytes=$totalBytes)"
}

Write-Log "Inventario completado. Visitados: $visited"
Write-Log "CSV -> $OutCsv"
Write-Log "LOG -> $LogPath"
if ($ComputeRootSize) { Write-Log "INFO -> $InfoTxt" }