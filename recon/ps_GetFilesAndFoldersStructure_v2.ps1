<#
.SYNOPSIS
  Inventario rápido con verificación de accesibilidad inmediata (folders) — FIX v2.4:
    - `Format-DateUtcOpt` ahora acepta $null sin error (el parámetro ya no fuerza [DateTime]).
    - Mantiene las correcciones de UNC/PSProvider y logging a consola + archivo.
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

function Write-Log {
  param([string]$Message,[string]$Level='INFO')
  $ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss.fff')
  $line = "[$ts][$Level] $Message"
  Write-Host $line
  if ($LogPath) { Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8 }
}

function Convert-ToSystemPath {
  param([string]$AnyPath)
  try { $sys = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($AnyPath) }
  catch { $sys = $AnyPath -replace '^Microsoft\.PowerShell\.Core\\FileSystem::','' }
  return $sys
}

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

$defaultColumns = @('Type','Path','Name','Parent','LastWriteTime','UserHasAccess','AccessStatus','AccessError')
$csvColumns = $defaultColumns.Clone()

if ($OutCsv -and (Test-Path -LiteralPath $OutCsv)) {
  try {
    $header = Get-Content -LiteralPath $OutCsv -First 1 -ErrorAction Stop
    if ($header -and ($header -notmatch '^#TYPE')) {
      $existing = $header -split ',' | ForEach-Object { $_.Trim('"') }
      if ($existing.Count -gt 1) { $csvColumns = $existing }
    }
    Write-Log "CSV existente detectado. Usaré columnas: $([string]::Join(',', $csvColumns))"
  } catch { Write-Log "No pude leer header de CSV existente, usaré columnas por defecto." 'WARN' }
} else {
  if ($LogPath) { Remove-Item -LiteralPath $LogPath -ErrorAction SilentlyContinue; New-Item -ItemType File -Path $LogPath -Force | Out-Null }
}

$batch   = New-Object System.Collections.Generic.List[object]
$BATCH_SIZE = 1000
function Flush-Batch {
  if ($batch.Count -eq 0) { return }
  if ($OutCsv) {
    $exists = Test-Path -LiteralPath $OutCsv
    $batch | Select-Object -Property $csvColumns | Export-Csv -LiteralPath $OutCsv -Append:$exists -NoTypeInformation -Encoding utf8
  } else { $batch }
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

$friendlyRoot = Convert-ToSystemPath -AnyPath $Path
if (-not (Test-Path -LiteralPath $friendlyRoot)) { Write-Log "Ruta no encontrada: $Path" 'ERROR'; throw "No such path: $Path" }
Write-Log "Inicio inventario en: $friendlyRoot"

$queue = [System.Collections.Generic.Queue[string]]::new()
$queue.Enqueue($friendlyRoot)

$visited = 0
$rootDepth = ($friendlyRoot -split '[\\/]').Length

while ($queue.Count -gt 0) {
  $dirFriendly = $queue.Dequeue(); $visited++
  Write-Progress -Activity "Inventariando..." -Status $dirFriendly -PercentComplete -1

  $depthNow = ($dirFriendly -split '[\\/]').Length - $rootDepth
  if ($Depth -ge 0 -and $depthNow -gt $Depth) { continue }

  $dirSys = Add-LongPathPrefix -SystemPath (Convert-ToSystemPath $dirFriendly)

  $attrInfo = Get-AttrSafe -AnySystemPath $dirSys
  $isReparse = $false
  if ($attrInfo.OK -and ($attrInfo.Attr -band [System.IO.FileAttributes]::ReparsePoint)) { $isReparse = $true }

  if ($IncludeFolders) {
    if ($SkipReparsePoints -and $isReparse) {
      $di = Get-DirInfoSafe -AnySystemPath $dirSys
      Add-Row @{
        Type='Folder'; Path=$dirFriendly; Name=$di?.Name;
        Parent=$di?.Parent?.FullName;
        LastWriteTime = (Format-DateUtcOpt $di?.LastWriteTime -Utc:$Utc);
        UserHasAccess=$false; AccessStatus='SKIPPED_REPARSE'; AccessError=$null
      }
      continue
    }

    $check = Test-DirReadable -DirPathForDotNet $dirSys
    $di = Get-DirInfoSafe -AnySystemPath $dirSys
    Add-Row @{
      Type='Folder'; Path=$dirFriendly; Name=$di?.Name;
      Parent=$di?.Parent?.FullName;
      LastWriteTime = (Format-DateUtcOpt $di?.LastWriteTime -Utc:$Utc);
      UserHasAccess=$check.OK; AccessStatus=($check.OK ? 'OK' : 'DENIED'); AccessError=$check.Error
    }
    if (-not $check.OK) { Write-Log "DENIED: $dirFriendly - $($check.Error)" 'WARN'; continue }
  }

  try {
    $entries = [System.IO.Directory]::EnumerateFileSystemEntries($dirSys, '*', (Get-EnumOptions))
    foreach ($entrySys in $entries) {
      $entryFriendly = $entrySys
      if ($entryFriendly -like '\\?\*') {
        if ($entryFriendly -like '\\?\UNC\*') { $entryFriendly = '\' + $entryFriendly.Substring(7) } else { $entryFriendly = $entryFriendly.Substring(4) }
      }

      $attr = Get-AttrSafe -AnySystemPath $entrySys
      if (-not $attr.OK) {
        if ($IncludeFiles) {
          Add-Row @{
            Type='File'; Path=$entryFriendly; Name=[System.IO.Path]::GetFileName($entryFriendly);
            Parent=[System.IO.Path]::GetDirectoryName($entryFriendly);
            LastWriteTime = $null;
            UserHasAccess=$false; AccessStatus='ATTR_DENIED'; AccessError=$attr.Error
          }
          Write-Log "ATTR_DENIED: $entryFriendly - $($attr.Error)" 'WARN'
        }
        continue
      }

      $isDir = ($attr.Attr -band [System.IO.FileAttributes]::Directory)
      $isReparseChild = ($attr.Attr -band [System.IO.FileAttributes]::ReparsePoint)

      if ($isDir) {
        if ($SkipReparsePoints -and $isReparseChild) {
          if ($IncludeFolders) {
            $childDi = Get-DirInfoSafe -AnySystemPath $entrySys
            Add-Row @{
              Type='Folder'; Path=$entryFriendly; Name=$childDi?.Name;
              Parent=$childDi?.Parent?.FullName;
              LastWriteTime = (Format-DateUtcOpt $childDi?.LastWriteTime -Utc:$Utc);
              UserHasAccess=$false; AccessStatus='SKIPPED_REPARSE'; AccessError=$null
            }
          }
          continue
        }
        $queue.Enqueue($entryFriendly)
      } else {
        if ($IncludeFiles) {
          $fi = Get-FileInfoSafe -AnySystemPath $entrySys
          Add-Row @{
            Type='File'; Path=$entryFriendly; Name=$fi?.Name; Parent=$fi?.DirectoryName;
            LastWriteTime = (Format-DateUtcOpt $fi?.LastWriteTime -Utc:$Utc);
            UserHasAccess=$true; AccessStatus='OK'; AccessError=$null
          }
        }
      }
    }
  } catch {
    if ($IncludeFolders) {
      $di = Get-DirInfoSafe -AnySystemPath $dirSys
      Add-Row @{
        Type='Folder'; Path=$dirFriendly; Name=$di?.Name;
        Parent=$di?.Parent?.FullName;
        LastWriteTime = (Format-DateUtcOpt $di?.LastWriteTime -Utc:$Utc);
        UserHasAccess=$false; AccessStatus='ENUMERATION_ERROR'; AccessError=$_.Exception.Message
      }
      Write-Log "ENUMERATION_ERROR: $dirFriendly - $($_.Exception.Message)" 'WARN'
    }
  }
}

Flush-Batch
Write-Log "Inventario completado. Visitados: $visited"
if ($OutCsv) { Write-Log "CSV -> $OutCsv" }
if ($LogPath) { Write-Log "LOG -> $LogPath" }
