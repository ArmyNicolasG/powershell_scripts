<#
.SYNOPSIS
  Inventario rápido con verificación de accesibilidad inmediata, salida unificada y saneo opcional de nombres.

  v2.6.4:
    - Arreglado: NO intenta renombrar si el nombre es válido.
    - Arreglado: NO intenta renombrar si no se pudo obtener FileInfo/DirectoryInfo (evita nombres vacíos).
    - Arreglado: manejo correcto de rutas long-path ya prefijadas (no vuelve a anteponer \\?\).
    - Contadores y folder-info.txt precisos. Salida unificada en -LogDir.

.PARAMETER Path
  Ruta raíz local o UNC desde la que se realizará el inventario.

.PARAMETER LogDir
  Carpeta local donde se guardarán TODOS los outputs: inventory.csv, inventory.log, folder-info.txt.

.PARAMETER Depth
  Profundidad máxima a recorrer. -1 = ilimitado. 0 = solo la raíz.

.PARAMETER IncludeFiles
  Incluir archivos en el inventario (True por defecto).

.PARAMETER IncludeFolders
  Incluir carpetas en el inventario (True por defecto).

.PARAMETER SkipReparsePoints
  Saltar puntos de reanálisis (symlinks/junctions) para evitar bucles (True por defecto).

.PARAMETER Utc
  Si se especifica, las fechas se emiten en UTC; de lo contrario en hora local.

.PARAMETER ComputeRootSize
  Si se especifica, acumula el tamaño en bytes de TODOS los archivos bajo la raíz y lo escribe en folder-info.txt.

.PARAMETER SanitizeNames
  Si se especifica, el script valida y renombra en el filesystem los nombres inválidos/extendidos.
  Las columnas OlderName / NewName en el CSV reflejan los cambios realizados.

.PARAMETER MaxNameLength
  Longitud máxima del nombre a aplicar durante el saneo (por defecto 255).

.PARAMETER ReplacementChar
  Carácter de reemplazo para caracteres inválidos detectados durante el saneo (por defecto "_").
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory)][string]$Path,
  [Parameter(Mandatory)][string]$LogDir,
  [int]$Depth = -1,
  [switch]$IncludeFiles = $true,
  [switch]$IncludeFolders = $true,
  [switch]$SkipReparsePoints = $true,
  [switch]$Utc,
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

Remove-Item -LiteralPath $OutCsv -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $LogPath -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $InfoTxt -ErrorAction SilentlyContinue

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
  # Si YA viene con \\?\, devolver tal cual
  if ($SystemPath -like '\\?\*') { return $SystemPath }
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

# ---------- Validación de nombres (sin regex) ----------
$InvalidSet = @('<','>',':','"','/','\','|','?','*')
$ReservedNames = @('CON','PRN','AUX','NUL','COM1','COM2','COM3','COM4','COM5','COM6','COM7','COM8','COM9','LPT1','LPT2','LPT3','LPT4','LPT5','LPT6','LPT7','LPT8','LPT9')

function Test-NameInvalid {
  param([string]$Name,[int]$MaxLen)
  if ([string]::IsNullOrEmpty($Name)) { return $true }
  foreach ($ch in $Name.ToCharArray()) { if ([int][char]$ch -lt 32) { return $true } }
  foreach ($bad in $InvalidSet) { if ($Name.Contains($bad)) { return $true } }
  if ($Name.EndsWith(' ') -or $Name.EndsWith('.')) { return $true }
  $stem = [System.IO.Path]::GetFileNameWithoutExtension($Name)
  if ($ReservedNames -contains $stem.ToUpper()) { return $true }
  if ($Name.Length -gt $MaxLen) { return $true }
  return $false
}

function Build-SanitizedName {
  param([string]$Name,[int]$MaxLen,[string]$ReplacementChar)
  if ([string]::IsNullOrWhiteSpace($Name)) { return 'unnamed' }
  $sb = New-Object System.Text.StringBuilder
  foreach ($ch in $Name.ToCharArray()) {
    $code = [int][char]$ch
    if ($code -lt 32 -or $InvalidSet -contains $ch) { [void]$sb.Append($ReplacementChar) } else { [void]$sb.Append($ch) }
  }
  $new = $sb.ToString()
  $new = $new.TrimEnd('.',' ')
  if ([string]::IsNullOrWhiteSpace($new)) { $new = 'unnamed' }
  $ext = [System.IO.Path]::GetExtension($new)
  if ($new.Length -gt $MaxLen) {
    if ($ext -and $ext.Length -lt $MaxLen) { $base = $new.Substring(0, [Math]::Max(1, $MaxLen - $ext.Length)); $new = $base + $ext }
    else { $new = $new.Substring(0, $MaxLen) }
  }
  $stem = [System.IO.Path]::GetFileNameWithoutExtension($new)
  if ($ReservedNames -contains $stem.ToUpper()) { $new = "${stem}_$($ext.TrimStart('.'))" }
  if ([string]::IsNullOrWhiteSpace($new)) { $new = 'unnamed' }
  return $new
}

function Ensure-UniqueName {
  param([string]$DirectoryPath,[string]$Candidate,[bool]$IsDirectory)
  $ext  = [System.IO.Path]::GetExtension($Candidate)
  $stem = [System.IO.Path]::GetFileNameWithoutExtension($Candidate)
  $i = 1; $final = $Candidate
  while ($true) {
    $target = Join-Path $DirectoryPath $final
    $exists = if ($IsDirectory) { [System.IO.Directory]::Exists((Add-LongPathPrefix $target)) } else { [System.IO.File]::Exists((Add-LongPathPrefix $target)) }
    if (-not $exists) { return $final }
    $final = ('{0}~{1}{2}' -f $stem, $i, $ext); $i++
  }
}

function Try-RenameItem {
  param([string]$CurrentPath,[string]$NewName,[bool]$IsDirectory)
  if ([string]::IsNullOrWhiteSpace($NewName)) { return @{ OK = $false; NewPath = $CurrentPath; Error = 'Empty NewName' } }
  $directory  = [System.IO.Path]::GetDirectoryName($CurrentPath)
  $targetPath = Join-Path $directory $NewName
  try {
    if ($IsDirectory) { [System.IO.Directory]::Move((Add-LongPathPrefix $CurrentPath),(Add-LongPathPrefix $targetPath)) }
    else              { [System.IO.File]::Move((Add-LongPathPrefix $CurrentPath),(Add-LongPathPrefix $targetPath)) }
    return @{ OK = $true; NewPath = $targetPath; Error = $null }
  } catch {
    return @{ OK = $false; NewPath = $CurrentPath; Error = $_.Exception.Message }
  }
}

# ---------- CSV ----------
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

# ---------- Contadores ----------
[int]$TotalFolders = 0
[int]$TotalFiles = 0
[int]$AccessibleFolders = 0
[int]$InaccessibleFolders = 0
[int]$AccessibleFiles = 0
[int]$InaccessibleFiles = 0
[int]$RenamedOrInvalidFolders = 0
[int]$RenamedOrInvalidFiles = 0
[long]$TotalBytes = 0

# ---------- Main ----------
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

  $dirSys = Convert-ToSystemPath $dirFriendly
  $dirLP  = Add-LongPathPrefix $dirSys

  $attrInfo = Get-AttrSafe -AnySystemPath $dirLP
  $isReparse = $false
  if ($attrInfo.OK -and ($attrInfo.Attr -band [System.IO.FileAttributes]::ReparsePoint)) { $isReparse = $true }

  # Saneo del propio directorio (excepto la raíz): SOLO si es inválido y hay info
  if ($SanitizeNames -and $dirFriendly -ne $friendlyRoot) {
    $di = Get-DirInfoSafe -AnySystemPath $dirLP
    if ($di -and -not [string]::IsNullOrEmpty($di.Name) -and (Test-NameInvalid -Name $di.Name -MaxLen $MaxNameLength)) {
      $RenamedOrInvalidFolders++
      $proposed = Build-SanitizedName -Name $di.Name -MaxLen $MaxNameLength -ReplacementChar $ReplacementChar
      if ($proposed -ne $di.Name) {
        $unique = Ensure-UniqueName -DirectoryPath $di.Parent.FullName -Candidate $proposed -IsDirectory $true
        $ren = Try-RenameItem -CurrentPath $di.FullName -NewName $unique -IsDirectory $true
        if ($ren.OK) {
          Write-Log "Renombrado carpeta: '$($di.Name)' -> '$unique' en '$($di.Parent.FullName)'"
          $dirFriendly = $ren.NewPath
          $dirSys = Convert-ToSystemPath $dirFriendly
          $dirLP  = Add-LongPathPrefix $dirSys
        } else {
          Write-Log "RENAME_FAILED carpeta '$($di.Name)' (prop.: '$unique'): $($ren.Error)" 'WARN'
        }
      }
    }
  }

  if ($IncludeFolders) {
    $TotalFolders++
    if ($SkipReparsePoints -and $isReparse) {
      $di = Get-DirInfoSafe -AnySystemPath $dirLP
      Add-Row @{
        Type='Folder'; Name=$di?.Name; OlderName=$null; NewName=$di?.Name;
        Path=$dirFriendly; Parent=$di?.Parent?.FullName;
        LastWriteTime = (Format-DateUtcOpt $di?.LastWriteTime -Utc:$Utc);
        UserHasAccess=$false; AccessStatus='SKIPPED_REPARSE'; AccessError=$null
      }
      $InaccessibleFolders++
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
    if ($check.OK) { $AccessibleFolders++ } else { $InaccessibleFolders++; Write-Log "DENIED: $dirFriendly - $($check.Error)" 'WARN'; continue }
  }

  try {
    $entries = [System.IO.Directory]::EnumerateFileSystemEntries($dirLP, '*', (Get-EnumOptions))
    foreach ($entryLP in $entries) {
      # friendly path (quitar prefijo long-path si viene)
      $entryFriendly = $entryLP
      if ($entryFriendly -like '\\?\*') {
        if ($entryFriendly -like '\\?\UNC\*') { $entryFriendly = '\' + $entryFriendly.Substring(7) } else { $entryFriendly = $entryFriendly.Substring(4) }
      }
      $entrySys = Convert-ToSystemPath $entryFriendly

      $attr = Get-AttrSafe -AnySystemPath $entryLP
      if (-not $attr.OK) {
        if ($IncludeFiles) {
          $TotalFiles++
          Add-Row @{
            Type='File'; Name=[System.IO.Path]::GetFileName($entryFriendly);
            OlderName=$null; NewName=[System.IO.Path]::GetFileName($entryFriendly);
            Path=$entryFriendly; Parent=[System.IO.Path]::GetDirectoryName($entryFriendly);
            LastWriteTime = $null; UserHasAccess=$false; AccessStatus='ATTR_DENIED'; AccessError=$attr.Error
          }
          $InaccessibleFiles++
          Write-Log "ATTR_DENIED: $entryFriendly - $($attr.Error)" 'WARN'
        }
        continue
      }

      $isDir = ($attr.Attr -band [System.IO.FileAttributes]::Directory)
      $isReparseChild = ($attr.Attr -band [System.IO.FileAttributes]::ReparsePoint)

      if ($isDir) {
        if ($IncludeFolders) {
          if ($SanitizeNames) {
            $cd = Get-DirInfoSafe -AnySystemPath (Add-LongPathPrefix $entrySys)
            if ($cd -and -not [string]::IsNullOrEmpty($cd.Name) -and (Test-NameInvalid -Name $cd.Name -MaxLen $MaxNameLength)) {
              $RenamedOrInvalidFolders++
              $proposed = Build-SanitizedName -Name $cd.Name -MaxLen $MaxNameLength -ReplacementChar $ReplacementChar
              if ($proposed -ne $cd.Name) {
                $unique = Ensure-UniqueName -DirectoryPath $cd.Parent.FullName -Candidate $proposed -IsDirectory $true
                $ren = Try-RenameItem -CurrentPath $cd.FullName -NewName $unique -IsDirectory $true
                if ($ren.OK) {
                  Write-Log "Renombrado carpeta: '$($cd.Name)' -> '$unique' en '$($cd.Parent.FullName)'"
                  $entryFriendly = $ren.NewPath
                  $entrySys = Convert-ToSystemPath $entryFriendly
                } else {
                  Write-Log "RENAME_FAILED carpeta '$($cd.Name)' (prop.: '$unique'): $($ren.Error)" 'WARN'
                }
              }
            }
          }

          if ($SkipReparsePoints -and $isReparseChild) {
            $childDi = Get-DirInfoSafe -AnySystemPath (Add-LongPathPrefix $entrySys)
            Add-Row @{
              Type='Folder'; Name=$childDi?.Name; OlderName=$null; NewName=$childDi?.Name;
              Path=$entryFriendly; Parent=$childDi?.Parent?.FullName;
              LastWriteTime = (Format-DateUtcOpt $childDi?.LastWriteTime -Utc:$Utc);
              UserHasAccess=$false; AccessStatus='SKIPPED_REPARSE'; AccessError=$null
            }
            $TotalFolders++; $InaccessibleFolders++
            continue
          }
        }
        $queue.Enqueue($entryFriendly)
      } else {
        if ($IncludeFiles) {
          $TotalFiles++
          $fi = Get-FileInfoSafe -AnySystemPath (Add-LongPathPrefix $entrySys)
          if ($fi) {
            $oldName = $fi.Name
            $newName = $oldName
            $nameWasInvalid = $false

            if ($SanitizeNames -and (Test-NameInvalid -Name $oldName -MaxLen $MaxNameLength)) {
              $nameWasInvalid = $true
              $proposed = Build-SanitizedName -Name $oldName -MaxLen $MaxNameLength -ReplacementChar $ReplacementChar
              if ($proposed -ne $oldName) {
                $unique = Ensure-UniqueName -DirectoryPath $fi.DirectoryName -Candidate $proposed -IsDirectory $false
                $ren = Try-RenameItem -CurrentPath $fi.FullName -NewName $unique -IsDirectory $false
                if ($ren.OK) {
                  Write-Log "Renombrado archivo: '$oldName' -> '$unique' en '$($fi.DirectoryName)'"
                  $entryFriendly = $ren.NewPath
                  $fi = Get-FileInfoSafe -AnySystemPath (Add-LongPathPrefix (Convert-ToSystemPath $entryFriendly))
                  $newName = $fi?.Name
                } else {
                  Write-Log "RENAME_FAILED archivo '$oldName' (prop.: '$unique'): $($ren.Error)" 'WARN'
                }
              }
            }

            if ($ComputeRootSize) { $TotalBytes += [int64]$fi.Length }
            $AccessibleFiles++
            if ($nameWasInvalid) { $RenamedOrInvalidFiles++ }
            Add-Row @{
              Type='File'; Name=$fi?.Name; OlderName=($(if ($newName -ne $oldName) { $oldName } else { $null }));
              NewName=$newName; Path=$entryFriendly; Parent=$fi?.DirectoryName;
              LastWriteTime = (Format-DateUtcOpt $fi?.LastWriteTime -Utc:$Utc);
              UserHasAccess=$true; AccessStatus='OK'; AccessError=$null
            }
          } else {
            $InaccessibleFiles++
            Add-Row @{
              Type='File'; Name=[System.IO.Path]::GetFileName($entryFriendly);
              OlderName=$null; NewName=[System.IO.Path]::GetFileName($entryFriendly);
              Path=$entryFriendly; Parent=[System.IO.Path]::GetDirectoryName($entryFriendly);
              LastWriteTime = $null; UserHasAccess=$false; AccessStatus='ATTR_DENIED'; AccessError='Unknown'
            }
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
      $InaccessibleFolders++
      Write-Log "ENUMERATION_ERROR: $dirFriendly - $($_.Exception.Message)" 'WARN'
    }
  }
}

Flush-Batch

# ---------- Folder info ----------
$report = New-Object System.Collections.Generic.List[string]
$report.Add(("RootPath: {0}" -f $friendlyRoot))                               | Out-Null
$report.Add(("TotalFolders: {0}" -f $TotalFolders))                            | Out-Null
$report.Add(("TotalFiles: {0}" -f $TotalFiles))                                | Out-Null
$report.Add(("AccessibleFolders: {0}" -f $AccessibleFolders))                  | Out-Null
$report.Add(("InaccessibleFolders: {0}" -f $InaccessibleFolders))              | Out-Null
$report.Add(("AccessibleFiles: {0}" -f $AccessibleFiles))                      | Out-Null
$report.Add(("InaccessibleFiles: {0}" -f $InaccessibleFiles))                  | Out-Null
$report.Add(("RenamedOrInvalidFolders: {0}" -f $RenamedOrInvalidFolders))      | Out-Null
$report.Add(("RenamedOrInvalidFiles: {0}" -f $RenamedOrInvalidFiles))          | Out-Null
if ($ComputeRootSize) { $report.Add(("TotalBytes: {0}" -f $TotalBytes))        | Out-Null }
$report.Add(("Timestamp: {0}" -f (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))  | Out-Null

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllLines($InfoTxt, $report, $utf8NoBom)
foreach ($line in $report) { Write-Log $line }