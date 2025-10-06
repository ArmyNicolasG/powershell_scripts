<#
.SYNOPSIS
  Inventario con verificación de accesibilidad inmediata, salida unificada y saneo opcional de nombres.
  v2.6.6 (hotfix logging server):
    - Logging robusto: StreamWriter Append + FileShare.ReadWrite + reintentos.
    - No se borra el .log al inicio (evita carrera con AV/EDR).
    - LogDir se crea con Directory.CreateDirectory (soporta long-path).
    - Mantiene: columnas CSV, saneo solo si inválido, folder-info.txt, ComputeRootSize.

.PARAMETER Path              Ruta raíz a inventariar.
.PARAMETER LogDir            Carpeta local de salida (CSV, LOG, TXT).
.PARAMETER Depth             Profundidad (−1 ilimitado).
.PARAMETER IncludeFiles      Incluir archivos (por defecto).
.PARAMETER IncludeFolders    Incluir carpetas (por defecto).
.PARAMETER SkipReparsePoints Saltar reparse points (por defecto).
.PARAMETER Utc               Fechas en UTC.
.PARAMETER ComputeRootSize   Suma de bytes del árbol (si se indica).
.PARAMETER SanitizeNames     Sanea/renombra SOLO si el nombre es inválido.
.PARAMETER MaxNameLength     Longitud máxima del NOMBRE (no ruta), por defecto 255.
.PARAMETER ReplacementChar   Carácter de reemplazo, por defecto "_".
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

# ---------- Helpers base ----------
function Convert-ToSystemPath {
  param([string]$AnyPath)
  try { $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($AnyPath) }
  catch { $AnyPath -replace '^Microsoft\.PowerShell\.Core\\FileSystem::','' }
}
function Add-LongPathPrefix {
  param([string]$SystemPath)
  if ($SystemPath -like '\\?\*') { return $SystemPath }
  if (-not $IsWindows) { return $SystemPath }
  if ($SystemPath -match '^[A-Za-z]:\\') { return "\\?\$SystemPath" }
  if ($SystemPath -like '\\*') { return "\\?\UNC\{0}" -f $SystemPath.TrimStart('\') }
  $SystemPath
}
function Ensure-Directory {
  param([Parameter(Mandatory)][string]$Dir)
  $lp = Add-LongPathPrefix (Convert-ToSystemPath $Dir)
  [void][System.IO.Directory]::CreateDirectory($lp)
  return $true
}

# ---------- Rutas de salida ----------
if (-not (Ensure-Directory -Dir $LogDir)) { throw "No se pudo preparar LogDir '$LogDir'." }
$OutCsv  = Join-Path $LogDir 'inventory.csv'
$LogPath = Join-Path $LogDir 'inventory.log'
$InfoTxt = Join-Path $LogDir 'folder-info.txt'

# No borramos el .log (evita "access denied" si está monitoreado por AV/EDR)
Remove-Item -LiteralPath $OutCsv -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $InfoTxt -ErrorAction SilentlyContinue

# ---------- Logging robusto ----------
function Write-Log {
  param([string]$Message,[string]$Level='INFO')
  $ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss.fff')
  $line = "[$ts][$Level] $Message"
  Write-Host $line
  try {
    $lp = Add-LongPathPrefix (Convert-ToSystemPath $LogPath)
    [void][System.IO.Directory]::CreateDirectory([System.IO.Path]::GetDirectoryName($lp))
    $max = 4
    for($i=1; $i -le $max; $i++){
      try {
        $fs = [System.IO.File]::Open($lp, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
        $sw = New-Object System.IO.StreamWriter($fs, [System.Text.UTF8Encoding]::new($true))
        $sw.WriteLine($line); $sw.Dispose(); $fs.Dispose(); break
      } catch {
        if ($i -eq $max) { Write-Host "[$ts][WARN] No se pudo escribir log: $($_.Exception.Message)" }
        else { Start-Sleep -Milliseconds (100 * $i * $i) }
      }
    }
  } catch {
    Write-Host "[$ts][WARN] Logging deshabilitado temporalmente: $($_.Exception.Message)"
  }
}

function Get-EnumOptions {
  $o = [System.IO.EnumerationOptions]::new()
  $o.RecurseSubdirectories=$false; $o.ReturnSpecialDirectories=$false; $o.IgnoreInaccessible=$false
  $o.AttributesToSkip = [System.IO.FileAttributes]::Offline -bor [System.IO.FileAttributes]::Temporary -bor [System.IO.FileAttributes]::Device
  $o
}
function Test-DirReadable { param([string]$DirPathForDotNet)
  try { $e=[System.IO.Directory]::EnumerateFileSystemEntries($DirPathForDotNet,'*',(Get-EnumOptions)); $it=$e.GetEnumerator(); $null=$it.MoveNext(); $it.Dispose(); @{OK=$true;Error=$null} }
  catch { @{OK=$false;Error=$_.Exception.Message} }
}
function Get-AttrSafe { param([string]$AnySystemPath)
  try { @{OK=$true;Attr=[System.IO.File]::GetAttributes($AnySystemPath);Error=$null} } catch { @{OK=$false;Attr=$null;Error=$_.Exception.Message} }
}
function Get-DirInfoSafe { param([string]$AnySystemPath) try { [System.IO.DirectoryInfo]::new($AnySystemPath) } catch { $null } }
function Get-FileInfoSafe { param([string]$AnySystemPath) try { [System.IO.FileInfo]::new($AnySystemPath) } catch { $null } }
function Format-DateUtcOpt { param($dt,[switch]$Utc)
  if ($null -eq $dt) { return $null }
  try { $d=[datetime]$dt; if ($Utc){$d.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss.fffZ')} else {$d.ToString('yyyy-MM-ddTHH:mm:ss.fffK')} } catch { $null }
}
function Get-BaseName { param([Parameter(Mandatory)][string]$PathString)
  $trim=[System.IO.Path]::TrimEndingDirectorySeparator($PathString)
  $name=[System.IO.Path]::GetFileName($trim)
  if ([string]::IsNullOrEmpty($name)) { try { return ([System.IO.DirectoryInfo]$trim).Name } catch { return $trim } }
  $name
}

# ---------- Validación de nombres ----------
$InvalidSet = @('<','>',':','"','/','\','|','?','*')
$ReservedNames = @('CON','PRN','AUX','NUL','COM1','COM2','COM3','COM4','COM5','COM6','COM7','COM8','COM9','LPT1','LPT2','LPT3','LPT4','LPT5','LPT6','LPT7','LPT8','LPT9')
function Test-NameInvalid { param([string]$Name,[int]$MaxLen)
  if ([string]::IsNullOrEmpty($Name)) { return $true }
  foreach ($ch in $Name.ToCharArray()) { if ([int][char]$ch -lt 32) { return $true } }
  foreach ($bad in $InvalidSet) { if ($Name.Contains($bad)) { return $true } }
  if ($Name.EndsWith(' ') -or $Name.EndsWith('.')) { return $true }
  $stem=[System.IO.Path]::GetFileNameWithoutExtension($Name)
  if ($ReservedNames -contains $stem.ToUpper()) { return $true }
  if ($Name.Length -gt $MaxLen) { return $true }
  $false
}
function Build-SanitizedName { param([string]$Name,[int]$MaxLen,[string]$ReplacementChar)
  if ([string]::IsNullOrWhiteSpace($Name)) { return 'unnamed' }
  $sb = New-Object System.Text.StringBuilder
  foreach ($ch in $Name.ToCharArray()) { $code=[int][char]$ch; if ($code -lt 32 -or $InvalidSet -contains $ch){$null=$sb.Append($ReplacementChar)} else {$null=$sb.Append($ch)} }
  $new=$sb.ToString().TrimEnd('.',' '); if ([string]::IsNullOrWhiteSpace($new)){$new='unnamed'}
  $ext=[System.IO.Path]::GetExtension($new)
  if ($new.Length -gt $MaxLen){ if ($ext -and $ext.Length -lt $MaxLen){$base=$new.Substring(0,[Math]::Max(1,$MaxLen-$ext.Length));$new=$base+$ext}else{$new=$new.Substring(0,$MaxLen)} }
  $stem=[System.IO.Path]::GetFileNameWithoutExtension($new); if ($ReservedNames -contains $stem.ToUpper()){$new="${stem}_$($ext.TrimStart('.'))"}
  if ([string]::IsNullOrWhiteSpace($new)){$new='unnamed'}; return $new
}
function Ensure-UniqueName { param([string]$DirectoryPath,[string]$Candidate,[bool]$IsDirectory)
  $ext=[System.IO.Path]::GetExtension($Candidate); $stem=[System.IO.Path]::GetFileNameWithoutExtension($Candidate); $i=1; $final=$Candidate
  while ($true){ $target=Join-Path $DirectoryPath $final; $exists= if($IsDirectory){[System.IO.Directory]::Exists((Add-LongPathPrefix $target))}else{[System.IO.File]::Exists((Add-LongPathPrefix $target))}; if(-not $exists){return $final}; $final=('{0}~{1}{2}' -f $stem,$i,$ext); $i++ }
}
function Try-RenameItem { param([string]$CurrentPath,[string]$NewName,[bool]$IsDirectory)
  if ([string]::IsNullOrWhiteSpace($NewName)) { return @{ OK = $false; NewPath = $CurrentPath; Error = 'Empty NewName' } }
  $directory=[System.IO.Path]::GetDirectoryName($CurrentPath); $targetPath=Join-Path $directory $NewName
  try{ if($IsDirectory){[System.IO.Directory]::Move((Add-LongPathPrefix $CurrentPath),(Add-LongPathPrefix $targetPath))}else{[System.IO.File]::Move((Add-LongPathPrefix $CurrentPath),(Add-LongPathPrefix $targetPath))}; @{OK=$true;NewPath=$targetPath;Error=$null} }
  catch{ @{OK=$false;NewPath=$CurrentPath;Error=$_.Exception.Message} }
}

# ---------- CSV ----------
$csvColumns = @('Type','Name','OlderName','NewName','Path','LastWriteTime','UserHasAccess','AccessStatus','AccessError')
$batch = New-Object System.Collections.Generic.List[object]; $BATCH_SIZE=1000
function Flush-Batch { if($batch.Count -eq 0){return}; $exists=Test-Path -LiteralPath $OutCsv; $batch | Select-Object -Property $csvColumns | Export-Csv -LiteralPath $OutCsv -Append:$exists -NoTypeInformation -Encoding utf8; $batch.Clear() }
function Add-Row { param([hashtable]$Row); $o=[ordered]@{}; foreach($c in $csvColumns){$o[$c]= $(if($Row.ContainsKey($c)){$Row[$c]}else{$null})}; $batch.Add([pscustomobject]$o)|Out-Null; if($batch.Count -ge $BATCH_SIZE){Flush-Batch} }

# ---------- Contadores ----------
[int]$TotalFolders=0; [int]$TotalFiles=0; [int]$AccessibleFolders=0; [int]$InaccessibleFolders=0; [int]$AccessibleFiles=0; [int]$InaccessibleFiles=0; [int]$RenamedOrInvalidFolders=0; [int]$RenamedOrInvalidFiles=0; [long]$TotalBytes=0

# ---------- Main ----------
$friendlyRoot = Convert-ToSystemPath -AnyPath $Path
if (-not (Test-Path -LiteralPath $friendlyRoot)) { Write-Log "Ruta no encontrada: $Path" 'ERROR'; throw "No such path: $Path" }
Write-Log "Inicio inventario en: $friendlyRoot"

$queue = [System.Collections.Generic.Queue[string]]::new(); $queue.Enqueue($friendlyRoot)
$rootDepth = ($friendlyRoot -split '[\\/]').Length

while ($queue.Count -gt 0) {
  $dirFriendly = $queue.Dequeue()
  Write-Progress -Activity "Inventariando..." -Status $dirFriendly -PercentComplete -1

  $depthNow = ($dirFriendly -split '[\\/]').Length - $rootDepth
  if ($Depth -ge 0 -and $depthNow -gt $Depth) { continue }

  $dirSys = Convert-ToSystemPath $dirFriendly; $dirLP  = Add-LongPathPrefix $dirSys
  $attrInfo = Get-AttrSafe -AnySystemPath $dirLP; $isReparse = $false
  if ($attrInfo.OK -and ($attrInfo.Attr -band [System.IO.FileAttributes]::ReparsePoint)) { $isReparse = $true }

  # Saneo del propio directorio (excepto raíz)
  if ($SanitizeNames -and $dirFriendly -ne $friendlyRoot) {
    $di = Get-DirInfoSafe -AnySystemPath $dirLP
    if ($di -and -not [string]::IsNullOrEmpty($di.Name) -and (Test-NameInvalid -Name $di.Name -MaxLen $MaxNameLength)) {
      $RenamedOrInvalidFolders++
      $proposed = Build-SanitizedName -Name $di.Name -MaxLen $MaxNameLength -ReplacementChar $ReplacementChar
      if ($proposed -ne $di.Name) {
        $unique = Ensure-UniqueName -DirectoryPath $di.Parent.FullName -Candidate $proposed -IsDirectory $true
        $ren = Try-RenameItem -CurrentPath $di.FullName -NewName $unique -IsDirectory $true
        if ($ren.OK) { Write-Log "Renombrado carpeta: '$($di.Name)' -> '$unique' en '$($di.Parent.FullName)'"; $dirFriendly = $ren.NewPath; $dirSys=Convert-ToSystemPath $dirFriendly; $dirLP=Add-LongPathPrefix $dirSys }
        else { Write-Log "RENAME_FAILED carpeta '$($di.Name)' (prop.: '$unique'): $($ren.Error)" 'WARN' }
      }
    }
  }

  if ($IncludeFolders) {
    $TotalFolders++
    $di = Get-DirInfoSafe -AnySystemPath $dirLP
    $folderName = if ($di) { $di.Name } else { Get-BaseName -PathString $dirFriendly }
    $folderLwt  = if ($di) { $di.LastWriteTime } else { [System.IO.Directory]::GetLastWriteTime($dirLP) }

    if ($SkipReparsePoints -and $isReparse) {
      Add-Row @{ Type='Folder'; Name=$folderName; OlderName=$null; NewName=$folderName; Path=$dirFriendly; LastWriteTime=(Format-DateUtcOpt $folderLwt -Utc:$Utc); UserHasAccess=$false; AccessStatus='SKIPPED_REPARSE'; AccessError=$null }
      $InaccessibleFolders++; continue
    }

    $check = Test-DirReadable -DirPathForDotNet $dirLP
    Add-Row @{ Type='Folder'; Name=$folderName; OlderName=$null; NewName=$folderName; Path=$dirFriendly; LastWriteTime=(Format-DateUtcOpt $folderLwt -Utc:$Utc); UserHasAccess=$check.OK; AccessStatus=($check.OK ? 'OK' : 'DENIED'); AccessError=$check.Error }
    if ($check.OK) { $AccessibleFolders++ } else { $InaccessibleFolders++; Write-Log "DENIED: $dirFriendly - $($check.Error)" 'WARN'; continue }
  }

  try {
    $entries = [System.IO.Directory]::EnumerateFileSystemEntries($dirLP, '*', (Get-EnumOptions))
    foreach ($entryLP in $entries) {
      $entryFriendly = $entryLP
      if ($entryFriendly -like '\\?\*') {
        if     ($entryFriendly -like '\\?\UNC\*') { $entryFriendly = '\' + $entryFriendly.Substring(7) }
        else                                      { $entryFriendly = $entryFriendly.Substring(4) }
      }
      $entrySys = Convert-ToSystemPath $entryFriendly

      $attr = Get-AttrSafe -AnySystemPath $entryLP
      if (-not $attr.OK) {
        if ($IncludeFiles) {
          $TotalFiles++
          $name  = Get-BaseName -PathString $entryFriendly
          Add-Row @{ Type='File'; Name=$name; OlderName=$null; NewName=$name; Path=$entryFriendly; LastWriteTime=$null; UserHasAccess=$false; AccessStatus='ATTR_DENIED'; AccessError=$attr.Error }
          $InaccessibleFiles++; Write-Log "ATTR_DENIED: $entryFriendly - $($attr.Error)" 'WARN'
        }
        continue
      }

      $isDir = ($attr.Attr -band [System.IO.FileAttributes]::Directory)
      $isReparseChild = ($attr.Attr -band [System.IO.FileAttributes]::ReparsePoint)

      if ($isDir) {
        if ($IncludeFolders) {
          $cd = Get-DirInfoSafe -AnySystemPath (Add-LongPathPrefix $entrySys)
          $childName = if ($cd) { $cd.Name } else { Get-BaseName -PathString $entryFriendly }
          $childLwt  = if ($cd) { $cd.LastWriteTime } else { [System.IO.Directory]::GetLastWriteTime((Add-LongPathPrefix $entrySys)) }

          if ($SanitizeNames -and $cd -and (Test-NameInvalid -Name $cd.Name -MaxLen $MaxNameLength)) {
            $RenamedOrInvalidFolders++
            $proposed = Build-SanitizedName -Name $cd.Name -MaxLen $MaxNameLength -ReplacementChar $ReplacementChar
            if ($proposed -ne $cd.Name) {
              $unique = Ensure-UniqueName -DirectoryPath $cd.Parent.FullName -Candidate $proposed -IsDirectory $true
              $ren = Try-RenameItem -CurrentPath $cd.FullName -NewName $unique -IsDirectory $true
              if ($ren.OK) {
                Write-Log "Renombrado carpeta: '$($cd.Name)' -> '$unique' en '$($cd.Parent.FullName)'"
                $entryFriendly = $ren.NewPath; $entrySys = Convert-ToSystemPath $entryFriendly
                $cd = Get-DirInfoSafe -AnySystemPath (Add-LongPathPrefix $entrySys)
                $childName = if ($cd) { $cd.Name } else { $childName }
                $childLwt  = if ($cd) { $cd.LastWriteTime } else { $childLwt }
              } else {
                Write-Log "RENAME_FAILED carpeta '$($cd.Name)' (prop.: '$unique'): $($ren.Error)" 'WARN'
              }
            }
          }

          if ($SkipReparsePoints -and $isReparseChild) {
            Add-Row @{ Type='Folder'; Name=$childName; OlderName=$null; NewName=$childName; Path=$entryFriendly; LastWriteTime=(Format-DateUtcOpt $childLwt -Utc:$Utc); UserHasAccess=$false; AccessStatus='SKIPPED_REPARSE'; AccessError=$null }
            $TotalFolders++; $InaccessibleFolders++; continue
          }
        }
        $queue.Enqueue($entryFriendly)
      } else {
        if ($IncludeFiles) {
          $TotalFiles++
          $fi = Get-FileInfoSafe -AnySystemPath (Add-LongPathPrefix $entrySys)
          $fileName = if ($fi) { $fi.Name } else { Get-BaseName -PathString $entryFriendly }
          $fileLwt  = if ($fi) { $fi.LastWriteTime } else { [System.IO.File]::GetLastWriteTime((Add-LongPathPrefix $entrySys)) }
          $olderName=$null; $newerName=$fileName; $wasInvalid=$false

          if ($fi -and $SanitizeNames -and (Test-NameInvalid -Name $fi.Name -MaxLen $MaxNameLength)) {
            $wasInvalid=$true
            $proposed = Build-SanitizedName -Name $fi.Name -MaxLen $MaxNameLength -ReplacementChar $ReplacementChar
            if ($proposed -ne $fi.Name) {
              $unique = Ensure-UniqueName -DirectoryPath $fi.DirectoryName -Candidate $proposed -IsDirectory $false
              $ren = Try-RenameItem -CurrentPath $fi.FullName -NewName $unique -IsDirectory $false
              if ($ren.OK) {
                Write-Log "Renombrado archivo: '$($fi.Name)' -> '$unique' en '$($fi.DirectoryName)'"
                $olderName=$fi.Name; $entryFriendly=$ren.NewPath
                $fi = Get-FileInfoSafe -AnySystemPath (Add-LongPathPrefix (Convert-ToSystemPath $entryFriendly))
                $fileName = if ($fi) { $fi.Name } else { $fileName }
                $newerName = $fileName
                $fileLwt = if ($fi) { $fi.LastWriteTime } else { $fileLwt }
              } else {
                Write-Log "RENAME_FAILED archivo '$($fi.Name)' (prop.: '$unique'): $($ren.Error)" 'WARN'
              }
            }
          }

          if ($fi -and $ComputeRootSize) { $TotalBytes += [int64]$fi.Length }
          $AccessibleFiles++; if ($wasInvalid) { $RenamedOrInvalidFiles++ }
          Add-Row @{ Type='File'; Name=$fileName; OlderName=$olderName; NewName=$newerName; Path=$entryFriendly; LastWriteTime=(Format-DateUtcOpt $fileLwt -Utc:$Utc); UserHasAccess=$true; AccessStatus='OK'; AccessError=$null }
        }
      }
    }
  } catch {
    if ($IncludeFolders) {
      $di = Get-DirInfoSafe -AnySystemPath $dirLP
      $folderName = if ($di) { $di.Name } else { Get-BaseName -PathString $dirFriendly }
      $folderLwt  = if ($di) { $di.LastWriteTime } else { [System.IO.Directory]::GetLastWriteTime($dirLP) }
      Add-Row @{ Type='Folder'; Name=$folderName; OlderName=$null; NewName=$folderName; Path=$dirFriendly; LastWriteTime=(Format-DateUtcOpt $folderLwt -Utc:$Utc); UserHasAccess=$false; AccessStatus='ENUMERATION_ERROR'; AccessError=$_.Exception.Message }
      $InaccessibleFolders++; Write-Log "ENUMERATION_ERROR: $dirFriendly - $($_.Exception.Message)" 'WARN'
    }
  }
}

Flush-Batch

# ---------- Folder info ----------
$report = New-Object System.Collections.Generic.List[string]
$report.Add(("RootPath: {0}" -f $friendlyRoot)) | Out-Null
$report.Add(("TotalFolders: {0}" -f $TotalFolders)) | Out-Null
$report.Add(("TotalFiles: {0}" -f $TotalFiles)) | Out-Null
$report.Add(("AccessibleFolders: {0}" -f $AccessibleFolders)) | Out-Null
$report.Add(("InaccessibleFolders: {0}" -f $InaccessibleFolders)) | Out-Null
$report.Add(("AccessibleFiles: {0}" -f $AccessibleFiles)) | Out-Null
$report.Add(("InaccessibleFiles: {0}" -f $InaccessibleFiles)) | Out-Null
$report.Add(("RenamedOrInvalidFolders: {0}" -f $RenamedOrInvalidFolders)) | Out-Null
$report.Add(("RenamedOrInvalidFiles: {0}" -f $RenamedOrInvalidFiles)) | Out-Null
if ($ComputeRootSize) { $report.Add(("TotalBytes: {0}" -f $TotalBytes)) | Out-Null }
$report.Add(("Timestamp: {0}" -f (Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))) | Out-Null

try {
  $lpInfo = Add-LongPathPrefix (Convert-ToSystemPath $InfoTxt)
  [System.IO.File]::WriteAllLines($lpInfo, $report, [System.Text.UTF8Encoding]::new($false))
  foreach ($line in $report) { Write-Log $line }
} catch {
  Write-Log "WARN: No se pudo escribir folder-info.txt -> $($_.Exception.Message)" 'WARN'
}

Write-Log "Inventario completado."
Write-Log "CSV  -> $OutCsv"
Write-Log "LOG  -> $LogPath"
Write-Log "INFO -> $InfoTxt"
if ($ComputeRootSize) { Write-Log "TotalBytes=$TotalBytes" }
