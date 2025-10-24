<#
.SYNOPSIS
  Inventario con verificación de accesibilidad inmediata, salida unificada y saneo opcional de nombres.

    - LogDir se crea con Directory.CreateDirectory (long-path).
    - LOG y CSV se escriben con StreamWriter (Append) + FileShare.ReadWrite + reintentos (sin Export-Csv).
    - Preflight de escritura en -LogDir (Test-CanWrite).
    - Columnas: Type, Name, OlderName, NewName, Path, LastWriteTime, CreationTime, FileSize, UserHasAccess, AccessStatus, AccessError.
    - Saneo solo si el NOMBRE es inválido; OlderName/NewName se rellenan cuando hay renombre real.
    - folder-info.txt con contadores y TotalBytes (si -ComputeRootSize).
    - NUEVO: inventory-failed-or-denied.csv con la misma estructura pero solo filas sin acceso.

.PARAMETER Path              Raíz a inventariar (local o UNC).
.PARAMETER LogDir            Carpeta local para outputs (CSV, LOG, TXT).
.PARAMETER Depth             Profundidad máxima (−1 ilimitado; 0 solo raíz).
.PARAMETER IncludeFiles      Incluir archivos (default).
.PARAMETER IncludeFolders    Incluir carpetas (default).
.PARAMETER SkipReparsePoints Saltar reparse points (default).
.PARAMETER Utc               Emite fechas en UTC; si no, locales.
.PARAMETER ComputeRootSize   Suma de bytes de todos los archivos bajo la raíz.
.PARAMETER SanitizeNames     Sanea/renombra SOLO si el nombre es inválido.
.PARAMETER MaxNameLength     Longitud máxima de NOMBRE (no ruta), por defecto 255.
.PARAMETER ReplacementChar   Carácter de reemplazo para inválidos, por defecto "_".
.PARAMETER InventorySummaryCsv Ruta al archivo CSV de resumen de inventario centralizado (opcional).
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
  [string]$ReplacementChar = "_",
  [string]$InventorySummaryCsv

)

# ---------- Helpers base ----------
function New-NamedMutex { param([string]$Name) [System.Threading.Mutex]::new($false, "Global\$Name") }

function Ensure-CsvHeaderUtf8Bom {
  param([Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$HeaderLine)
  if (-not (Test-Path -LiteralPath $Path)) {
    $lp = Add-LongPathPrefix (Convert-ToSystemPath $Path)
    [void][System.IO.Directory]::CreateDirectory([System.IO.Path]::GetDirectoryName($lp))
    $fs = [System.IO.File]::Open($lp, [System.IO.FileMode]::CreateNew,
                                 [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
    $sw = New-Object System.IO.StreamWriter($fs, [System.Text.UTF8Encoding]::new($true)) # BOM
    $sw.WriteLine($HeaderLine); $sw.Dispose(); $fs.Dispose()
  }
}

function Append-LinesUtf8Retry {
  param([Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string[]]$Lines,
        [int]$MaxRetries = 40, [int]$InitialDelayMs = 120)
  $lp = Add-LongPathPrefix (Convert-ToSystemPath $Path)
  [void][System.IO.Directory]::CreateDirectory([System.IO.Path]::GetDirectoryName($lp))
  $attempt = 0
  while ($true) {
    try {
      $fs = [System.IO.File]::Open($lp, [System.IO.FileMode]::Append,
                                   [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
      $sw = New-Object System.IO.StreamWriter($fs, [System.Text.UTF8Encoding]::new($false))
      foreach ($l in $Lines) { $sw.WriteLine($l) }
      $sw.Dispose(); $fs.Dispose()
      break
    } catch [System.IO.IOException],[System.UnauthorizedAccessException] {
      if ($attempt -ge $MaxRetries) { throw }
      Start-Sleep -Milliseconds ([int]($InitialDelayMs * [math]::Pow(1.25, $attempt)))
      $attempt++
    }
  }
}

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
function Test-CanWrite {
  param([string]$Dir)
  try {
    $lpDir = Add-LongPathPrefix (Convert-ToSystemPath $Dir)
    [void][System.IO.Directory]::CreateDirectory($lpDir)
    $probe = Join-Path $Dir ('.write_probe_{0}.tmp' -f [guid]::NewGuid())
    $lp = Add-LongPathPrefix (Convert-ToSystemPath $probe)
    $fs = [System.IO.File]::Open($lp, [System.IO.FileMode]::CreateNew,
                                 [System.IO.FileAccess]::Write,
                                 [System.IO.FileShare]::ReadWrite)
    $fs.Dispose()
    Remove-Item -LiteralPath $probe -ErrorAction SilentlyContinue
    return $true
  } catch { return $false }
}

# ---------- Rutas de salida ----------
if (-not (Ensure-Directory -Dir $LogDir)) { throw "No se pudo preparar LogDir '$LogDir'." }
$OutCsv        = Join-Path $LogDir 'inventory.csv'
$OutCsvDenied  = Join-Path $LogDir 'inventory-failed-or-denied.csv'   
$LogPath       = Join-Path $LogDir 'inventory.log'
$InfoTxt       = Join-Path $LogDir 'folder-info.txt'

Remove-Item -LiteralPath $OutCsv -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $OutCsvDenied -ErrorAction SilentlyContinue  
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
    $max = 5
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
  } catch { Write-Host "[$ts][WARN] Logging deshabilitado: $($_.Exception.Message)" }
}

# ---------- Utilidades CSV/fechas/attrs ----------
function Write-LinesWithRetry {
  param([string]$Path,[string[]]$Lines)
  $lp = Add-LongPathPrefix (Convert-ToSystemPath $Path)
  [void][System.IO.Directory]::CreateDirectory([System.IO.Path]::GetDirectoryName($lp))
  $max = 5
  for($i=1; $i -le $max; $i++){
    try {
      $fs = [System.IO.File]::Open($lp, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
      $sw = New-Object System.IO.StreamWriter($fs, [System.Text.UTF8Encoding]::new($false))
      foreach($l in $Lines){ $sw.WriteLine($l) }
      $sw.Dispose(); $fs.Dispose(); return
    } catch {
      if ($i -eq $max) { throw }
      Start-Sleep -Milliseconds (100 * $i * $i)
    }
  }
}

function Get-EnumOptions {
  $o = [System.IO.EnumerationOptions]::new()
  $o.RecurseSubdirectories=$false; $o.ReturnSpecialDirectories=$false; $o.IgnoreInaccessible=$false
  $o.AttributesToSkip = [System.IO.FileAttributes]::Offline -bor [System.IO.FileAttributes]::Temporary -bor [System.IO.FileAttributes]::Device
  $o
}

# --- lectura real de archivo ---
function Test-FileReadable {
  param([Parameter(Mandatory)][string]$FilePathLP)
  try {
    $fs = [System.IO.File]::Open($FilePathLP,
                                 [System.IO.FileMode]::Open,
                                 [System.IO.FileAccess]::Read,
                                 [System.IO.FileShare]::ReadWrite)
    $null = $fs.CanRead
    $fs.Dispose()
    return @{ OK = $true; Error = $null }
  } catch {
    return @{ OK = $false; Error = $_.Exception.Message }
  }
}

# --- comprobación robusta de listado de carpeta ---
function Test-DirListImmediate {
  param([Parameter(Mandatory)][string]$DirPathLP)

  $tryOnce = {
    try {
      $enum = [System.IO.Directory]::EnumerateFileSystemEntries($args[0], '*', (Get-EnumOptions))
      $it = $enum.GetEnumerator()
      $null = $it.MoveNext()
      $it.Dispose()
      return @{ OK = $true; Error = $null }
    } catch {
      return @{ OK = $false; Error = $_.Exception.Message }
    }
  }

  $r = & $tryOnce $DirPathLP
  if ($r.OK) { return $r }

  Start-Sleep -Milliseconds 80
  $r2 = & $tryOnce $DirPathLP
  if ($r2.OK) { return $r2 }

  try {
    $filesEnum = [System.IO.Directory]::EnumerateFiles($DirPathLP, '*', (Get-EnumOptions))
    $fIt = $filesEnum.GetEnumerator()
    $fOk = $fIt.MoveNext(); $fIt.Dispose()
  } catch { $fOk = $false }

  try {
    $dirsEnum = [System.IO.Directory]::EnumerateDirectories($DirPathLP, '*', (Get-EnumOptions))
    $dIt = $dirsEnum.GetEnumerator()
    $dOk = $dIt.MoveNext(); $dIt.Dispose()
  } catch { $dOk = $false }

  if ($fOk -or $dOk) { return @{ OK = $true; Error = $r2.Error } }
  else { return $r2 }
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
$InvalidSet = @('<','>',':','"','/','\','|','*','?')
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

# --- Cálculo recursivo único del tamaño del árbol raíz ---
function Compute-RootBytes {
  param([Parameter(Mandatory)][string]$RootPath)
  $lpRoot = Add-LongPathPrefix (Convert-ToSystemPath $RootPath)
  $opts = [System.IO.EnumerationOptions]::new()
  $opts.RecurseSubdirectories   = $true
  $opts.ReturnSpecialDirectories= $false
  $opts.IgnoreInaccessible      = $true
  $opts.AttributesToSkip        = [System.IO.FileAttributes]::Offline -bor `
                                  [System.IO.FileAttributes]::Temporary -bor `
                                  [System.IO.FileAttributes]::Device
  [long]$sum = 0
  try {
    $files = [System.IO.Directory]::EnumerateFiles($lpRoot, '*', $opts)
    foreach ($f in $files) {
      try { $fi = [System.IO.FileInfo]::new($f); $sum += [int64]$fi.Length } catch { }
    }
  } catch { }
  return $sum
}

# ---------- CSV ----------
$csvColumns = @('Type','Name','OlderName','NewName','Path','LastWriteTime','CreationTime','FileSize','UserHasAccess','AccessStatus','AccessError')
$batch        = New-Object System.Collections.Generic.List[object]
$batchDenied  = New-Object System.Collections.Generic.List[object]   # NUEVO
$BATCH_SIZE=1000

# cabeceras
Write-LinesWithRetry -Path $OutCsv       -Lines @(($csvColumns -join ','))
Write-LinesWithRetry -Path $OutCsvDenied -Lines @(($csvColumns -join ','))  # NUEVO

function Flush-Batch {
  if ($batch.Count -eq 0) { return }
  $lines = $batch | Select-Object -Property $csvColumns | ConvertTo-Csv -NoTypeInformation
  if ($lines.Count -gt 0) { $lines = $lines[1..($lines.Count-1)] }
  Write-LinesWithRetry -Path $OutCsv -Lines $lines
  $batch.Clear()
}
function Flush-BatchDenied {                                            # NUEVO
  if ($batchDenied.Count -eq 0) { return }
  $lines = $batchDenied | Select-Object -Property $csvColumns | ConvertTo-Csv -NoTypeInformation
  if ($lines.Count -gt 0) { $lines = $lines[1..($lines.Count-1)] }
  Write-LinesWithRetry -Path $OutCsvDenied -Lines $lines
  $batchDenied.Clear()
}

function Add-Row {
  param([hashtable]$Row)
  # principal
  $o=[ordered]@{}; foreach($c in $csvColumns){ $o[$c] = $(if($Row.ContainsKey($c)){$Row[$c]}else{$null}) }
  $obj = [pscustomobject]$o
  $batch.Add($obj) | Out-Null
  if($batch.Count -ge $BATCH_SIZE){ Flush-Batch }

  # duplicado a "denied" si aplica
  if ($Row.ContainsKey('UserHasAccess') -and -not [bool]$Row['UserHasAccess']) {
    $batchDenied.Add([pscustomobject]$o) | Out-Null
    if ($batchDenied.Count -ge $BATCH_SIZE) { Flush-BatchDenied }
  }
}

# ---------- Contadores ----------
[int]$TotalFolders=0; [int]$TotalFiles=0; [int]$AccessibleFolders=0; [int]$InaccessibleFolders=0; [int]$AccessibleFiles=0; [int]$InaccessibleFiles=0; [int]$RenamedOrInvalidFolders=0; [int]$RenamedOrInvalidFiles=0; [long]$TotalBytes=0

# Preflight de escritura
if (-not (Test-CanWrite -Dir $LogDir)) {
  Write-Host "[WARN] La cuenta actual no puede escribir en $LogDir. Considera usar un -LogDir en D:\logs o ajustar ACL/Defender."
}

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
    $folderCrt  = if ($di) { $di.CreationTime } else { [System.IO.Directory]::GetCreationTime($dirLP) }

    if ($SkipReparsePoints -and $isReparse) {
      Add-Row @{ Type='Folder'; Name=$folderName; OlderName=$null; NewName=$folderName; Path=$dirFriendly;
                 LastWriteTime=(Format-DateUtcOpt $folderLwt -Utc:$Utc);
                 CreationTime=(Format-DateUtcOpt $folderCrt -Utc:$Utc);
                 FileSize=$null; UserHasAccess=$false; AccessStatus='SKIPPED_REPARSE'; AccessError=$null }
      $InaccessibleFolders++; continue
    }

    $check = Test-DirListImmediate -DirPathLP $dirLP
    Add-Row @{ Type='Folder'; Name=$folderName; OlderName=$null; NewName=$folderName; Path=$dirFriendly;
               LastWriteTime=(Format-DateUtcOpt $folderLwt -Utc:$Utc);
               CreationTime=(Format-DateUtcOpt $folderCrt -Utc:$Utc);
               FileSize=$null; UserHasAccess=$check.OK; AccessStatus=($check.OK ? 'OK' : 'DENIED'); AccessError=$check.Error }
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
          Add-Row @{ Type='File'; Name=$name; OlderName=$null; NewName=$name; Path=$entryFriendly;
                     LastWriteTime=$null; CreationTime=$null; FileSize=$null;
                     UserHasAccess=$false; AccessStatus='ATTR_DENIED'; AccessError=$attr.Error }
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
          $childCrt  = if ($cd) { $cd.CreationTime } else { [System.IO.Directory]::GetCreationTime((Add-LongPathPrefix $entrySys)) }

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
                $childCrt  = if ($cd) { $cd.CreationTime } else { $childCrt }
              } else {
                Write-Log "RENAME_FAILED carpeta '$($cd.Name)' (prop.: '$unique'): $($ren.Error)" 'WARN'
              }
            }
          }

          if ($SkipReparsePoints -and $isReparseChild) {
            Add-Row @{ Type='Folder'; Name=$childName; OlderName=$null; NewName=$childName; Path=$entryFriendly;
                       LastWriteTime=(Format-DateUtcOpt $childLwt -Utc:$Utc);
                       CreationTime=(Format-DateUtcOpt $childCrt -Utc:$Utc);
                       FileSize=$null; UserHasAccess=$false; AccessStatus='SKIPPED_REPARSE'; AccessError=$null }
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
          $fileCrt  = if ($fi) { $fi.CreationTime } else { [System.IO.File]::GetCreationTime((Add-LongPathPrefix $entrySys)) }
          $fileLen  = if ($fi) { [nullable[int64]]$fi.Length } else { $null }
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
                if ($fi) {
                  $fileName = $fi.Name
                  $newerName = $fi.Name
                  $fileLwt = $fi.LastWriteTime
                  $fileCrt = $fi.CreationTime
                  $fileLen = [nullable[int64]]$fi.Length
                }
              } else {
                Write-Log "RENAME_FAILED archivo '$($fi.Name)' (prop.: '$unique'): $($ren.Error)" 'WARN'
              }
            }
          }

          $probe = Test-FileReadable -FilePathLP (Add-LongPathPrefix $entrySys)
          if ($probe.OK) {
            $AccessibleFiles++
            Add-Row @{
              Type='File'; Name=$fileName; OlderName=$olderName; NewName=$newerName; Path=$entryFriendly;
              LastWriteTime=(Format-DateUtcOpt $fileLwt -Utc:$Utc);
              CreationTime=(Format-DateUtcOpt $fileCrt -Utc:$Utc);
              FileSize=$fileLen;
              UserHasAccess=$true; AccessStatus='OK'; AccessError=$null
            }
            if ($wasInvalid) { $RenamedOrInvalidFiles++ }
          } else {
            $InaccessibleFiles++
            Add-Row @{
              Type='File'; Name=$fileName; OlderName=$olderName; NewName=$newerName; Path=$entryFriendly;
              LastWriteTime=(Format-DateUtcOpt $fileLwt -Utc:$Utc);
              CreationTime=(Format-DateUtcOpt $fileCrt -Utc:$Utc);
              FileSize=$fileLen;
              UserHasAccess=$false; AccessStatus='READ_DENIED'; AccessError=$probe.Error
            }
            Write-Log "READ_DENIED: $entryFriendly - $($probe.Error)" 'WARN'
          }
        }
      }
    }
  } catch {
    if ($IncludeFolders) {
      $di = Get-DirInfoSafe -AnySystemPath $dirLP
      $folderName = if ($di) { $di.Name } else { Get-BaseName -PathString $dirFriendly }
      $folderLwt  = if ($di) { $di.LastWriteTime } else { [System.IO.Directory]::GetLastWriteTime($dirLP) }
      $folderCrt  = if ($di) { $di.CreationTime } else { [System.IO.Directory]::GetCreationTime($dirLP) }
      Add-Row @{ Type='Folder'; Name=$folderName; OlderName=$null; NewName=$folderName; Path=$dirFriendly;
                 LastWriteTime=(Format-DateUtcOpt $folderLwt -Utc:$Utc);
                 CreationTime=(Format-DateUtcOpt $folderCrt -Utc:$Utc);
                 FileSize=$null; UserHasAccess=$false; AccessStatus='ENUMERATION_ERROR'; AccessError=$_.Exception.Message }
      $InaccessibleFolders++; Write-Log "ENUMERATION_ERROR: $dirFriendly - $($_.Exception.Message)" 'WARN'
    }
  }
}

Flush-Batch
Flush-BatchDenied   # NUEVO

# ---------- Folder info ----------
if ($ComputeRootSize) { $TotalBytes = Compute-RootBytes -RootPath $friendlyRoot }

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
Write-Log "CSV  -> $OutCsvDenied (solo denegados)"   # NUEVO
Write-Log "LOG  -> $LogPath"
Write-Log "INFO -> $InfoTxt"
if ($ComputeRootSize) { Write-Log "TotalBytes=$TotalBytes" }


# ===== CSV centralizado de inventarios (compatible Excel, robusto, sin duplicados) =====
try {
  $subcarpeta  = (Split-Path -Leaf (Convert-ToSystemPath $Path))
  $invRoot     = (Split-Path -Parent (Convert-ToSystemPath $LogDir))
  $sumCsv      = Join-Path $invRoot 'resumen-conciliaciones.csv'

  # -- cálculo GB + texto (es-ES) --
  [int64]$bytes = $TotalBytes
  [double]$gb   = 0.0
  if ($bytes -gt 0) { $gb = [math]::Round(([double]$bytes / 1GB), 6) }
  $es = [System.Globalization.CultureInfo]::GetCultureInfo('es-ES')
  $gb_texto = $gb.ToString('0.######', $es)

  $nowStr = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')

  $row = [pscustomobject]@{
    'Subcarpeta'               = $subcarpeta
    'Tamano_Bytes'             = $bytes
    'Tamano_GB'                = $gb
    'Tamano_GB_Texto'          = $gb_texto
    'Carpetas_inaccesibles'    = $InaccessibleFolders
    'Carpetas_accesibles'      = $AccessibleFolders
    'Archivos_accesibles'      = $AccessibleFiles
    'Archivos_inaccesibles'    = $InaccessibleFiles
    'FechaHora'                = $nowStr
  }

  # Mutex global (fallback local)
  $mutex = $null
  try   { $mutex = [System.Threading.Mutex]::new($false,'Global\inventory_summary_mutex') }
  catch { $mutex = [System.Threading.Mutex]::new($false,'inventory_summary_mutex') }
  $null = $mutex.WaitOne()

  # ---------- util: Exporta tabla a csv (con ; y BOM) ----------
  function Export-Table([array]$data,[string]$path){
    $retry=0; while($true){
      try { $data | Export-Csv -LiteralPath $path -NoTypeInformation -Delimiter ';' -Encoding utf8BOM; break }
      catch [System.UnauthorizedAccessException],[System.IO.IOException] {
        if ($retry -ge 12) { throw }; Start-Sleep -Milliseconds (250 * [math]::Pow(1.6,$retry)); $retry++
      }
    }
  }

  # ---------- Carga segura del maestro (si existe) ----------
  $master = @()
  if (Test-Path -LiteralPath $sumCsv) {
    try {
      $master = Import-Csv -LiteralPath $sumCsv -Delimiter ';' -Encoding UTF8
    } catch {
      # si falla lectura por lock, reintenta breve
      $retry=0; while($true){
        try { $master = Import-Csv -LiteralPath $sumCsv -Delimiter ';' -Encoding UTF8; break }
        catch {
          if ($retry -ge 12) { throw }
          Start-Sleep -Milliseconds (250 * [math]::Pow(1.6,$retry)); $retry++
        }
      }
    }
  }

  # ---------- Migración de encabezados si faltan ----------
  $needGBText = ($master.Count -gt 0 -and -not ($master[0].PSObject.Properties.Name -contains 'Tamano_GB_Texto'))
  $needFecha  = ($master.Count -gt 0 -and -not ($master[0].PSObject.Properties.Name -contains 'FechaHora'))
  if ($needGBText -or $needFecha) {
    foreach($r in $master){
      if ($needGBText -and -not $r.PSObject.Properties.Match('Tamano_GB_Texto')) {
        $val = if ($r.Tamano_GB) { ([double]$r.Tamano_GB).ToString('0.######',$es) } else { '' }
        Add-Member -InputObject $r -NotePropertyName 'Tamano_GB_Texto' -NotePropertyValue $val
      }
      if ($needFecha -and -not $r.PSObject.Properties.Match('FechaHora')) {
        Add-Member -InputObject $r -NotePropertyName 'FechaHora' -NotePropertyValue ''
      }
    }
  }

  # ---------- Sweeper de órfanos + consolidación ----------
  $tmpRows = @()
  Get-ChildItem -LiteralPath $invRoot -Filter '.__tmp_inv_*.csv' -File -ErrorAction SilentlyContinue | ForEach-Object {
    try {
      $rows = Import-Csv -LiteralPath $_.FullName -Delimiter ';' -Encoding UTF8
      if ($rows) {
        # Asegura columnas nuevas si vienen de versiones previas
        foreach($r in $rows){
          if (-not $r.PSObject.Properties.Match('Tamano_GB_Texto')) {
            $val = if ($r.Tamano_GB) { ([double]$r.Tamano_GB).ToString('0.######',$es) } else { '' }
            Add-Member -InputObject $r -NotePropertyName 'Tamano_GB_Texto' -NotePropertyValue $val
          }
          if (-not $r.PSObject.Properties.Match('FechaHora')) {
            Add-Member -InputObject $r -NotePropertyName 'FechaHora' -NotePropertyValue ''
          }
        }
        $tmpRows += $rows
      }
      Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue
    } catch {
      # Si algo falla, deja el tmp para un próximo intento
    }
  }

  # ---------- UPsert: 1 fila por Subcarpeta (preferimos la más reciente) ----------
  $all = @($master + $tmpRows)

  # Reemplaza/crea la fila de la subcarpeta actual
  $all = $all | Where-Object { $_.Subcarpeta -ne $subcarpeta }
  $all += $row

  # Si aún quedaran duplicados por sweep (mismo nombre), nos quedamos con la más reciente por FechaHora
  $all =
    $all |
    Group-Object Subcarpeta |
    ForEach-Object {
      if ($_.Count -gt 1 -and ($_.Group | Where-Object { $_.FechaHora })) {
        $_.Group | Sort-Object { try { [datetime]$_.FechaHora } catch { Get-Date '1900-01-01' } } -Descending | Select-Object -First 1
      } else {
        $_.Group | Select-Object -Last 1
      }
    }

  # ---------- Guardar maestro ----------
  $tmpOut = Join-Path $invRoot (".__rewrite_{0}.csv" -f ([guid]::NewGuid()))
  Export-Table -data $all -path $tmpOut
  Move-Item -LiteralPath $tmpOut -Destination $sumCsv -Force
}
catch {
  Write-Log "WARN: No se pudo actualizar resumen-conciliaciones.csv -> $($_.Exception.Message)" 'WARN'
}
finally {
  if ($mutex) { $mutex.ReleaseMutex(); $mutex.Dispose() }
}
# ===== FIN CSV centralizado =====
