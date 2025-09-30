<# 
.SYNOPSIS
  Inventario de permisos NTFS por archivo con UPN y rol (Owner/Reader/Editor), CSV incremental con rotación.

.PARAMETER Path
  Carpeta raíz a analizar (p. ej. D:\Datos o \\FS01\Compartido).

.PARAMETER Depth
  Profundidad máxima (por defecto sin límite). En PS 7+ se usa -Depth de Get-ChildItem.

.PARAMETER ExpandGroups
  Si se especifica, expande grupos AD a usuarios (recursivo).

.PARAMETER OutCsv
  Ruta base del CSV. Se escriben filas en tiempo real. Con -MaxRowsPerCsv se rota:
  <nombre>-1.csv, <nombre>-2.csv, ...

.PARAMETER MaxRowsPerCsv
  Máximo de filas de datos por archivo CSV (no incluye el header). 0 o ausente = sin rotación.

.PARAMETER LogPath
  Ruta base de log. Si no se especifica y hay -OutCsv, se deriva del mismo nombre.
  Se rota en paralelo: <nombre>-1.log, <nombre>-2.log, ...

.PARAMETER Utc
  Exporta fechas en UTC (sufijo Z). Si no, usa hora local con offset.

.EXAMPLE
  .\Get-FileNtfsPermissions.ps1 -Path "D:\Datos" -ExpandGroups -OutCsv "C:\Temp\ntfs_permisos.csv" -MaxRowsPerCsv 200000 -LogPath "C:\Temp\ntfs_permisos.log"
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)]
  [string]$Path,
  [int]$Depth = -1,
  [switch]$ExpandGroups,
  [string]$OutCsv,
  [int]$MaxRowsPerCsv = 0,
  [string]$LogPath,
  [switch]$Utc
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# --- Caches para acelerar búsquedas AD/SID ---
$SidToNtCache       = @{}
$SamToUpnCache      = @{}
$DnToUpnCache       = @{}
$GroupMembersCache  = @{}

# --- Formato de fecha ---
function Format-Date {
  param([datetime]$dt, [bool]$AsUtc = $false)
  if ($null -eq $dt) { return $null }
  if ($AsUtc) { return $dt.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ',[System.Globalization.CultureInfo]::InvariantCulture) }
  else        { return $dt.ToString('yyyy-MM-ddTHH:mm:sszzz',[System.Globalization.CultureInfo]::InvariantCulture) }
}

# --- Logging (console + archivo con rotación paralela al CSV) ---
$csvWriter = $null
$logWriter = $null
$csvIndex  = 0      # 0 => aún no abierto
$csvRowsInCurrent = 0
$asUtcBool = [bool]$Utc

# Derivar LogPath si no se pasó y hay OutCsv
if (-not $LogPath -and $OutCsv) {
  $LogPath = [IO.Path]::ChangeExtension($OutCsv, ".log")
}

# Columnas en orden fijo
$Columns = @('FileName','FilePath','UPN','Role','Rights','AccessSource','AceIdentity','Inherited')

function Write-Log {
  param([string]$Message,[string]$Level="INFO")
  $ts = (Get-Date).ToString('yyyy-MM-ddTHH:mm:ss.fffzzz',[System.Globalization.CultureInfo]::InvariantCulture)
  $line = "[$ts][$Level] $Message"
  Write-Host $line
  if ($null -ne $logWriter) { $logWriter.WriteLine($line) }
}

function Ensure-Dir($filePath) {
  $dir = Split-Path -Parent $filePath
  if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
}

function Get-RotatedName {
  param([string]$basePath,[int]$index,[string]$extIfNoExt)
  # basePath puede venir con extensión; queremos <nombre>-<index>.<ext>
  $dir  = Split-Path -Parent $basePath
  $name = Split-Path -Leaf   $basePath
  $stem = [IO.Path]::GetFileNameWithoutExtension($name)
  $ext  = [IO.Path]::GetExtension($name)
  if ([string]::IsNullOrWhiteSpace($ext)) { $ext = $extIfNoExt }
  return (Join-Path $dir ("{0}-{1}{2}" -f $stem, $index, $ext))
}

function Open-Writers {
  # Abre/rota CSV y LOG (UTF-8 sin BOM), escribe header CSV si aplica
  param()

  # Cerrar actuales si existen
  if ($null -ne $csvWriter) { $csvWriter.Dispose(); $csvWriter = $null }
  if ($null -ne $logWriter) { $logWriter.Dispose(); $logWriter = $null }

  if ($OutCsv) {
    $script:csvIndex++
    $csvPath = Get-RotatedName -basePath $OutCsv -index $script:csvIndex -extIfNoExt ".csv"
    Ensure-Dir $csvPath
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    $script:csvWriter = New-Object System.IO.StreamWriter($csvPath, $true, $utf8NoBom)
    $script:csvWriter.AutoFlush = $true
    # Header
    $headerLine = ($Columns | ConvertTo-Csv -NoTypeInformation)[0]
    $script:csvWriter.WriteLine($headerLine)
    $script:csvRowsInCurrent = 0
    Write-Host "➡️  CSV activo: $csvPath"
  }

  if ($LogPath) {
    $logPathIdx = Get-RotatedName -basePath $LogPath -index $script:csvIndex -extIfNoExt ".log"
    Ensure-Dir $logPathIdx
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    $script:logWriter = New-Object System.IO.StreamWriter($logPathIdx, $true, $utf8NoBom)
    $script:logWriter.AutoFlush = $true
    Write-Host "➡️  LOG activo: $logPathIdx"
  }
}

function Rotate-IfNeeded {
  if (-not $OutCsv -or $MaxRowsPerCsv -le 0) { return }
  if ($script:csvRowsInCurrent -ge $MaxRowsPerCsv) {
    Write-Log ("Rotando archivos: filas={0}, Max={1}" -f $script:csvRowsInCurrent, $MaxRowsPerCsv)
    Open-Writers
  }
}

function Write-CsvRow {
  param([psobject]$Row)
  if ($null -eq $csvWriter) { return }
  # Ordenar columnas
  $ordered = [ordered]@{}
  foreach ($c in $Columns) { $ordered[$c] = $Row.$c }
  $tmp = New-Object psobject -Property $ordered
  $csvLines = $tmp | ConvertTo-Csv -NoTypeInformation
  if ($csvLines.Count -ge 2) { $csvWriter.WriteLine($csvLines[1]) } else { $csvWriter.WriteLine($csvLines[0]) }
  $script:csvRowsInCurrent++
  Rotate-IfNeeded
}

# --- Resolución de identidades ---
function Resolve-NtToUPN {
  param([string]$Identity) # 'DOM\user' o 'SID' o 'BUILTIN\...'
  if ([string]::IsNullOrWhiteSpace($Identity)) { return $null }
  if ($Identity -match '^(NT AUTHORITY|BUILTIN)\\') { return $null }

  # Si viene como SID
  if ($Identity -match '^S-1-') {
    if ($SidToNtCache.ContainsKey($Identity)) { $Identity = $SidToNtCache[$Identity] }
    else {
      try {
        $nt = (New-Object System.Security.Principal.SecurityIdentifier($Identity)).
              Translate([System.Security.Principal.NTAccount]).Value
        $SidToNtCache[$Identity] = $nt
        $Identity = $nt
      } catch { return $null }
    }
  }

  # Extraer samAccountName
  $sam = if ($Identity -match '^[^\\]+\\(?<sam>.+)$') { $Matches['sam'] } else { $Identity }

  if ($SamToUpnCache.ContainsKey($sam)) { return $SamToUpnCache[$sam] }

  try {
    $u = Get-ADUser -Identity $sam -Properties UserPrincipalName -ErrorAction Stop
    $upn = $u.UserPrincipalName
    $SamToUpnCache[$sam] = $upn
    return $upn
  } catch {
    return $null
  }
}

function Get-ADGroupUsersRecursive {
  param([string]$GroupSam)

  if ($GroupMembersCache.ContainsKey($GroupSam)) { return $GroupMembersCache[$GroupSam] }

  try { $grp = Get-ADGroup -Identity $GroupSam -ErrorAction Stop } catch { return @() }

  $users = @()
  try { $members = Get-ADGroupMember -Identity $grp.DistinguishedName -Recursive -ErrorAction Stop } catch { $members = @() }

  foreach ($m in $members) {
    if ($m.objectClass -eq 'user') {
      if ($DnToUpnCache.ContainsKey($m.DistinguishedName)) { $upn = $DnToUpnCache[$m.DistinguishedName] }
      else {
        try {
          $u = Get-ADUser -Identity $m.DistinguishedName -Properties UserPrincipalName -ErrorAction Stop
          $upn = $u.UserPrincipalName
          $DnToUpnCache[$m.DistinguishedName] = $upn
        } catch { $upn = $null }
      }
      if ($upn) { $users += $upn }
    }
  }

  $users = $users | Sort-Object -Unique
  $GroupMembersCache[$GroupSam] = $users
  return $users
}

function Map-RightsToRole {
  param([System.Security.AccessControl.FileSystemRights]$Rights)
  if ($Rights.HasFlag([System.Security.AccessControl.FileSystemRights]::FullControl)) { return 'Editor' }
  if ($Rights.HasFlag([System.Security.AccessControl.FileSystemRights]::Modify))      { return 'Editor' }
  if ($Rights.HasFlag([System.Security.AccessControl.FileSystemRights]::Write))       { return 'Editor' }
  if ($Rights.HasFlag([System.Security.AccessControl.FileSystemRights]::CreateFiles)) { return 'Editor' }
  if ($Rights.HasFlag([System.Security.AccessControl.FileSystemRights]::ReadData) -or
      $Rights.HasFlag([System.Security.AccessControl.FileSystemRights]::Read) -or
      $Rights.HasFlag([System.Security.AccessControl.FileSystemRights]::ReadAndExecute)) { return 'Reader' }
  return 'Reader'
}

# Helper de enumeración con Depth en PS7+
function Get-Files {
  param([string]$Base,[int]$Depth)
  $params = @{ LiteralPath = $Base; File = $true; Force = $true; ErrorAction='SilentlyContinue' }
  $supportsDepth = $PSVersionTable.PSVersion.Major -ge 7 -and (Get-Command Get-ChildItem).Parameters.ContainsKey('Depth')
  if ($Depth -ge 0 -and $supportsDepth) { Get-ChildItem @params -Recurse -Depth $Depth } else { Get-ChildItem @params -Recurse }
}

# Validación de ruta
if (-not (Test-Path -LiteralPath $Path)) {
  throw "La ruta no existe o no es accesible: $Path"
}

# Abrimos primera pareja CSV/LOG si corresponde
if ($OutCsv) { Open-Writers } elseif ($LogPath) {
  # Si no hay CSV pero sí LOG, abrimos un índice 1 para el log
  $csvIndex = 1
  Open-Writers
}

# --- Recorremos archivos en streaming ---
$files = Get-Files -Base $Path -Depth $Depth

foreach ($f in $files) {
  # Log de archivo detectado
  Write-Log ("FILE Found: '{0}' Size={1} LastWrite={2}" -f $f.FullName, [int64]$f.Length, (Format-Date $f.LastWriteTime $asUtcBool))

  $acl = $null
  try {
    $acl = Get-Acl -LiteralPath $f.FullName
  } catch {
    Write-Log ("ACL ERROR: '{0}' => {1}" -f $f.FullName, $_.Exception.Message) "ERROR"
    continue
  }

  # Propietario
  $ownerNt  = $acl.Owner
  $ownerUpn = Resolve-NtToUPN $ownerNt
  if ($ownerUpn) {
    $row = [pscustomobject]@{
      FileName      = $f.Name
      FilePath      = $f.FullName
      UPN           = $ownerUpn
      Role          = 'Owner'
      Rights        = 'Owner'
      AccessSource  = 'Owner'
      AceIdentity   = $ownerNt
      Inherited     = $null
    }
    if ($csvWriter) { Write-CsvRow -Row $row }
    Write-Output $row
    Write-Log ("OWNER: {0} -> {1}" -f $ownerNt, $ownerUpn)
  } else {
    Write-Log ("OWNER UPN not resolved: {0}" -f $ownerNt) "WARN"
  }

  # ACEs
  foreach ($ace in $acl.Access) {
    if ($ace.AccessControlType -ne 'Allow') { continue }

    $identity = $ace.IdentityReference.Value
    $role     = Map-RightsToRole -Rights $ace.FileSystemRights
    $inherited = [bool]$ace.IsInherited

    # BUILTIN/NT AUTHORITY
    if ($identity -match '^(NT AUTHORITY|BUILTIN)\\') {
      $row = [pscustomobject]@{
        FileName      = $f.Name
        FilePath      = $f.FullName
        UPN           = $null
        Role          = $role
        Rights        = ($ace.FileSystemRights -as [string])
        AccessSource  = 'Cuenta especial'
        AceIdentity   = $identity
        Inherited     = $inherited
      }
      if ($csvWriter) { Write-CsvRow -Row $row }
      Write-Output $row
      Write-Log ("ACE SPECIAL: {0} Rights={1} Inh={2}" -f $identity, $row.Rights, $inherited)
      continue
    }

    # Usuario directo o grupo
    $upn = Resolve-NtToUPN $identity
    $isGroup = $false; $sam = $null
    if (-not $upn) {
      try {
        $sam = ($identity -match '^[^\\]+\\(?<sam>.+)$') ? $Matches['sam'] : $identity
        $null = Get-ADGroup -Identity $sam -ErrorAction Stop
        $isGroup = $true
      } catch { $isGroup = $false }
    }

    if ($isGroup -and $ExpandGroups.IsPresent) {
      $usersUpn = Get-ADGroupUsersRecursive -GroupSam $sam
      Write-Log ("GROUP EXPAND: {0} -> {1} users" -f $sam, $usersUpn.Count)
      foreach ($userUpn in $usersUpn) {
        $row = [pscustomobject]@{
          FileName      = $f.Name
          FilePath      = $f.FullName
          UPN           = $userUpn
          Role          = $role
          Rights        = ($ace.FileSystemRights -as [string])
          AccessSource  = "Grupo:$sam"
          AceIdentity   = $identity
          Inherited     = $inherited
        }
        if ($csvWriter) { Write-CsvRow -Row $row }
        Write-Output $row
      }
    } else {
      $row = [pscustomobject]@{
        FileName      = $f.Name
        FilePath      = $f.FullName
        UPN           = $upn
        Role          = $role
        Rights        = ($ace.FileSystemRights -as [string])
        AccessSource  = ($isGroup ? "Grupo(sin expandir)" : "Directo")
        AceIdentity   = $identity
        Inherited     = $inherited
      }
      if ($csvWriter) { Write-CsvRow -Row $row }
      Write-Output $row
      Write-Log ("ACE: {0} -> {1} Rights={2} Inh={3}" -f $identity, ($upn ?? 'N/A'), $row.Rights, $inherited)
    }
  }
}

# Cierre de escritores
if ($csvWriter) { $csvWriter.Dispose() }
if ($logWriter) { $logWriter.Dispose() }

Write-Host "✅ Finalizado."