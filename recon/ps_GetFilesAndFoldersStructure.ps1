<#
.SYNOPSIS
  Reporte de archivos y carpetas con conteos y tamaños (CSV UTF-8 incremental + logging).

.DESCRIPTION
  - Archivos: Name, Path, FileSizeBytes, LastWriteTime (ISO 8601)
  - Carpetas: Name, Path, ItemCountImmediate, ItemCountTotal, FolderSizeBytes, LastWriteTime
  - CSV incremental (append) y log por ítem (INFO/WARN/ERROR).
  - UserHasAccess: SOLO se evalúa para carpetas; en archivos queda vacío.

.PARAMETER ComputerName
  Equipo destino (usa "localhost" por defecto).

.PARAMETER Path
  Ruta base (ej. "D:\Datos" o "\\FS01\Compartido").

.PARAMETER Depth
  Profundidad máxima (PS7+). -1 = sin límite.

.PARAMETER OutCsv
  Ruta del CSV de salida (UTF-8, sin BOM), modo append.

.PARAMETER Utc
  Exporta fechas en UTC (Z). Si no, hora local con offset.

.PARAMETER LogPath
  Ruta del .log (opcional).
#>

[CmdletBinding()]
param(
  [string]$ComputerName = 'localhost',
  [Parameter(Mandatory = $true)][string]$Path,
  [int]$Depth = -1,
  [string]$OutCsv,
  [switch]$Utc,
  [string]$LogPath
)

function Invoke-Local {
  param([ScriptBlock]$Script, [hashtable]$ParamMap)
  & $Script @ParamMap
}
function Invoke-Remote {
  param([string]$ComputerName, [ScriptBlock]$Script, [hashtable]$ParamMap)
  Invoke-Command -ComputerName $ComputerName -ScriptBlock $Script -ArgumentList $ParamMap['Path'], $ParamMap['Depth'], $ParamMap['Utc'], $ParamMap['LogPath'], $ParamMap['OutCsv']
}

# --- Lógica principal (se ejecuta en el equipo de destino) ---
$core = {
  param($Path, $Depth, $Utc, $LogPath, $OutCsv)

  Set-StrictMode -Version Latest
  $ErrorActionPreference = 'Stop'

  # ---- Helpers ----
  function Format-Date {
    param([datetime]$dt, [bool]$AsUtc = $false)
    if ($null -eq $dt) { return $null }
    if ($AsUtc) { return $dt.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ', [CultureInfo]::InvariantCulture) }
    else        { return $dt.ToString('yyyy-MM-ddTHH:mm:sszzz', [CultureInfo]::InvariantCulture) }
  }
  $asUtcBool = [bool]$Utc

  $logWriter = $null
  $csvWriter = $null
  $fileRows  = 0
  $dirRows   = 0

  try {
    # --- Log ---
    if ($LogPath) {
      $logDir = Split-Path -Parent $LogPath
      if ($logDir -and -not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
      $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
      $logWriter = New-Object System.IO.StreamWriter($LogPath, $true, $utf8NoBom); $logWriter.AutoFlush = $true
    }
    function Write-Log {
      param([string]$Message, [ValidateSet('INFO','WARN','ERROR')][string]$Level = 'INFO')
      $ts = (Get-Date).ToString('yyyy-MM-ddTHH:mm:ss.fffzzz', [CultureInfo]::InvariantCulture)
      $line = "[$ts][$Level] $Message"
      switch ($Level) { 'INFO' {Write-Host $line} 'WARN' {Write-Warning $line} 'ERROR' {Write-Error $line} }
      if ($logWriter) { $logWriter.WriteLine($line) }
    }

    # --- CSV (manual, sin ConvertTo-Csv) ---
    $columns = @(
      'Type','Name','Path',
      'ItemCountImmediate','ItemCountTotal','FolderSizeBytes','FileSizeBytes',
      'LastWriteTime','UserHasAccess'   # UserHasAccess solo se calcula para carpetas
    )
    function Escape-CsvValue {
      param([object]$v)
      if ($null -eq $v) { return '' }
      $s = [string]$v
      $needsQuotes = $s.Contains('"') -or $s.Contains(',') -or $s.Contains("`n") -or $s.Contains("`r")
      if ($s.Contains('"')) { $s = $s -replace '"','""' }
      if ($needsQuotes) { return '"' + $s + '"' }
      return $s
    }
    function Write-CsvHeader {
      param([string[]]$Cols)
      if ($csvWriter) { $csvWriter.WriteLine( ($Cols | ForEach-Object { Escape-CsvValue $_ }) -join ',' ) }
    }
    function Write-CsvRow {
      param([psobject]$Row)
      if (-not $csvWriter) { return }
      $vals = foreach ($c in $columns) { if ($Row.PSObject.Properties.Name -contains $c) { $Row.$c } else { $null } }
      $line = ($vals | ForEach-Object { Escape-CsvValue $_ }) -join ','
      $csvWriter.WriteLine($line)
    }

    if ($OutCsv) {
      $csvDir = Split-Path -Parent $OutCsv
      if ($csvDir -and -not (Test-Path $csvDir)) { New-Item -ItemType Directory -Path $csvDir | Out-Null }
      $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
      $fileExists = Test-Path -LiteralPath $OutCsv
      $csvWriter = New-Object System.IO.StreamWriter($OutCsv, $true, $utf8NoBom); $csvWriter.AutoFlush = $true
      $needHeader = $true; if ($fileExists -and (Get-Item -LiteralPath $OutCsv).Length -gt 0) { $needHeader = $false }
      if ($needHeader) { Write-CsvHeader -Cols $columns }
    }

    # --- Validaciones ---
    if (-not (Test-Path -LiteralPath $Path)) {
      Write-Log "Ruta no existe o no es accesible: $Path" "ERROR"
      throw "La ruta no existe o no es accesible: $Path"
    }

    # --- Enumeración segura ---
    function Get-Children {
      param([string]$Base,[switch]$FilesOnly,[switch]$DirsOnly,[int]$Depth)
      $params = @{ LiteralPath = $Base; Force = $true; ErrorAction = 'SilentlyContinue'; Recurse = $true }
      if ($FilesOnly) { $params['File'] = $true }
      if ($DirsOnly)  { $params['Directory'] = $true }
      $supportsDepth = $PSVersionTable.PSVersion.Major -ge 7 -and (Get-Command Get-ChildItem).Parameters.ContainsKey('Depth')
      if ($Depth -ge 0 -and $supportsDepth) { $params['Depth'] = $Depth }
      Get-ChildItem @params
    }

    # --- Carpetas (incluida la raíz) ---
    $rootDir    = Get-Item -LiteralPath $Path -ErrorAction Stop
    $allDirs    = @($rootDir) + (Get-Children -Base $Path -DirsOnly -Depth $Depth)

    foreach ($d in $allDirs) {
      $userHasAccess = $true
      try {
        # Inmediatos
        $immediateCount = 0
        try {
          Get-ChildItem -LiteralPath $d.FullName -Force -ErrorAction Stop | ForEach-Object { $immediateCount++ }
        } catch {
          if ($_.Exception -is [UnauthorizedAccessException] -or $_.Exception -is [System.Security.SecurityException]) {
            $userHasAccess = $false; $immediateCount = 0
            Write-Log ("ACCESS DENIED (inmediatos): '{0}' -> {1}" -f $d.FullName, $_.Exception.Message) "WARN"
          } else { throw }
        }

        # Recursivo
        $totalItems = 0; $totalSize = [int64]0
        try {
          Get-ChildItem -LiteralPath $d.FullName -Force -Recurse -ErrorAction Stop | ForEach-Object {
            $totalItems++; if (-not $_.PSIsContainer) { $totalSize += [int64]$_.Length }
          }
        } catch {
          if ($_.Exception -is [UnauthorizedAccessException] -or $_.Exception -is [System.Security.SecurityException]) {
            $userHasAccess = $false; $totalItems = 0; $totalSize = 0
            Write-Log ("ACCESS DENIED (recursivo): '{0}' -> {1}" -f $d.FullName, $_.Exception.Message) "WARN"
          } else { throw }
        }

        $row = [pscustomobject]@{
          Type               = 'Folder'
          Name               = $d.Name
          Path               = $d.FullName
          ItemCountImmediate = $immediateCount
          ItemCountTotal     = $totalItems
          FolderSizeBytes    = $totalSize
          FileSizeBytes      = $null
          LastWriteTime      = (Format-Date -dt $d.LastWriteTime -AsUtc:$asUtcBool)
          UserHasAccess      = [bool]$userHasAccess
        }

        Write-Log ("FOLDER: Path='{0}' Immediate={1} TotalItems={2} SizeBytes={3} LastWrite='{4}' Access={5}" -f `
          $row.Path, $row.ItemCountImmediate, $row.ItemCountTotal, $row.FolderSizeBytes, $row.LastWriteTime, $row.UserHasAccess) ($userHasAccess ? 'INFO' : 'WARN')

        try { Write-CsvRow -Row $row; $dirRows++ } catch { Write-Log ("CSV WRITE ERROR (folder): '{0}' -> {1}" -f $row.Path, $_.Exception.Message) "ERROR" }
        $row
      }
      catch {
        Write-Log ("FOLDER ERROR: Path='{0}' Error='{1}'" -f $d.FullName, $_.Exception.Message) "ERROR"
        $row = [pscustomobject]@{
          Type='Folder'; Name=$d.Name; Path=$d.FullName
          ItemCountImmediate=0; ItemCountTotal=0; FolderSizeBytes=0; FileSizeBytes=$null
          LastWriteTime=(Format-Date -dt $d.LastWriteTime -AsUtc:$asUtcBool); UserHasAccess=$false
        }
        try { Write-CsvRow -Row $row; $dirRows++ } catch { Write-Log ("CSV WRITE ERROR (folder-catch): '{0}' -> {1}" -f $row.Path, $_.Exception.Message) "ERROR" }
        $row
      }
    }

    # --- Archivos (UserHasAccess no se evalúa; queda vacío) ---
    Get-Children -Base $Path -FilesOnly -Depth $Depth | ForEach-Object {
      $f = $_
      try {
        $size = $null; $lw = $null
        try {
          $size = [int64]$f.Length
          $lw   = (Format-Date -dt $f.LastWriteTime -AsUtc:$asUtcBool)
        } catch {
          # No hacemos evaluación de acceso para archivos: si falla props, dejamos valores por defecto
          Write-Log ("FILE PROP WARN: '{0}' -> {1}" -f $f.FullName, $_.Exception.Message) "WARN"
          $size = 0; $lw = $null
        }

        $row = [pscustomobject]@{
          Type='File'; Name=$f.Name; Path=$f.FullName
          ItemCountImmediate=$null; ItemCountTotal=$null; FolderSizeBytes=$null
          FileSizeBytes=$size; LastWriteTime=$lw; UserHasAccess=$null
        }

        Write-Log ("FILE: Path='{0}' SizeBytes={1} LastWrite='{2}'" -f $row.Path, $row.FileSizeBytes, $row.LastWriteTime) "INFO"
        try { Write-CsvRow -Row $row; $fileRows++ } catch { Write-Log ("CSV WRITE ERROR (file): '{0}' -> {1}" -f $row.Path, $_.Exception.Message) "ERROR" }
        $row
      }
      catch {
        Write-Log ("FILE ERROR: Path='{0}' Error='{1}'" -f $f.FullName, $_.Exception.Message) "ERROR"
        $row = [pscustomobject]@{
          Type='File'; Name=$f.Name; Path=$f.FullName
          ItemCountImmediate=$null; ItemCountTotal=$null; FolderSizeBytes=$null
          FileSizeBytes=0; LastWriteTime=$null; UserHasAccess=$null
        }
        try { Write-CsvRow -Row $row; $fileRows++ } catch { Write-Log ("CSV WRITE ERROR (file-catch): '{0}' -> {1}" -f $row.Path, $_.Exception.Message) "ERROR" }
        $row
      }
    }

    if ($OutCsv) {
      Write-Log ("CSV resumen -> Folders escritos: {0} | Files escritos: {1} | CSV: {2}" -f $dirRows, $fileRows, $OutCsv) "INFO"
    }
  }
  finally {
    if ($csvWriter) { $csvWriter.Dispose() }
    if ($logWriter) { $logWriter.Dispose() }
  }
}

# --- Ejecución local/remota ---
$paramMap = @{ Path = $Path; Depth = $Depth; Utc = [bool]$Utc; LogPath = $LogPath; OutCsv = $OutCsv }

try {
  if ([string]::IsNullOrWhiteSpace($ComputerName) -or $ComputerName -eq 'localhost') {
    $result = Invoke-Local -Script $core -ParamMap $paramMap
  } else {
    $result = Invoke-Remote -ComputerName $ComputerName -Script $core -ParamMap $paramMap
  }
  if (-not $OutCsv) { $result } else { Write-Host "✅ CSV incremental: $OutCsv" }
}
catch {
  Write-Error $_.Exception.Message
  throw
}
