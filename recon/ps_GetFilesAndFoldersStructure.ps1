[CmdletBinding()]
param(
  [string]$ComputerName = 'localhost',
  [Parameter(Mandatory = $true)]
  [string]$Path,
  [int]$Depth = -1,   # -1 = sin límite; ej. 3 (solo PS7+)
  [string]$OutCsv,
  [switch]$Utc,
  [string]$LogPath
)

function Invoke-Local  { param([ScriptBlock]$Script, [hashtable]$ParamMap) & $Script @ParamMap }
function Invoke-Remote {
  param([string]$ComputerName,[ScriptBlock]$Script,[hashtable]$ParamMap)
  Invoke-Command -ComputerName $ComputerName -ScriptBlock $Script -ArgumentList $ParamMap['Path'], $ParamMap['Depth'], $ParamMap['Utc'], $ParamMap['LogPath'], $ParamMap['OutCsv']
}

$core = {
  param($Path, $Depth, $Utc, $LogPath, $OutCsv)

  Set-StrictMode -Version Latest
  $ErrorActionPreference = 'Stop'

  function Format-Date {
    param([datetime]$dt, [bool]$AsUtc = $false)
    if ($null -eq $dt) { return $null }
    if ($AsUtc) { return $dt.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ',[Globalization.CultureInfo]::InvariantCulture) }
    return $dt.ToString('yyyy-MM-ddTHH:mm:sszzz',[Globalization.CultureInfo]::InvariantCulture)
  }

  $asUtcBool = [bool]$Utc
  $logWriter = $null; $csvWriter = $null

  try {
    # LOG
    if ($LogPath) {
      $logDir = Split-Path -Parent $LogPath
      if ($logDir -and -not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
      $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
      $logWriter = New-Object System.IO.StreamWriter($LogPath, $true, $utf8NoBom); $logWriter.AutoFlush = $true
    }
    function Write-Log {
      param([string]$Message, [ValidateSet('INFO','WARN','ERROR')][string]$Level = "INFO")
      $ts = (Get-Date).ToString('yyyy-MM-ddTHH:mm:ss.fffzzz',[Globalization.CultureInfo]::InvariantCulture)
      $line = "[$ts][$Level] $Message"
      switch ($Level) { 'INFO'{Write-Host $line} 'WARN'{Write-Warning $line} 'ERROR'{Write-Error $line} }
      if ($null -ne $logWriter) { $logWriter.WriteLine($line) }
    }

    # CSV
    $columns = @(
      'Type','Name','Path',
      'ItemCountImmediate',
      'LastWriteTime','AccessStatus','AccessErrors','UserHasAccess'
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
    function Write-CsvHeader { param([string[]]$Cols) if ($null -eq $csvWriter) { return }; $csvWriter.WriteLine( ($Cols | % { Escape-CsvValue $_ }) -join ',' ) }
    function Write-CsvRow    {
      param([psobject]$Row)
      if ($null -eq $csvWriter) { return }
      $vals = foreach ($c in $columns) { if ($Row.PSObject.Properties.Name -contains $c) { $Row.$c } else { $null } }
      $csvWriter.WriteLine( ($vals | % { Escape-CsvValue $_ }) -join ',' )
    }

    if ($OutCsv) {
      $csvDir = Split-Path -Parent $OutCsv
      if ($csvDir -and -not (Test-Path $csvDir)) { New-Item -ItemType Directory -Path $csvDir | Out-Null }
      $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
      $fileExists = Test-Path -LiteralPath $OutCsv
      $csvWriter = New-Object System.IO.StreamWriter($OutCsv, $true, $utf8NoBom); $csvWriter.AutoFlush = $true
      $needHeader = -not $fileExists -or ((Get-Item -LiteralPath $OutCsv).Length -eq 0)
      if ($needHeader) { Write-CsvHeader -Cols $columns }
    }

    # Validación
    if (-not (Test-Path -LiteralPath $Path)) { Write-Log "Ruta no existe o no es accesible: $Path" "ERROR"; throw "La ruta no existe o no es accesible: $Path" }

    # Enumeración helper (PS7 Depth aware)
    function Get-Children {
      param([string]$Base,[switch]$FilesOnly,[switch]$DirsOnly,[int]$Depth)
      $params = @{
        LiteralPath = $Base; Force = $true; ErrorAction = 'SilentlyContinue'; Recurse = $true
      }
      if ($FilesOnly) { $params['File'] = $true }
      if ($DirsOnly)  { $params['Directory'] = $true }
      $supportsDepth = $PSVersionTable.PSVersion.Major -ge 7 -and (Get-Command Get-ChildItem).Parameters.ContainsKey('Depth')
      if ($Depth -ge 0 -and $supportsDepth) { $params['Depth'] = $Depth }
      Get-ChildItem @params
    }

    # Acumuladores resumen
    $script:TotalFiles = 0
    $script:TotalFolders = 0
    $script:TotalAccessErrors = 0

    # 1) Carpetas (incluye raíz)
    $rootDir = Get-Item -LiteralPath $Path -ErrorAction Stop
    $allDirsEnum = @($rootDir) + (Get-Children -Base $Path -DirsOnly -Depth $Depth)

    foreach ($d in $allDirsEnum) {
      $script:TotalFolders++

      # ¿Podemos leer metadatos básicos de la carpeta?
      $dirReadable = $true
      try { $null = $d.Attributes } catch { $dirReadable = $false }

      # Conteo inmediato (no recursivo)
      $ev1 = @()
      $immediateCount = (Get-ChildItem -LiteralPath $d.FullName -Force -ErrorAction SilentlyContinue -ErrorVariable +ev1 | Measure-Object).Count

      $accErrors = $ev1.Count
      $script:TotalAccessErrors += $accErrors

      $accessStatus = if (-not $dirReadable) { 'DENIED' } elseif ($accErrors -gt 0) { 'PARTIAL' } else { 'OK' }

      $row = [pscustomobject]@{
        Type               = 'Folder'
        Name               = $d.Name
        Path               = $d.FullName
        ItemCountImmediate = $immediateCount
        LastWriteTime      = (Format-Date -dt $d.LastWriteTime -AsUtc:$asUtcBool)
        AccessStatus       = $accessStatus
        AccessErrors       = $accErrors
        UserHasAccess      = [bool]$dirReadable
      }

      $lvl = switch ($accessStatus) { 'OK'{'INFO'} 'PARTIAL'{'WARN'} default{'ERROR'} }
      Write-Log ("FOLDER: Path='{0}' Immediate={1} Access={2} Errors={3}" -f `
        $row.Path,$row.ItemCountImmediate,$row.AccessStatus,$row.AccessErrors) $lvl

      Write-CsvRow -Row $row
      $row
    }

    # 2) Archivos (streaming, sin tamaños)
    Get-Children -Base $Path -FilesOnly -Depth $Depth | ForEach-Object {
      $f = $_
      $lw = $null; $accessStatus = 'OK'; $accErrors = 0; $hasAccess = $true
      try {
        try { $lw = (Format-Date -dt $f.LastWriteTime -AsUtc:$asUtcBool) }
        catch {
          if ($_.Exception -is [System.UnauthorizedAccessException] -or $_.Exception -is [System.Security.SecurityException]) {
            $hasAccess = $false; $accessStatus = 'DENIED'; $accErrors = 1; $script:TotalAccessErrors++
          } else { throw }
        }
        if ($hasAccess) { $script:TotalFiles++ }
      }
      catch {
        $hasAccess = $false; $accessStatus = 'DENIED'; $accErrors = 1; $script:TotalAccessErrors++
      }

      $row = [pscustomobject]@{
        Type               = 'File'
        Name               = $f.Name
        Path               = $f.FullName
        ItemCountImmediate = $null
        LastWriteTime      = $lw
        AccessStatus       = $accessStatus
        AccessErrors       = $accErrors
        UserHasAccess      = [bool]$hasAccess
      }

      $lvl = switch ($accessStatus) { 'OK'{'INFO'} default{'WARN'} }
      Write-Log ("FILE: Path='{0}' LastWrite='{1}' Access={2}" -f `
        $row.Path,$row.LastWriteTime,$row.AccessStatus) $lvl

      Write-CsvRow -Row $row
      $row
    }

    # 3) Summary final
    $summary = [pscustomobject]@{
      Type               = 'Summary'
      Name               = 'TOTAL'
      Path               = $Path
      ItemCountImmediate = $null
      LastWriteTime      = (Format-Date -dt (Get-Date) -AsUtc:$asUtcBool)
      AccessStatus       = 'OK'
      AccessErrors       = $script:TotalAccessErrors
      UserHasAccess      = $true
    }
    Write-Log ("SUMMARY: Files={0} Folders={1} AccessErrors={2}" -f `
      $script:TotalFiles,$script:TotalFolders,$script:TotalAccessErrors) 'INFO'
    Write-CsvRow -Row $summary
    $summary
  }
  finally {
    if ($null -ne $csvWriter) { $csvWriter.Dispose() }
    if ($null -ne $logWriter) { $logWriter.Dispose() }
  }
}

$paramMap = @{ Path = $Path; Depth = $Depth; Utc = [bool]$Utc; LogPath = $LogPath; OutCsv = $OutCsv }

try {
  if ([string]::IsNullOrWhiteSpace($ComputerName) -or $ComputerName -eq 'localhost') {
    $result = Invoke-Local -Script $core -ParamMap $paramMap
  } else {
    $result = Invoke-Remote -ComputerName $ComputerName -Script $core -ParamMap $paramMap
  }
  if (-not $OutCsv) { $result } else { Write-Host "✅ CSV incremental: $OutCsv" }
}
catch { Write-Error $_.Exception.Message; throw }