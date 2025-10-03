<#
Reporte de archivos/carpetas con flags de acceso corregidos y columna CumulativeBytes.
- AccessStatus: OK / PARTIAL / DENIED
- AccessErrors: número de errores al enumerar
- CumulativeBytes: acumulado de bytes de archivos procesados (total al final)
#>

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

    $columns = @(
      'Type','Name','Path',
      'ItemCountImmediate','ItemCountTotal','FolderSizeBytes','FileSizeBytes',
      'CumulativeBytes','LastWriteTime','AccessStatus','AccessErrors','UserHasAccess'
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

    if (-not (Test-Path -LiteralPath $Path)) { Write-Log "Ruta no existe o no es accesible: $Path" "ERROR"; throw "La ruta no existe o no es accesible: $Path" }

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

    # Acumuladores globales
    $script:CumulativeBytes = [int64]0
    $script:TotalFiles = 0
    $script:TotalFolders = 0
    $script:TotalAccessErrors = 0

    # 1) Carpetas
    $rootDir = Get-Item -LiteralPath $Path -ErrorAction Stop
    $allDirsEnum = @($rootDir) + (Get-Children -Base $Path -DirsOnly -Depth $Depth)

    foreach ($d in $allDirsEnum) {
      $script:TotalFolders++

      # ¿Podemos leer el objeto carpeta?
      $dirReadable = $true
      try { $null = $d.Attributes } catch { $dirReadable = $false }

      # Inmediatos
      $ev1 = @()
      $immediateCount = (Get-ChildItem -LiteralPath $d.FullName -Force -ErrorAction SilentlyContinue -ErrorVariable +ev1 | Measure-Object).Count

      # Recursivo: contar y sumar tamaños (ignorando errores, pero contabilizándolos)
      $ev2 = @()
      $totalItems = 0; $totalSize = [int64]0
      Get-ChildItem -LiteralPath $d.FullName -Force -Recurse -ErrorAction SilentlyContinue -ErrorVariable +ev2 |
        ForEach-Object {
          $totalItems++
          if (-not $_.PSIsContainer) { $totalSize += [int64]$_.Length }
        }

      $accErrors = ($ev1.Count + $ev2.Count)
      $script:TotalAccessErrors += $accErrors

      $accessStatus = if (-not $dirReadable) { 'DENIED' } elseif ($accErrors -gt 0) { 'PARTIAL' } else { 'OK' }

      $row = [pscustomobject]@{
        Type               = 'Folder'
        Name               = $d.Name
        Path               = $d.FullName
        ItemCountImmediate = $immediateCount
        ItemCountTotal     = $totalItems
        FolderSizeBytes    = $totalSize
        FileSizeBytes      = $null
        CumulativeBytes    = $script:CumulativeBytes
        LastWriteTime      = (Format-Date -dt $d.LastWriteTime -AsUtc:$asUtcBool)
        AccessStatus       = $accessStatus
        AccessErrors       = $accErrors
        UserHasAccess      = [bool]$dirReadable
      }

      $lvl = switch ($accessStatus) { 'OK'{'INFO'} 'PARTIAL'{'WARN'} default{'ERROR'} }
      Write-Log ("FOLDER: Path='{0}' Immediate={1} TotalItems={2} SizeBytes={3} Access={4} Errors={5}" -f `
        $row.Path,$row.ItemCountImmediate,$row.ItemCountTotal,$row.FolderSizeBytes,$row.AccessStatus,$row.AccessErrors) $lvl

      Write-CsvRow -Row $row
      $row
    }

    # 2) Archivos (streaming con acumulado)
    Get-Children -Base $Path -FilesOnly -Depth $Depth | ForEach-Object {
      $f = $_
      $size = $null; $lw = $null; $accessStatus = 'OK'; $accErrors = 0; $hasAccess = $true
      try {
        try { $size = [int64]$f.Length; $lw = (Format-Date -dt $f.LastWriteTime -AsUtc:$asUtcBool) }
        catch {
          if ($_.Exception -is [System.UnauthorizedAccessException] -or $_.Exception -is [System.Security.SecurityException]) {
            $hasAccess = $false; $accessStatus = 'DENIED'; $size = 0; $lw = $null; $accErrors = 1; $script:TotalAccessErrors++
          } else { throw }
        }
        if ($hasAccess) { $script:CumulativeBytes += $size; $script:TotalFiles++ }
      }
      catch {
        $hasAccess = $false; $accessStatus = 'DENIED'; $accErrors = 1; $script:TotalAccessErrors++
      }

      $row = [pscustomobject]@{
        Type               = 'File'
        Name               = $f.Name
        Path               = $f.FullName
        ItemCountImmediate = $null
        ItemCountTotal     = $null
        FolderSizeBytes    = $null
        FileSizeBytes      = $size
        CumulativeBytes    = $script:CumulativeBytes
        LastWriteTime      = $lw
        AccessStatus       = $accessStatus
        AccessErrors       = $accErrors
        UserHasAccess      = [bool]$hasAccess
      }

      $lvl = switch ($accessStatus) { 'OK'{'INFO'} 'PARTIAL'{'WARN'} default{'WARN'} }
      Write-Log ("FILE: Path='{0}' SizeBytes={1} LastWrite='{2}' Access={3}" -f `
        $row.Path,$row.FileSizeBytes,$row.LastWriteTime,$row.AccessStatus) $lvl

      Write-CsvRow -Row $row
      $row
    }

    # 3) Summary final (total bajo $Path)
    $summary = [pscustomobject]@{
      Type               = 'Summary'
      Name               = 'TOTAL'
      Path               = $Path
      ItemCountImmediate = $null
      ItemCountTotal     = $script:TotalFiles
      FolderSizeBytes    = $null
      FileSizeBytes      = $null
      CumulativeBytes    = $script:CumulativeBytes
      LastWriteTime      = (Format-Date -dt (Get-Date) -AsUtc:$asUtcBool)
      AccessStatus       = 'OK'
      AccessErrors       = $script:TotalAccessErrors
      UserHasAccess      = $true
    }
    Write-Log ("SUMMARY: Files={0} Folders={1} TotalBytes={2} AccessErrors={3}" -f `
      $script:TotalFiles,$script:TotalFolders,$script:CumulativeBytes,$script:TotalAccessErrors) 'INFO'
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
