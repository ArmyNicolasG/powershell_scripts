<#
REPORTE DE ACCESIBILIDAD DE CARPETAS (rápido, sin tamaños ni archivos)
- Recorre carpetas en BFS y registra:
  Type=Folder, Name, Path, ItemCountImmediate, LastWriteTime, AccessStatus(OK/PARTIAL/DENIED), AccessErrors, UserHasAccess
- "UserHasAccess" indica si se pudo abrir/listar la carpeta (no es evaluación de ACLs).
- "AccessErrors" son errores encontrados al listar hijos; aún así seguimos con lo que sí se pudo leer.
#>

[CmdletBinding()]
param(
  [string]$ComputerName = 'localhost',
  [Parameter(Mandatory = $true)]
  [string]$Path,                 # Ej: D:\Datos o \\Server\Share\Ruta
  [int]$Depth = -1,              # -1 = sin límite; 0 = solo carpeta raíz; 1 = raíz + hijos directos
  [string]$OutCsv,               # CSV incremental (UTF-8)
  [switch]$Utc,                  # Fechas en UTC (si no, local con offset)
  [string]$LogPath               # LOG en tiempo real
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

  # Normaliza ruta para evitar barra final duplicada
  function Normalize-PathNoTrail([string]$p) {
    if ([string]::IsNullOrWhiteSpace($p)) { return $p }
    $np = $p.TrimEnd('\','/')
    if ($np.Length -eq 2 -and $np[1] -eq ':') { return $np } # Ej: "D:"
    return $np
  }

  $asUtcBool = [bool]$Utc
  $Path = Normalize-PathNoTrail $Path

  # LOG
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

    # CSV
    $columns = @('Type','Name','Path','ItemCountImmediate','LastWriteTime','AccessStatus','AccessErrors','UserHasAccess')
    function Escape-CsvValue { param([object]$v)
      if ($null -eq $v) { return '' }
      $s = [string]$v
      $needsQuotes = $s.Contains('"') -or $s.Contains(',') -or $s.Contains("`n") -or $s.Contains("`r")
      if ($s.Contains('"')) { $s = $s -replace '"','""' }
      if ($needsQuotes) { return '"' + $s + '"' } else { return $s }
    }
    function Write-CsvHeader { param([string[]]$Cols) if ($null -eq $csvWriter) { return }; $csvWriter.WriteLine( ($Cols | % { Escape-CsvValue $_ }) -join ',' ) }
    function Write-CsvRow    { param([psobject]$Row)
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

    # Validación de raíz
    if (-not (Test-Path -LiteralPath $Path)) {
      Write-Log "Ruta no existe o no es accesible: $Path" "ERROR"; throw "La ruta no existe o no es accesible: $Path"
    }

    # Enumeración segura por carpeta (TopDirectoryOnly)
    function List-Names-Safe {
      param([string]$dir)
      $errors = 0
      $okOpen = $false
      $names  = @()
      $dirNames = @()

      # Paso 1: probar "abrir" la carpeta (comprobación real de acceso)
      try {
        $enum = [System.IO.Directory]::EnumerateFileSystemEntries($dir, '*', [System.IO.SearchOption]::TopDirectoryOnly).GetEnumerator()
        $okOpen = $true
        # No necesitamos recorrer aquí; hacemos la lista con PowerShell para separar dirs
      }
      catch {
        # Si no podemos enumerar ni abrir, es DENIED.
        return [pscustomobject]@{
          CanOpen = $false; Names = @(); DirNames = @(); Errors = 1
        }
      }

      # Paso 2: listar NOMBRES (no metadatos) + capturar errores puntuales
      $ev = @()
      $names = Get-ChildItem -LiteralPath $dir -Force -Name -ErrorAction SilentlyContinue -ErrorVariable +ev
      $errors += $ev.Count

      $ev2 = @()
      $dirNames = Get-ChildItem -LiteralPath $dir -Force -Name -Directory -ErrorAction SilentlyContinue -ErrorVariable +ev2
      $errors += $ev2.Count

      return [pscustomobject]@{
        CanOpen = $okOpen; Names = $names; DirNames = $dirNames; Errors = $errors
      }
    }

    # BFS (cola) para garantizar avance aunque haya errores parciales
    $queue = New-Object 'System.Collections.Generic.Queue[object]'
    $queue.Enqueue([pscustomobject]@{ Path = $Path; Depth = 0 })

    $totalFolders = 0
    $totalErrors  = 0

    while ($queue.Count -gt 0) {
      $node = $queue.Dequeue()
      $curPath = $node.Path
      $curDepth = [int]$node.Depth
      $totalFolders++

      # LastWriteTime (no bloqueante)
      $lw = $null
      try { $lw = (Get-Item -LiteralPath $curPath -ErrorAction Stop).LastWriteTime } catch {}

      # Listar de forma segura
      $r = List-Names-Safe -dir $curPath
      $totalErrors += [int]$r.Errors

      $status = if (-not $r.CanOpen) { 'DENIED' } elseif ($r.Errors -gt 0) { 'PARTIAL' } else { 'OK' }
      $hasAccess = [bool]$r.CanOpen
      $immediateCount = @($r.Names).Count

      # Registrar fila
      $row = [pscustomobject]@{
        Type               = 'Folder'
        Name               = (Split-Path -Leaf $curPath)
        Path               = $curPath
        ItemCountImmediate = $immediateCount
        LastWriteTime      = if ($lw) { if ($asUtcBool) { (Get-Date $lw) } else { $lw } } else { $null } |
                             ForEach-Object { if ($_ -ne $null) { if ($asUtcBool) { $_.ToUniversalTime() } else { $_ } } } |
                             ForEach-Object { if ($_ -ne $null) { Format-Date -dt $_ -AsUtc:$asUtcBool } else { $null } }
        AccessStatus       = $status
        AccessErrors       = [int]$r.Errors
        UserHasAccess      = $hasAccess
      }

      $lvl = switch ($status) { 'OK'{'INFO'} 'PARTIAL'{'WARN'} default{'ERROR'} }
      $msg = "FOLDER: '$($row.Path)' | Immediate=$($row.ItemCountImmediate) | Access=$($row.AccessStatus) | Errors=$($row.AccessErrors)"
      switch ($lvl) { 'INFO'{Write-Host $msg} 'WARN'{Write-Warning $msg} 'ERROR'{Write-Error $msg} }
      Write-CsvRow -Row $row

      # Encolar subcarpetas si aún hay profundidad disponible y pudimos abrir la carpeta
      $nextDepthAllowed = ($Depth -lt 0) -or ($curDepth -lt $Depth)
      if ($hasAccess -and $nextDepthAllowed -and $r.DirNames) {
        foreach ($dn in $r.DirNames) {
          $child = Join-Path -Path $curPath -ChildPath $dn
          $queue.Enqueue([pscustomobject]@{ Path = $child; Depth = $curDepth + 1 })
        }
      }
    }

    # Summary
    $summary = [pscustomobject]@{
      Type               = 'Summary'
      Name               = 'TOTAL'
      Path               = $Path
      ItemCountImmediate = $null
      LastWriteTime      = (Format-Date -dt (Get-Date) -AsUtc:$asUtcBool)
      AccessStatus       = 'OK'
      AccessErrors       = $totalErrors
      UserHasAccess      = $true
    }
    Write-Host ("SUMMARY: Folders={0} Errors={1}" -f $totalFolders,$totalErrors)
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
