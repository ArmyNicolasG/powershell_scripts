<# 
.SYNOPSIS
  Reporte de archivos y carpetas con conteos y tamaños (CSV UTF-8 incremental + logging por ítem).

.DESCRIPTION
  Recorre la ruta indicada y produce un listado con:
  - Archivos: Nombre, Ruta, Tamaño (FileSizeBytes), Última modificación (ISO 8601)
  - Carpetas: Nombre, Ruta, #Inmediatos, #Totales (recursivos),
              Tamaño total carpeta (FolderSizeBytes), Última mod. (ISO 8601)
  Además:
  - Log por consola y (opcional) a archivo .log en tiempo real por cada ítem y errores.
  - Si se especifica -OutCsv, escribe el CSV en tiempo real (append), una fila por ítem.

.PARAMETER ComputerName
  File Server destino. Use "localhost" o deje en blanco para local.

.PARAMETER Path
  Ruta base a analizar (p. ej. "D:\Datos" o "\\FS01\Compartido").

.PARAMETER Depth
  Profundidad máxima (PS7+). Por defecto -1 (sin límite).

.PARAMETER OutCsv
  Ruta para exportar CSV (UTF-8). Se escribe incrementalmente fila a fila.

.PARAMETER Utc
  Si se especifica, las fechas se exportan en UTC con sufijo "Z".
  Si no, se exportan en hora local con offset (p. ej. -05:00).

.PARAMETER LogPath
  Ruta de archivo .log para registrar cada carpeta/archivo y errores durante la ejecución.

.EXAMPLE
  .\Get-FileTreeReport.ps1 -Path "D:\Volumen\Proyectos" -OutCsv "C:\Temp\reporte.csv" -LogPath "C:\Temp\reporte.log"

.EXAMPLE
  .\Get-FileTreeReport.ps1 -Path "\\FS01\Compartido" -Depth 3 -Utc -OutCsv "C:\Temp\reporte.csv" -LogPath "C:\Temp\reporte.log"
#>

[CmdletBinding()]
param(
  [string]$ComputerName = 'localhost',
  [Parameter(Mandatory=$true)]
  [string]$Path,
  [int]$Depth = -1,   # -1 = sin límite; ej. 3 (solo PS7+)
  [string]$OutCsv,
  [switch]$Utc,
  [string]$LogPath
)

function Invoke-Local {
  param([ScriptBlock]$Script, [hashtable]$ParamMap)
  & $Script @ParamMap
}

function Invoke-Remote {
  param(
    [string]$ComputerName,
    [ScriptBlock]$Script,
    [hashtable]$ParamMap
  )
  Invoke-Command -ComputerName $ComputerName -ScriptBlock $Script -ArgumentList $ParamMap['Path'], $ParamMap['Depth'], $ParamMap['Utc'], $ParamMap['LogPath'], $ParamMap['OutCsv']
}

# --- Lógica principal que se ejecuta en el equipo de destino ---
$core = {
  param($Path, $Depth, $Utc, $LogPath, $OutCsv)

  Set-StrictMode -Version Latest
  $ErrorActionPreference = 'Stop'

  # --- Helpers de fecha y logging ---
  function Format-Date {
    param([datetime]$dt, [bool]$AsUtc = $false)
    if ($null -eq $dt) { return $null }
    if ($AsUtc) {
      return $dt.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ', [System.Globalization.CultureInfo]::InvariantCulture)
    } else {
      return $dt.ToString('yyyy-MM-ddTHH:mm:sszzz', [System.Globalization.CultureInfo]::InvariantCulture)
    }
  }

  $asUtcBool = [bool]$Utc

  $logWriter = $null
  $csvWriter = $null
  $csvHeaderWritten = $false

  try {
    # --- Preparar LOG ---
    if ($LogPath) {
      $logDir = Split-Path -Parent $LogPath
      if ($logDir -and -not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
      $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
      $logWriter = New-Object System.IO.StreamWriter($LogPath, $true, $utf8NoBom)
      $logWriter.AutoFlush = $true
    }

    function Write-Log {
      param([string]$Message,[string]$Level = "INFO")
      $ts = (Get-Date).ToString('yyyy-MM-ddTHH:mm:ss.fffzzz',[System.Globalization.CultureInfo]::InvariantCulture)
      $line = "[$ts][$Level] $Message"
      Write-Host $line
      if ($null -ne $logWriter) { $logWriter.WriteLine($line) }
    }

    # --- Preparar CSV incremental ---
    $columns = @('Type','Name','Path','ItemCountImmediate','ItemCountTotal','FolderSizeBytes','FileSizeBytes','LastWriteTime')
    if ($OutCsv) {
      $csvDir = Split-Path -Parent $OutCsv
      if ($csvDir -and -not (Test-Path $csvDir)) { New-Item -ItemType Directory -Path $csvDir | Out-Null }
      $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
      $fileExists = Test-Path -LiteralPath $OutCsv
      $csvWriter = New-Object System.IO.StreamWriter($OutCsv, $true, $utf8NoBom)
      $csvWriter.AutoFlush = $true

      # Escribir header solo si el archivo no existe o está vacío
      $needHeader = $true
      if ($fileExists) {
        $len = (Get-Item -LiteralPath $OutCsv).Length
        if ($len -gt 0) { $needHeader = $false }
      }
      if ($needHeader) {
        $headerLine = ($columns | ConvertTo-Csv -NoTypeInformation)[0]
        $csvWriter.WriteLine($headerLine)
      }
      $csvHeaderWritten = $true
    }

    function Write-CsvRow {
      param([psobject]$Row)
      if ($null -eq $csvWriter) { return }

      # Forzar orden de columnas y escapes correctos
      $ordered = [ordered]@{}
      foreach ($c in $columns) { $ordered[$c] = $Row.$c }
      $tmp = New-Object psobject -Property $ordered

      # ConvertTo-Csv produce una cabecera + datos; nos quedamos solo con la fila (Skip 1)
      $csvLines = $tmp | ConvertTo-Csv -NoTypeInformation
      if ($csvLines.Count -ge 2) {
        $csvWriter.WriteLine($csvLines[1])
      } elseif ($csvLines.Count -eq 1) {
        # Caso raro: sin encabezado; escribe única línea
        $csvWriter.WriteLine($csvLines[0])
      }
    }

    # Validar ruta base
    if (-not (Test-Path -LiteralPath $Path)) {
      Write-Log "Ruta no existe o no es accesible: $Path" "ERROR"
      throw "La ruta no existe o no es accesible: $Path"
    }

    # Helper para Get-ChildItem con soporte -Depth en PS7+
    function Get-Children {
      param(
        [string]$Base,
        [switch]$FilesOnly,
        [switch]$DirsOnly,
        [int]$Depth
      )
      $params = @{
        LiteralPath = $Base
        Force       = $true
        ErrorAction = 'SilentlyContinue'
        Recurse     = $true
      }
      if ($FilesOnly) { $params['File'] = $true }
      if ($DirsOnly)  { $params['Directory'] = $true }

      $supportsDepth = $PSVersionTable.PSVersion.Major -ge 7 -and
                       (Get-Command Get-ChildItem).Parameters.ContainsKey('Depth')
      if ($Depth -ge 0 -and $supportsDepth) { $params['Depth'] = $Depth }

      Get-ChildItem @params
    }

    # 1) Carpetas (incluye raíz)
    $rootDir = Get-Item -LiteralPath $Path -ErrorAction Stop
    $allDirsEnum = @($rootDir) + (Get-Children -Base $Path -DirsOnly -Depth $Depth)

    foreach ($d in $allDirsEnum) {
      try {
        # Inmediatos (no recursivo)
        $immediateCount = 0
        Get-ChildItem -LiteralPath $d.FullName -Force -ErrorAction SilentlyContinue |
          ForEach-Object { $immediateCount++ }

        # Recursivo: contar items totales y acumular tamaño solo de archivos (streaming)
        $totalItems = 0
        $totalSize  = [int64]0
        Get-ChildItem -LiteralPath $d.FullName -Force -Recurse -ErrorAction SilentlyContinue |
          ForEach-Object {
            $totalItems++
            if (-not $_.PSIsContainer) {
              $totalSize += [int64]$_.Length
            }
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
        }

        # Log por carpeta
        Write-Log ("FOLDER: Path='{0}' Immediate={1} TotalItems={2} SizeBytes={3} LastWrite='{4}'" -f `
          $row.Path, $row.ItemCountImmediate, $row.ItemCountTotal, $row.FolderSizeBytes, $row.LastWriteTime)

        # CSV inmediato
        Write-CsvRow -Row $row

        # Emitir a la tubería (por si el caller lo necesita)
        $row
      }
      catch {
        Write-Log ("FOLDER ERROR: Path='{0}' Error='{1}'" -f $d.FullName, $_.Exception.Message) "ERROR"
        $row = [pscustomobject]@{
          Type               = 'Folder'
          Name               = $d.Name
          Path               = $d.FullName
          ItemCountImmediate = 0
          ItemCountTotal     = 0
          FolderSizeBytes    = 0
          FileSizeBytes      = $null
          LastWriteTime      = (Format-Date -dt $d.LastWriteTime -AsUtc:$asUtcBool)
        }
        Write-CsvRow -Row $row
        $row
      }
    }

    # 2) Archivos (streaming)
    Get-Children -Base $Path -FilesOnly -Depth $Depth |
      ForEach-Object {
        $f = $_
        try {
          $row = [pscustomobject]@{
            Type               = 'File'
            Name               = $f.Name
            Path               = $f.FullName
            ItemCountImmediate = $null
            ItemCountTotal     = $null
            FolderSizeBytes    = $null
            FileSizeBytes      = [int64]$f.Length
            LastWriteTime      = (Format-Date -dt $f.LastWriteTime -AsUtc:$asUtcBool)
          }

          # Log por archivo
          Write-Log ("FILE: Path='{0}' SizeBytes={1} LastWrite='{2}'" -f `
            $row.Path, $row.FileSizeBytes, $row.LastWriteTime)

          # CSV inmediato
          Write-CsvRow -Row $row

          # Emitir a la tubería
          $row
        }
        catch {
          Write-Log ("FILE ERROR: Path='{0}' Error='{1}'" -f $f.FullName, $_.Exception.Message) "ERROR"
          $row = [pscustomobject]@{
            Type               = 'File'
            Name               = $f.Name
            Path               = $f.FullName
            ItemCountImmediate = $null
            ItemCountTotal     = $null
            FolderSizeBytes    = $null
            FileSizeBytes      = 0
            LastWriteTime      = $null
          }
          Write-CsvRow -Row $row
          $row
        }
      }

  }
  finally {
    if ($null -ne $csvWriter) { $csvWriter.Dispose() }
    if ($null -ne $logWriter) { $logWriter.Dispose() }
  }
}

# --- Selección de ejecución local/remota ---
$paramMap = @{ Path = $Path; Depth = $Depth; Utc = [bool]$Utc; LogPath = $LogPath; OutCsv = $OutCsv }

try {
  if ([string]::IsNullOrWhiteSpace($ComputerName) -or $ComputerName -eq 'localhost') {
    $result = Invoke-Local -Script $core -ParamMap $paramMap
  } else {
    $result = Invoke-Remote -ComputerName $ComputerName -Script $core -ParamMap $paramMap
  }

  # Si OutCsv se especificó, ya se fue escribiendo incrementalmente.
  # Aun así devolvemos a la tubería los objetos (por si el caller los captura).
  if (-not $OutCsv) {
    $result
  } else {
    Write-Host "✅ CSV incremental: $OutCsv"
  }
}
catch {
  Write-Error $_.Exception.Message
  throw
}
