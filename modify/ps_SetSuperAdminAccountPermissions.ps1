[CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='Medium')]
param(
  [Parameter(Mandatory=$true)]
  [string]$Path,

  [Parameter(Mandatory=$true)]
  [string]$Account,

  [string]$LogPath,

  [switch]$TakeOwnership,

  [switch]$UseLongPath  # opcional: forzar prefijo \\?\ para rutas largas
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# --- Helpers ---
function Write-Log {
  param([string]$Message, [string]$Level = 'INFO')
  $ts = (Get-Date).ToString('yyyy-MM-ddTHH:mm:ss.fffzzz', [System.Globalization.CultureInfo]::InvariantCulture)
  $line = "[$ts][$Level] $Message"
  Write-Host $line
  if ($script:logWriter) { $script:logWriter.WriteLine($line) }
}

function Ensure-Dir([string]$FilePath) {
  $dir = Split-Path -Parent $FilePath
  if ($dir -and -not (Test-Path -LiteralPath $dir)) {
    New-Item -ItemType Directory -Path $dir | Out-Null
  }
}

# PSPath -> ruta nativa para procesos externos (UNC o local)
function Get-NativePath {
  param([Parameter(Mandatory=$true)][string]$LiteralPath, [switch]$Long)
  $native = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($LiteralPath)
  if (-not $Long) { return $native }

  if ($native -like '\\*') {
    # UNC -> \\?\UNC\server\share\resto
    if ($native -like '\\?\*') { return $native } # ya es long
    return '\\?\UNC\' + $native.TrimStart('\')
  } else {
    # Local -> \\?\C:\...
    if ($native -like '\\?\*') { return $native }
    return '\\?\' + $native
  }
}

function Resolve-AccountToSidOrName([string]$acct) {
  try {
    $nt = New-Object System.Security.Principal.NTAccount($acct)
    $sid = $nt.Translate([System.Security.Principal.SecurityIdentifier])
    return "*$($sid.Value)"  # icacls acepta *SID
  } catch { return $acct }
}

function Invoke-External {
  param([string]$FilePath, [string]$ArgumentString)
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $FilePath
  $psi.Arguments = $ArgumentString
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError  = $true
  $psi.UseShellExecute = $false
  $psi.CreateNoWindow = $true
  $p = New-Object System.Diagnostics.Process
  $p.StartInfo = $psi
  [void]$p.Start()
  while (-not $p.HasExited) {
    if (-not $p.StandardOutput.EndOfStream) { Write-Log ($p.StandardOutput.ReadLine()) 'ICACLS' }
    if (-not $p.StandardError.EndOfStream)  { Write-Log ($p.StandardError.ReadLine())  'ICACLS-ERR' }
    Start-Sleep -Milliseconds 50
  }
  while (-not $p.StandardOutput.EndOfStream) { Write-Log ($p.StandardOutput.ReadLine()) 'ICACLS' }
  while (-not $p.StandardError.EndOfStream)  { Write-Log ($p.StandardError.ReadLine())  'ICACLS-ERR' }
  return $p.ExitCode
}

# --- Inicio ---
if (-not (Test-Path -LiteralPath $Path)) {
  throw "La ruta no existe o no es accesible: $Path"
}

# RUTA NATIVA (¡clave!)
$nativePath = Get-NativePath -LiteralPath $Path -Long:$UseLongPath
# ruta “bonita” para mostrar en log (sin prefijo PSProvider)
$displayPath = Get-NativePath -LiteralPath $Path

# Logging
$script:logWriter = $null
try {
  if ($LogPath) {
    Ensure-Dir $LogPath
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    $script:logWriter = New-Object System.IO.StreamWriter($LogPath, $true, $utf8NoBom)
    $script:logWriter.AutoFlush = $true
  }
} catch { Write-Warning "No se pudo abrir el log en '$LogPath': $($_.Exception.Message)" }

Write-Log "Inicio. Path='$displayPath' Account='$Account' TakeOwnership=$TakeOwnership LongPath=$UseLongPath"

$icacls  = (Get-Command icacls.exe -ErrorAction Stop).Source
$takeown = (Get-Command takeown.exe -ErrorAction SilentlyContinue).Source

$principal = Resolve-AccountToSidOrName $Account
Write-Log "Principal para icacls: $principal"

# 1) (Opcional) tomar propiedad
if ($TakeOwnership) {
  if ($PSCmdlet.ShouldProcess($displayPath, "take ownership recursively")) {
    if (-not $takeown) {
      Write-Log "takeown.exe no disponible; omitiendo TakeOwnership" "WARN"
    } else {
      $argStr = "/F `"$nativePath`" /R /D Y"
      Write-Log "Ejecutando: takeown $argStr"
      $exit = Invoke-External -FilePath $takeown -ArgumentString $argStr
      Write-Log "takeown exit code: $exit"
    }
  } else {
    Write-Log "WHATIF: takeown /F `"$nativePath`" /R /D Y"
  }
}

# 2) Conceder Control total sin reemplazar ACLs
if ($PSCmdlet.ShouldProcess($displayPath, "grant FullControl (OI)(CI) to $Account recursively")) {
  Write-Log "Concediendo Control total (sin reemplazar ACLs existentes)..."
  $grantSpec = "${principal}:(OI)(CI)F"
  $argStr = "`"$nativePath`" /grant `"$grantSpec`" /T /C"
  Write-Log "Ejecutando: icacls $argStr"
  $exit = Invoke-External -FilePath $icacls -ArgumentString $argStr
  Write-Log "icacls /grant exit code: $exit"
} else {
  Write-Log "WHATIF: icacls `"$nativePath`" /grant `"$($principal):(OI)(CI)F`" /T /C"
}

Write-Log "Fin."
if ($script:logWriter) { $script:logWriter.Dispose() }
