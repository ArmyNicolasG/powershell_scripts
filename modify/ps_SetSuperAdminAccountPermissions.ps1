<# 
.SYNOPSIS
  Agrega (no reemplaza) Control total para una cuenta de dominio a todos los archivos y carpetas bajo una ruta, recursivo.
  Mantiene permisos existentes y herencia tal como están. Soporta logging en tiempo real.

.PARAMETER Path
  Ruta raíz (ej. D:\Datos o \\FS01\Compartido).

.PARAMETER Account
  Cuenta a la que se le otorgará Control total (ej. CONTOSO\svc_auditor o auditor@contoso.com).

.PARAMETER LogPath
  Ruta de archivo .log (UTF-8) donde se registrará la ejecución en tiempo real (opcional).

.PARAMETER TakeOwnership
  Si se indica, INTENTA tomar propiedad de los objetos antes de conceder permisos (ayuda con Access Denied al cambiar ACL).
  No cambia herencia ni borra ACEs, pero sí cambia el OWNER del objeto (metadato de seguridad). Úsalo sólo si lo necesitas.

.EXAMPLE
  .\ps_SetSuperAdminAccountPermissions.ps1 -Path "D:\Data" -Account "CONTOSO\svc_auditor" -LogPath "C:\Temp\grant.log"

.EXAMPLE
  .\ps_SetSuperAdminAccountPermissions.ps1 -Path "\\FS01\Compartido" -Account "auditor@contoso.com" -TakeOwnership -LogPath "C:\Temp\grant.log"

.NOTES
  Ejecuta en consola elevada. `-WhatIf` y `-Confirm` funcionan al ser función avanzada con SupportsShouldProcess.
#>

[CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='Medium')]
param(
  [Parameter(Mandatory=$true)]
  [string]$Path,

  [Parameter(Mandatory=$true)]
  [string]$Account,

  [string]$LogPath,

  [switch]$TakeOwnership
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ----- Helpers -----
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

# Resuelve cuenta a SID si es posible (icacls acepta SID con prefijo *)
function Resolve-AccountToSidOrName([string]$acct) {
  try {
    $nt = New-Object System.Security.Principal.NTAccount($acct)
    $sid = $nt.Translate([System.Security.Principal.SecurityIdentifier])
    return "*$($sid.Value)"  # formato icacls para SID explícito
  } catch {
    return $acct            # DOM\sam o UPN; icacls intentará resolverlo
  }
}

# Ejecuta un proceso y vuelca stdout/stderr a log en tiempo real
function Invoke-External {
  param(
    [string]$FilePath,
    [string]$ArgumentString   # una sola cadena con todos los argumentos ya entrecomillados
  )
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
    if (-not $p.StandardOutput.EndOfStream) {
      Write-Log ($p.StandardOutput.ReadLine()) 'ICACLS'
    }
    if (-not $p.StandardError.EndOfStream) {
      Write-Log ($p.StandardError.ReadLine()) 'ICACLS-ERR'
    }
    Start-Sleep -Milliseconds 50
  }
  while (-not $p.StandardOutput.EndOfStream) {
    Write-Log ($p.StandardOutput.ReadLine()) 'ICACLS'
  }
  while (-not $p.StandardError.EndOfStream) {
    Write-Log ($p.StandardError.ReadLine()) 'ICACLS-ERR'
  }
  return $p.ExitCode
}

# ----- Inicio -----
if (-not (Test-Path -LiteralPath $Path)) {
  throw "La ruta no existe o no es accesible: $Path"
}

$absPath = (Resolve-Path -LiteralPath $Path).Path

# Preparar logging
$script:logWriter = $null
try {
  if ($LogPath) {
    Ensure-Dir $LogPath
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    $script:logWriter = New-Object System.IO.StreamWriter($LogPath, $true, $utf8NoBom)
    $script:logWriter.AutoFlush = $true
  }
} catch {
  Write-Warning "No se pudo abrir el log en '$LogPath': $($_.Exception.Message)"
}

$whatIfActive = $WhatIfPreference -eq $true
Write-Log "Inicio. Path='$absPath' Account='$Account' TakeOwnership=$TakeOwnership WhatIf=$whatIfActive"

# Herramientas
$icacls = (Get-Command icacls.exe -ErrorAction Stop).Source
$takeown = (Get-Command takeown.exe -ErrorAction SilentlyContinue).Source

# Principal para icacls
$principal = Resolve-AccountToSidOrName $Account
Write-Log "Principal para icacls: $principal"

# 1) (Opcional) Tomar propiedad
if ($TakeOwnership) {
  $action = "take ownership recursively"
  $target = $absPath
  if ($PSCmdlet.ShouldProcess($target, $action)) {
    if (-not $takeown) {
      Write-Log "takeown.exe no disponible; omitiendo TakeOwnership" "WARN"
    } else {
      Write-Log "Tomando propiedad (recursivo). Esto NO altera herencia ni ACEs, solo el OWNER."
      # takeown /F "path" /R /D Y
      $argStr = "/F `"$absPath`" /R /D Y"
      $exit = Invoke-External -FilePath $takeown -ArgumentString $argStr
      Write-Log "takeown exit code: $exit"
    }
  } else {
    Write-Log "WHATIF: takeown /F `"$absPath`" /R /D Y"
  }
}

# 2) Conceder Control total sin reemplazar ACLs existentes
# icacls "path" /grant "<principal>:(OI)(CI)F" /T /C
$actionGrant = "grant FullControl (OI)(CI) to $Account recursively"
$targetGrant = $absPath
if ($PSCmdlet.ShouldProcess($targetGrant, $actionGrant)) {
  Write-Log "Concediendo Control total (sin reemplazar ACLs existentes)..."
  $grantSpec = "${principal}:(OI)(CI)F"
  $argStr = "`"$absPath`" /grant `"$grantSpec`" /T /C"
  $exit = Invoke-External -FilePath $icacls -ArgumentString $argStr
  Write-Log "icacls /grant exit code: $exit"
} else {
  Write-Log "WHATIF: icacls `"$absPath`" /grant `"$($principal):(OI)(CI)F`" /T /C"
}

Write-Log "Fin."
if ($script:logWriter) { $script:logWriter.Dispose() }
