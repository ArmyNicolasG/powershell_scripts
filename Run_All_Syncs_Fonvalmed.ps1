<# 
Runner de ejecuciones para sincronización incremental con AzCopy sync.
Invoca ps_SyncAzureFiles.ps1 (modo orquestador con ventanas) para cada conjunto.

Mejora: crea una carpeta de corrida con timestamp a partir de LogRoot y guarda todo allí.
#>

[CmdletBinding()]
param(
  [string] $SyncScriptPath = "C:\Source\scripts\ps_SyncAzureFiles.ps1",
  [string] $AzCopyPath     = "C:\Source\scripts\azcopy.exe",
  [string] $SasNas1        = "",
  [string] $SasNas2        = "",
  [string] $LogRoot        = "C:\Source\logs\sync",
  [int]    $MaxOpenWindows = 3,
  [int]    $AzConcurrency  = 16,
  [int]    $AzBufferGB     = 1
)

function Ensure-Dir([string]$p) {
  if ([string]::IsNullOrWhiteSpace($p)) { return }
  if (-not (Test-Path -LiteralPath $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null }
}

function Read-RequiredValue {
  param(
    [Parameter(Mandatory)] [string] $Prompt,
    [string] $CurrentValue
  )

  if (-not [string]::IsNullOrWhiteSpace($CurrentValue)) { return $CurrentValue.Trim() }

  while ($true) {
    $inputValue = Read-Host $Prompt
    if (-not [string]::IsNullOrWhiteSpace($inputValue)) { return $inputValue.Trim() }
    Write-Host "Valor requerido. Intenta nuevamente."
  }
}

function Get-SasForStorageAccount {
  param([Parameter(Mandatory)] [string] $StorageAccount)

  $account = $StorageAccount.Trim().ToLowerInvariant()
  if ($account -match "nas1") {
    return [pscustomobject]@{ Token = $SasNas1; Label = "NAS1" }
  }
  if ($account -match "nas2") {
    return [pscustomobject]@{ Token = $SasNas2; Label = "NAS2" }
  }

  throw "No se pudo determinar el SAS para StorageAccount '$StorageAccount'. Debe contener 'nas1' o 'nas2'."
}

function New-RunLogRoot {
  param([Parameter(Mandatory)] [string] $BaseLogRoot)

  $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
  $runRoot = ($BaseLogRoot.TrimEnd("\") + "-" + $stamp)
  Ensure-Dir $runRoot
  $runRoot
}

function Run-Sync {
  param(
    [Parameter(Mandatory)] [string] $SourceRoot,
    [Parameter(Mandatory)] [string] $StorageAccount,
    [Parameter(Mandatory)] [string] $ShareName,
    [Parameter(Mandatory)] [string] $DestBaseSubPath,
    [Parameter(Mandatory)] [string] $LogFile,

    [string] $DoOnly,
    [string] $Exclude,
    [switch] $PreservePermissions
  )

  $sasSelection = Get-SasForStorageAccount -StorageAccount $StorageAccount

  $args = @(
    $SyncScriptPath,
    "-OpenNewWindows",
    "-SourceRoot", $SourceRoot,
    "-StorageAccount", $StorageAccount,
    "-ShareName", $ShareName,
    "-DestBaseSubPath", $DestBaseSubPath,
    "-Sas", $sasSelection.Token,
    "-AzCopyPath", $AzCopyPath,
    "-LogFile", $LogFile,
    "-MaxOpenWindows", $MaxOpenWindows,
    "-AzConcurrency", $AzConcurrency,
    "-AzBufferGB", $AzBufferGB
  )

  if ($PreservePermissions) { $args += "-PreservePermissions" }
  if (-not [string]::IsNullOrWhiteSpace($DoOnly)) { $args += @("-DoOnly", $DoOnly) }
  if (-not [string]::IsNullOrWhiteSpace($Exclude)) { $args += @("-Exclude", $Exclude) }

  Write-Host ""
  Write-Host "== Ejecutando =="
  Write-Host ("SourceRoot: {0}" -f $SourceRoot)
  Write-Host ("Destino:    {0} / {1} / {2}" -f $StorageAccount, $ShareName, $DestBaseSubPath)
  Write-Host ("SAS usado:  {0}" -f $sasSelection.Label)
  if ($DoOnly) { Write-Host ("DoOnly:     {0}" -f $DoOnly) }
  if ($Exclude) { Write-Host ("Exclude:    {0}" -f $Exclude) }
  Write-Host ("LogFile:    {0}" -f $LogFile)

  & pwsh @args
  if ($LASTEXITCODE -ne 0) {
    Write-Host ("Error al ejecutar. ExitCode={0}" -f $LASTEXITCODE)
  }
}

# base + carpeta de corrida con timestamp
Ensure-Dir $LogRoot
$RunLogRoot = New-RunLogRoot -BaseLogRoot $LogRoot

Write-Host ""
Write-Host ("Logs de esta corrida: {0}" -f $RunLogRoot)

$SasNas1 = Read-RequiredValue -Prompt "Ingresa el token SAS para NAS1 (storagefonvalmednas1)" -CurrentValue $SasNas1
$SasNas2 = Read-RequiredValue -Prompt "Ingresa el token SAS para NAS2 (storagefonvalmednas2)" -CurrentValue $SasNas2

# NAS2
Ensure-Dir (Join-Path $RunLogRoot "nas2-archivo-central")
Run-Sync `
  -SourceRoot "\\NASFVMED2\ARCHIVO_CENTRAL_2024" `
  -StorageAccount "storagefonvalmednas2" `
  -ShareName "nasfvmed2-archivo-central" `
  -DestBaseSubPath "ARCHIVO_CENTRAL_2024" `
  -LogFile (Join-Path $RunLogRoot "nas2-archivo-central\sync.log") `
  -PreservePermissions

# NAS1 BACKUP - DISCO PST
Ensure-Dir (Join-Path $RunLogRoot "nas1-hot\backup-disco-pst")
Run-Sync `
  -SourceRoot "\\Nasfvmed1\DISCOS EXTERNOS\BACKUP - DISCO PST" `
  -StorageAccount "storagefonvalmednas1" `
  -ShareName "nasfvmed1-hot" `
  -DestBaseSubPath "BACKUP - DISCO PST" `
  -LogFile (Join-Path $RunLogRoot "nas1-hot\backup-disco-pst\sync.log") `
  -DoOnly "Correos PST MANUAL 2025" `
  -PreservePermissions

Ensure-Dir (Join-Path $RunLogRoot "nas1-cold\backup-disco-pst")
Run-Sync `
  -SourceRoot "\\Nasfvmed1\DISCOS EXTERNOS\BACKUP - DISCO PST" `
  -StorageAccount "storagefonvalmednas1" `
  -ShareName "nasfvmed1-cold" `
  -DestBaseSubPath "BACKUP - DISCO PST" `
  -LogFile (Join-Path $RunLogRoot "nas1-cold\backup-disco-pst\sync.log") `
  -DoOnly "DISCO PERSONAL" `
  -PreservePermissions

# NAS1 BACKUP WEB FONVALMED
Ensure-Dir (Join-Path $RunLogRoot "nas1-hot\backup-web-fonvalmed")
Run-Sync `
  -SourceRoot "\\Nasfvmed1\DISCOS EXTERNOS\BACKUP WEB FONVALMED" `
  -StorageAccount "storagefonvalmednas1" `
  -ShareName "nasfvmed1-hot" `
  -DestBaseSubPath "BACKUP WEB FONVALMED" `
  -LogFile (Join-Path $RunLogRoot "nas1-hot\backup-web-fonvalmed\sync.log") `
  -DoOnly "Backups Fonvalmed Web" `
  -PreservePermissions

# NAS1 DISCO1
Ensure-Dir (Join-Path $RunLogRoot "nas1-cold\disco1")
Run-Sync `
  -SourceRoot "\\Nasfvmed1\DISCOS EXTERNOS\DISCO1" `
  -StorageAccount "storagefonvalmednas1" `
  -ShareName "nasfvmed1-cold" `
  -DestBaseSubPath "DISCO1" `
  -LogFile (Join-Path $RunLogRoot "nas1-cold\disco1\sync.log") `
  -Exclude "BACKUP DANA PORTATIL" `
  -PreservePermissions

# NAS1 DISCO2 COLD
Ensure-Dir (Join-Path $RunLogRoot "nas1-cold\disco2")
Run-Sync `
  -SourceRoot "\\Nasfvmed1\DISCOS EXTERNOS\DISCO2" `
  -StorageAccount "storagefonvalmednas1" `
  -ShareName "nasfvmed1-cold" `
  -DestBaseSubPath "DISCO2" `
  -LogFile (Join-Path $RunLogRoot "nas1-cold\disco2\sync.log") `
  -DoOnly "1_4_1 PASAO;1_4_5 INFORMES MENSUALES;1_4_7 SST;02_AMB;CADENA;CONSORCIO LOS PARRA 2021;FONVALMED" `
  -PreservePermissions

# NAS1 DISCO2 COOL
Ensure-Dir (Join-Path $RunLogRoot "nas1-cool\disco2")
Run-Sync `
  -SourceRoot "\\Nasfvmed1\DISCOS EXTERNOS\DISCO2" `
  -StorageAccount "storagefonvalmednas1" `
  -ShareName "nasfvmed1-cool" `
  -DestBaseSubPath "DISCO2" `
  -LogFile (Join-Path $RunLogRoot "nas1-cool\disco2\sync.log") `
  -DoOnly "1_4_4;1_4_6 INFORME FINAL;BACKUP FUNCIONARIOS;BACKUP WEB FONVALMED;Backups Fonvalmed Web;COMPONENTE SST;EMPALME ALCALDIA 2023;GERAL CORREO" `
  -Exclude "INFO ANDRES GERALDO" `
  -PreservePermissions

# NAS1 DISCO2 CORREO* COOL
$disco2Root = "\\Nasfvmed1\DISCOS EXTERNOS\DISCO2"
$correoNames = (Get-ChildItem -LiteralPath $disco2Root -Directory -ErrorAction SilentlyContinue | Where-Object { $_.Name -like "CORREO*" }).Name
if ($correoNames -and $correoNames.Count -gt 0) {
  Ensure-Dir (Join-Path $RunLogRoot "nas1-cool\disco2-correos")
  $correoDoOnly = $correoNames -join ";"
  Run-Sync `
    -SourceRoot $disco2Root `
    -StorageAccount "storagefonvalmednas1" `
    -ShareName "nasfvmed1-cool" `
    -DestBaseSubPath "DISCO2" `
    -LogFile (Join-Path $RunLogRoot "nas1-cool\disco2-correos\sync.log") `
    -DoOnly $correoDoOnly `
    -Exclude "INFO ANDRES GERALDO" `
    -PreservePermissions
} else {
  Write-Host ""
  Write-Host "No se encontraron carpetas CORREO* en DISCO2. Se omite este bloque."
}

# NAS1 DISCO3
Ensure-Dir (Join-Path $RunLogRoot "nas1-cold\disco3")
Run-Sync `
  -SourceRoot "\\Nasfvmed1\DISCOS EXTERNOS\DISCO3" `
  -StorageAccount "storagefonvalmednas1" `
  -ShareName "nasfvmed1-cold" `
  -DestBaseSubPath "DISCO3" `
  -LogFile (Join-Path $RunLogRoot "nas1-cold\disco3\sync.log") `
  -DoOnly "DISCO (C) JHON MORENO;DISCO BACKUP;INFO DISCO DURO PORTATIL" `
  -PreservePermissions

Ensure-Dir (Join-Path $RunLogRoot "nas1-cool\disco3")
Run-Sync `
  -SourceRoot "\\Nasfvmed1\DISCOS EXTERNOS\DISCO3" `
  -StorageAccount "storagefonvalmednas1" `
  -ShareName "nasfvmed1-cool" `
  -DestBaseSubPath "DISCO3" `
  -LogFile (Join-Path $RunLogRoot "nas1-cool\disco3\sync.log") `
  -DoOnly "YURLEY" `
  -PreservePermissions

# NAS1 HISTORIA DE LOS FUNCIONARIOS
Ensure-Dir (Join-Path $RunLogRoot "nas1-cold\historia-funcionarios")
Run-Sync `
  -SourceRoot "\\Nasfvmed1\DISCOS EXTERNOS\HISTORIA DE LOS FUNCIONARIOS" `
  -StorageAccount "storagefonvalmednas1" `
  -ShareName "nasfvmed1-cold" `
  -DestBaseSubPath "HISTORIA DE LOS FUNCIONARIOS" `
  -LogFile (Join-Path $RunLogRoot "nas1-cold\historia-funcionarios\sync.log") `
  -PreservePermissions

# NAS1 PST-CORREO CONTACTENOS
Ensure-Dir (Join-Path $RunLogRoot "nas1-cool\pst-correo-contactenos")
Run-Sync `
  -SourceRoot "\\Nasfvmed1\DISCOS EXTERNOS\PST-CORREO CONTACTENOS" `
  -StorageAccount "storagefonvalmednas1" `
  -ShareName "nasfvmed1-cool" `
  -DestBaseSubPath "PST-CORREO CONTACTENOS" `
  -LogFile (Join-Path $RunLogRoot "nas1-cool\pst-correo-contactenos\sync.log") `
  -DoOnly "CORREO CONTACTENOS 04-09-2023;CORREO CONTACTENOS 07-11-2024;CORREO CONTACTENOS 09-04-2024" `
  -PreservePermissions

Ensure-Dir (Join-Path $RunLogRoot "nas1-hot\pst-correo-contactenos")
Run-Sync `
  -SourceRoot "\\Nasfvmed1\DISCOS EXTERNOS\PST-CORREO CONTACTENOS" `
  -StorageAccount "storagefonvalmednas1" `
  -ShareName "nasfvmed1-hot" `
  -DestBaseSubPath "PST-CORREO CONTACTENOS" `
  -LogFile (Join-Path $RunLogRoot "nas1-hot\pst-correo-contactenos\sync.log") `
  -DoOnly "CORREO CONTACTENOS 16-07-2025" `
  -PreservePermissions

Write-Host ""
Write-Host "Runner finalizado. Las ventanas seguiran ejecutandose segun la cola y MaxOpenWindows."