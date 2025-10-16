<#
.SYNOPSIS
  Orquestador: para cada subcarpeta inmediata:
    - ejecuta inventario y subida en paralelo,
    - centraliza logs,
    - opcionalmente copia archivos sueltos de la raíz a "Archivos sueltos pre-migracion",
    - al final genera un resumen consolidado.

.PARAMETER RootPath
  Carpeta raíz (local o UNC) que contiene las subcarpetas a procesar.

.PARAMETER InventoryScript
  Ruta del script de inventario (ps_GetFilesAndFoldersStructure_v2.ps1).

.PARAMETER UploadScript
  Ruta del script de subida (ps_UploadToFileShareFromCsv_v2.ps1 o vNext).

.PARAMETER InventoryLogRoot
  Carpeta base para logs de inventario (se creará <InventoryLogRoot>\<Subcarpeta>\...).

.PARAMETER UploadLogRoot
  Carpeta base para logs de subida (se creará <UploadLogRoot>\<Subcarpeta>\...).

.PARAMETER StorageAccount, ShareName, DestBaseSubPath, Sas, ServiceType, Overwrite, PreservePermissions, AzCopyPath, MaxLogSizeMB
  Passthrough al script de subida.

.PARAMETER MaxParallel
  Cuántas subcarpetas se procesan en paralelo (sin abrir ventanas). Default: 2.

.PARAMETER OpenNewWindows
  Si se indica, abre una ventana pwsh por carpeta y ejecuta inventario + subida allí
  (en ese modo, este proceso no controla la concurrencia).

.PARAMETER IncludeLooseFilesAsFolder
  Si true (default), copia archivos sueltos de la raíz a "Archivos sueltos pre-migracion"
  y procesa esa carpeta también.

.PARAMETER ComputeRootSize
  Pasa -ComputeRootSize al script de inventario.

.OUTPUTS
  Crea <UploadLogRoot>\summary.csv con columnas: Folder, Inv_* y Az_*.

#>

[CmdletBinding()]
param(
  [Parameter(Mandatory)][string]$RootPath,
  [Parameter(Mandatory)][string]$InventoryScript,
  [Parameter(Mandatory)][string]$UploadScript,

  [Parameter(Mandatory)][string]$InventoryLogRoot,
  [Parameter(Mandatory)][string]$UploadLogRoot,

  [Parameter(Mandatory)][string]$StorageAccount,
  [Parameter(Mandatory)][string]$ShareName,
  [Parameter(Mandatory)][string]$DestBaseSubPath,
  [Parameter(Mandatory)][string]$Sas,

  [ValidateSet('FileShare','Blob')][string]$ServiceType = 'FileShare',
  [ValidateSet('ifSourceNewer','true','false','prompt')][string]$Overwrite = 'ifSourceNewer',
  [switch]$PreservePermissions,
  [string]$AzCopyPath = 'azcopy',
  [int]$MaxLogSizeMB = 64,

  [int]$MaxParallel = 2,
  [switch]$OpenNewWindows,
  [switch]$IncludeLooseFilesAsFolder = $true,
  [switch]$ComputeRootSize
)

# ---------------- Helpers ----------------
function Ensure-Dir([string]$p){ if (-not (Test-Path -LiteralPath $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null } }
function Sanitize-Name([string]$n){ if ([string]::IsNullOrWhiteSpace($n)) { return 'unnamed' }; $s=($n -replace '[<>:"/\\|?*\x00-\x1F]','_').Trim().TrimEnd('.'); if ($s) { $s } else { 'unnamed' } }
function Join-UrlPath([string]$a,[string]$b){ $a=$a -replace '\\','/'; if($a -and -not $a.StartsWith('/')){$a='/'+$a}; $b=$b -replace '\\','/'; if($a.EndsWith('/')){$a+$b}else{$a+'/'+$b} }
function Now() { (Get-Date).ToString('yyyy-MM-dd HH:mm:ss.fff') }
function Info([string]$m){ Write-Host "[$(Now)][INFO] $m" }
function Warn([string]$m){ Write-Host "[$(Now)][WARN] $m" -ForegroundColor Yellow }

# ---------------- Preparación raíz ----------------
$root = (Resolve-Path -LiteralPath $RootPath).Path
Ensure-Dir $InventoryLogRoot
Ensure-Dir $UploadLogRoot

# Archivos sueltos a subcarpeta
$looseFolder = Join-Path $root 'Archivos sueltos pre-migracion'
if ($IncludeLooseFilesAsFolder) {
  try {
    $files = Get-ChildItem -LiteralPath $root -File -Force -ErrorAction SilentlyContinue
    if ($files.Count -gt 0) {
      Ensure-Dir $looseFolder
      Info "Copiando $($files.Count) archivos sueltos -> '$looseFolder'."
      foreach ($f in $files) {
        try { Copy-Item -LiteralPath $f.FullName -Destination (Join-Path $looseFolder $f.Name) -Force -ErrorAction Stop }
        catch { Warn "No se pudo copiar '$($f.FullName)': $($_.Exception.Message)" }
      }
    }
  } catch { Warn "Fallo al enumerar/copiar archivos sueltos: $($_.Exception.Message)" }
}

# Subcarpetas inmediatas
$folders = @(Get-ChildItem -LiteralPath $root -Directory -Force -ErrorAction SilentlyContinue)
if ($IncludeLooseFilesAsFolder -and (Test-Path -LiteralPath $looseFolder)) {
  $folders = @($folders + (Get-Item -LiteralPath $looseFolder))
}
if ($folders.Count -eq 0) { Info "No hay subcarpetas para procesar."; return }

Info ("Carpetas a procesar: {0}" -f $folders.Count)

# ---------------- Modo Ventanas ----------------
if ($OpenNewWindows) {
  $pwshExe = (Get-Command pwsh -ErrorAction SilentlyContinue)?.Source; if (-not $pwshExe) { $pwshExe='pwsh' }
  foreach($dir in $folders){
    $name=$dir.Name; $safe=Sanitize-Name $name
    $invLog=Join-Path $InventoryLogRoot $safe; $upLog=Join-Path $UploadLogRoot $safe
    Ensure-Dir $invLog; Ensure-Dir $upLog
    $destSub = Join-UrlPath $DestBaseSubPath $name

    $cmd = @"
`$ErrorActionPreference='Continue';
Write-Host '=== ($name) INVENTARIO -> $($dir.FullName) ===';
& '$InventoryScript' -Path '$($dir.FullName)' -LogDir '$invLog' $(if($ComputeRootSize){'-ComputeRootSize'});
Write-Host '=== ($name) SUBIDA -> $($dir.FullName) ===';
& '$UploadScript' -SourceRoot '$($dir.FullName)' -StorageAccount '$StorageAccount' -ShareName '$ShareName' -DestSubPath '$destSub' -Sas '$Sas' -ServiceType $ServiceType -Overwrite $Overwrite $(if($PreservePermissions){'-PreservePermissions'}) -AzCopyPath '$AzCopyPath' -LogDir '$upLog' -MaxLogSizeMB $MaxLogSizeMB;
Write-Host '=== ($name) FINALIZADO ===';
"@
    Start-Process -FilePath $pwshExe -ArgumentList @('-NoLogo','-NoExit','-Command', $cmd) | Out-Null
    Info "Ventana lanzada para: $name"
  }
  Info "Se lanzaron $($folders.Count) ventanas. Cierra esta consola si quieres."
  return
}

# ---------------- Función por carpeta (inyectada al paralelo) ----------------
function Invoke-FolderWork {
  param(
    [string]$Folder,
    [string]$InventoryScript, [string]$InventoryLogRoot, [switch]$ComputeRootSize,
    [string]$UploadScript,    [string]$UploadLogRoot,
    [string]$StorageAccount,  [string]$ShareName, [string]$DestBaseSubPath, [string]$Sas,
    [ValidateSet('FileShare','Blob')]$ServiceType = 'FileShare',
    [string]$AzCopyPath = 'azcopy',
    [ValidateSet('ifSourceNewer','true','false','prompt')][string]$Overwrite = 'ifSourceNewer',
    [switch]$PreservePermissions,
    [int]$MaxLogSizeMB = 64
  )

  $folderName = Split-Path $Folder -Leaf
  $invLogDir  = Join-Path $InventoryLogRoot $folderName
  $uplLogDir  = Join-Path $UploadLogRoot    $folderName
  New-Item -ItemType Directory -Force -Path $invLogDir,$uplLogDir | Out-Null

  Write-Host "[INFO] [$folderName] Inventario -> $invLogDir"

  # ==== FIX: splatting con hashtable (no arrays) ====
  $invArgs = @{
    Path    = $Folder
    LogDir  = $invLogDir
  }
  if ($ComputeRootSize) { $invArgs.ComputeRootSize = $true }

  & $InventoryScript @invArgs 2>&1 | ForEach-Object { Write-Host $_ }

  Write-Host "[INFO] [$folderName] Subida -> $uplLogDir"

  # ==== FIX: usar parámetros de la función, sin $using:, y normalizar '/' ====
  $destUrlSub = ($DestBaseSubPath -replace '\\','/').TrimEnd('/')
  if ($destUrlSub) { $destUrlSub = "$destUrlSub/$folderName" } else { $destUrlSub = $folderName }

  $uplArgs = @{
    SourceRoot        = $Folder
    StorageAccount    = $StorageAccount
    ShareName         = $ShareName
    DestSubPath       = $destUrlSub
    Sas               = $Sas
    ServiceType       = $ServiceType
    LogDir            = $uplLogDir
    AzCopyPath        = $AzCopyPath
    Overwrite         = $Overwrite
    MaxLogSizeMB      = $MaxLogSizeMB
  }
  if ($PreservePermissions) { $uplArgs.PreservePermissions = $true }

  & $UploadScript @uplArgs 2>&1 | ForEach-Object { Write-Host $_ }

  [pscustomobject]@{ Folder = $Folder; Status = 'Done' }
}

# ---------------- Modo Paralelo (sin ventanas) ----------------
$throttle = [Math]::Max(1,$MaxParallel)
Info "Procesando en paralelo con Throttle=$throttle (inventario y subida en paralelo por carpeta)."

# Paquete de parámetros comunes para inyectar al runspace paralelo
$common = @{
  InventoryScript     = $InventoryScript
  InventoryLogRoot    = $InventoryLogRoot
  ComputeRootSize     = $ComputeRootSize
  UploadScript        = $UploadScript
  UploadLogRoot       = $UploadLogRoot
  StorageAccount      = $StorageAccount
  ShareName           = $ShareName
  DestBaseSubPath     = $DestBaseSubPath
  Sas                 = $Sas
  ServiceType         = $ServiceType
  AzCopyPath          = $AzCopyPath
  Overwrite           = $Overwrite
  PreservePermissions = $PreservePermissions
  MaxLogSizeMB        = $MaxLogSizeMB
}

if ($PSVersionTable.PSVersion.Major -ge 7) {
  # Inyectar la función en cada runspace
  $funcDef = ${function:Invoke-FolderWork}.Ast.Extent.Text
  $folders | ForEach-Object -Parallel {
    Invoke-Expression $using:funcDef
    Invoke-FolderWork @using:common -Folder $_.FullName
  } -ThrottleLimit $throttle
}
else {
  # Compatibilidad PS 5.1
  try { Import-Module ThreadJob -ErrorAction Stop } catch { Warn "ThreadJob no disponible; usando Start-Job." ; $useStartJob=$true }
  $jobs=@()
  foreach ($d in $folders) {
    while ( ($jobs | Where-Object { $_.State -eq 'Running' }).Count -ge $throttle ) {
      $done = Wait-Job -Job $jobs -Any -Timeout 2
      if ($done) { Receive-Job $done | ForEach-Object { Write-Host $_ }; Remove-Job $done; $jobs = $jobs | Where-Object Id -ne $done.Id }
    }
    $scriptBlock = {
      param($Folder,$Common,$FuncText)
      Invoke-Expression $FuncText
      Invoke-FolderWork @Common -Folder $Folder
    }
    if ($useStartJob) { $jobs += Start-Job       -ScriptBlock $scriptBlock -ArgumentList $d.FullName, $common, (${function:Invoke-FolderWork}.Ast.Extent.Text) }
    else              { $jobs += Start-ThreadJob -ScriptBlock $scriptBlock -ArgumentList $d.FullName, $common, (${function:Invoke-FolderWork}.Ast.Extent.Text) }
  }
  if ($jobs.Count) { Wait-Job -Job $jobs | Out-Null; foreach($j in $jobs){ Receive-Job $j | ForEach-Object { Write-Host $_ }; Remove-Job $j } }
}

Info "Procesamiento paralelo finalizado. Preparando resumen…"

# ---------------- Resumen final ----------------
function Parse-FolderInfo([string]$folderLogDir){
  $p = Join-Path $folderLogDir 'folder-info.txt'
  if (-not (Test-Path -LiteralPath $p)) { return @{} }
  $h=@{}; foreach($line in (Get-Content -LiteralPath $p -ErrorAction SilentlyContinue)){ if ($line -match '^\s*([^:]+):\s*(.*)\s*$'){ $h[$matches[1]] = $matches[2] } }
  return $h
}
function Get-LatestUploadLog([string]$uploadFolder){
  if (-not (Test-Path -LiteralPath $uploadFolder)) { return $null }
  (Get-ChildItem -LiteralPath $uploadFolder -File -Filter 'upload-logs-*.txt' -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1)?.FullName
}
function Parse-AzCopySummary([string]$logPath){
  if (-not $logPath -or -not (Test-Path -LiteralPath $logPath)) { return @{} }
  $h=@{}; $lines = Get-Content -LiteralPath $logPath -ErrorAction SilentlyContinue
  $idx = ($lines | Select-String -SimpleMatch 'RESUMEN DE AZCOPY' | Select-Object -First 1).LineNumber
  if ($idx) {
    for($i=$idx; $i -lt $lines.Count; $i++){
      $l=$lines[$i]
      if     ($l -match '^\s*JobID:\s*(.+)$')                 { $h['Az_JobID']=$matches[1].Trim() }
      elseif ($l -match '^\s*Estado:\s*(.+)$')                { $h['Az_Status']=$matches[1].Trim() }
      elseif ($l -match '^\s*Total transfers:\s*(\d+)')       { $h['Az_Total']=[int]$matches[1] }
      elseif ($l -match '^\s*Completados:\s*(\d+)')           { $h['Az_Completed']=[int]$matches[1] }
      elseif ($l -match '^\s*Fallidos:\s*(\d+)')              { $h['Az_Failed']=[int]$matches[1] }
      elseif ($l -match '^\s*Saltados:\s*(\d+)')              { $h['Az_Skipped']=[int]$matches[1] }
      elseif ($l -match '^\s*Bytes enviados:\s*(\d+)')        { $h['Az_Bytes']=[int64]$matches[1] }
      elseif ($l -match '^\s*Duración \(s\):\s*([\d\.]+)')    { $h['Az_DurationSec']=[double]$matches[1] }
    }
  }
  return $h
}

$summaryRows = New-Object System.Collections.Generic.List[object]
foreach($dir in $folders){
  $name = $dir.Name
  $invLog = Join-Path $InventoryLogRoot $name
  $upLog  = Join-Path $UploadLogRoot $name
  $inv = Parse-FolderInfo $invLog
  $azp = Get-LatestUploadLog $upLog
  $az  = Parse-AzCopySummary $azp

  $summaryRows.Add([pscustomobject]@{
    Folder                 = $name
    Inv_TotalFolders       = ($inv['TotalFolders'] ?? '')
    Inv_TotalFiles         = ($inv['TotalFiles'] ?? '')
    Inv_AccessibleFolders  = ($inv['AccessibleFolders'] ?? '')
    Inv_AccessibleFiles    = ($inv['AccessibleFiles'] ?? '')
    Inv_InaccessibleFiles  = ($inv['InaccessibleFiles'] ?? '')
    Inv_RenamedFolders     = ($inv['RenamedOrInvalidFolders'] ?? '')
    Inv_RenamedFiles       = ($inv['RenamedOrInvalidFiles'] ?? '')
    Inv_TotalBytes         = ($inv['TotalBytes'] ?? '')
    Az_Status              = ($az['Az_Status'] ?? '')
    Az_Total               = ($az['Az_Total'] ?? '')
    Az_Completed           = ($az['Az_Completed'] ?? '')
    Az_Failed              = ($az['Az_Failed'] ?? '')
    Az_Skipped             = ($az['Az_Skipped'] ?? '')
    Az_Bytes               = ($az['Az_Bytes'] ?? '')
    Az_DurationSec         = ($az['Az_DurationSec'] ?? '')
    Az_JobID               = ($az['Az_JobID'] ?? '')
    UploadLog              = $azp
  }) | Out-Null
}

$summaryCsv = Join-Path $UploadLogRoot 'summary.csv'
$summaryRows | Export-Csv -LiteralPath $summaryCsv -NoTypeInformation -Encoding UTF8

$summaryRows |
  Select-Object Folder,Inv_TotalFiles,Inv_AccessibleFiles,Inv_InaccessibleFiles,Inv_TotalBytes,
                Az_Status,Az_Total,Az_Completed,Az_Failed,Az_Skipped,Az_Bytes,Az_DurationSec |
  Format-Table -AutoSize

Info "Resumen CSV -> $summaryCsv"