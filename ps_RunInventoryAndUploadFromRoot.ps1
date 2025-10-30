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

.PARAMETER AzConcurrency
  Número de procesos concurrentes para cada llamada del comando AzCopy (opcional PERO IMPORTANTE PARA CARPETAS GRANDES).

.PARAMETER AzBufferGB
  Tamaño del buffer en GB para cada llamada del comando AzCopy (opcional PERO IMPORTANTE PARA CARPETAS GRANDES).

.PARAMETER MaxParallel
  Cuántas subcarpetas se procesan en paralelo (sin abrir ventanas). Default: 2.

.PARAMETER OpenNewWindows
  Si se indica, abre una ventana pwsh por carpeta y ejecuta inventario + subida allí
  (en ese modo, este proceso no controla la concurrencia).

.PARAMETER WindowLaunchDelaySeconds
  Tiempo de espera (en segundos) antes de abrir una nueva ventana para cada subcarpeta. Default: 15s. (OpenNewWindows debe estar activo).

.PARAMETER IncludeLooseFilesAsFolder
  Si true (default), copia archivos sueltos de la raíz a "Archivos sueltos pre-migracion"
  y procesa esa carpeta también.

.PARAMETER ComputeRootSize
  Pasa -ComputeRootSize al script de inventario.

.PARAMETER DoUpload
  Si se indica, ejecuta el script de subida, sino, no se ejecutará ninguna subida.

.PARAMETER DoInventory
  Si se indica, ejecuta el script de inventario, sino, no se ejecutará ninguna operación de inventario.

.PARAMETER InventorySummaryCsv
  Si se indica, se generará un CSV de resumen de inventario centralizado, sino, se guardará este archivo en el InventoryLogRoot.

.PARAMETER UploadSummaryCsv
  Si se indica, se generará un CSV de resumen de subida centralizado, sino, se guardará este archivo en el UploadLogRoot.

.OUTPUTS
  Crea <UploadLogRoot>\summary.csv con columnas: Folder, Inv_* y Az_* + Diff/FailedCount.
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$RootPath,

  [Parameter()]
  [string]$InventoryScript,

  [Parameter()]
  [string]$UploadScript,

  [Parameter()]
  [string]$InventoryLogRoot,

  [Parameter()]
  [string]$UploadLogRoot,

  [Parameter()]
  [string]$StorageAccount,

  [Parameter()]
  [string]$ShareName,

  [Parameter()]
  [string]$DestBaseSubPath,

  [Parameter()]
  [string]$Sas,

  [ValidateSet('FileShare','Blob')]
  [string]$ServiceType = 'FileShare',

  [ValidateSet('ifSourceNewer','true','false','prompt')]
  [string]$Overwrite = 'ifSourceNewer',

  [switch]$PreservePermissions,
  [string]$AzCopyPath = 'azcopy',
  [int]$MaxLogSizeMB = 64,

  [int]$MaxParallel = 2,
  [switch]$OpenNewWindows,
  [int]$WindowLaunchDelaySeconds = 15,
  [switch]$IncludeLooseFilesAsFolder = $true,
  [switch]$ComputeRootSize,
  [switch]$DoInventory,   # default: false
  [switch]$DoUpload,      # default: false

  [string]$InventorySummaryCsv,
  [string]$UploadSummaryCsv,

    [string]$DoOnly,    # Lista ; separada de nombres de carpetas a procesar exclusivamente
  [string]$Exclude,    # Lista ; separada de nombres de carpetas a excluir
  [Nullable[int]]$AzConcurrency,
  [Nullable[int]]$AzBufferGB
)


# ---------------- Helpers ----------------
function Ensure-Dir([string]$p){ if (-not (Test-Path -LiteralPath $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null } }
function Sanitize-Name([string]$n){ if ([string]::IsNullOrWhiteSpace($n)) { return 'unnamed' }; $s=($n -replace '[<>:"/\\|?*\x00-\x1F]','_').Trim().TrimEnd('.'); if ($s) { $s } else { 'unnamed' } }
function Join-UrlPath([string]$a,[string]$b){ $a=$a -replace '\\','/'; if($a -and -not $a.StartsWith('/')){$a='/'+$a}; $b=$b -replace '\\','/'; if($a.EndsWith('/')){$a+$b}else{$a+'/'+$b} }
function Now() { (Get-Date).ToString('yyyy-MM-dd HH:mm:ss.fff') }
function Info([string]$m){ Write-Host "[$(Now)][INFO] $m" }
function Warn([string]$m){ Write-Host "[$(Now)][WARN] $m" -ForegroundColor Yellow }
function Parse-NameList([string]$s){
  $set = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
  if ([string]::IsNullOrWhiteSpace($s)) { return $set }
  foreach($raw in ($s -split ';')){
    $n = $raw.Trim()
    if ($n.Length -gt 0) { [void]$set.Add($n) }
  }
  return $set
}


# --- Validación de intención ---
if (-not $DoInventory -and -not $DoUpload) {
   Warn "No se ejecutará nada: faltan flags. Usa -DoInventory y/o -DoUpload."
   Warn "Ejemplos:"
   Warn "  - Solo inventario: .\ps_RunInventoryAndUploadFromRoot.ps1 ... -DoInventory"
   Warn "  - Solo subida:     .\ps_RunInventoryAndUploadFromRoot.ps1 ... -DoUpload"
   Warn "  - Ambos:           .\ps_RunInventoryAndUploadFromRoot.ps1 ... -DoInventory -DoUpload"
  return
}

# ---------------- Preparación raíz ----------------
$root = (Resolve-Path -LiteralPath $RootPath).Path
Ensure-Dir $InventoryLogRoot
Ensure-Dir $UploadLogRoot

# Defaults para CSVs centralizados si no se especifican
if (-not $InventorySummaryCsv -or [string]::IsNullOrWhiteSpace($InventorySummaryCsv)) {
  $InventorySummaryCsv = Join-Path $InventoryLogRoot 'resumen-conciliaciones.csv'
}
if (-not $UploadSummaryCsv -or [string]::IsNullOrWhiteSpace($UploadSummaryCsv)) {
  $UploadSummaryCsv = Join-Path $UploadLogRoot 'resumen-subidas.csv'
}


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

# ---------------- Filtro por -DoOnly y/o -Exclude (coincidencia EXACTA, robusta) ----------------
function Normalize-Name([string]$s) {
  if ([string]::IsNullOrWhiteSpace($s)) { return '' }
  # No colapsar espacios internos; solo normalizar forma Unicode y recortar extremos.
  return $s.Trim().Normalize([System.Text.NormalizationForm]::FormKC)
}

# Construye índice exacto: nombre normalizado -> objeto DirectoryInfo (o similar)
$index = @{}
foreach ($d in $folders) {
  $key = Normalize-Name $d.Name
  if (-not $index.ContainsKey($key)) { $index[$key] = $d }
}

# Convierte entradas de -DoOnly/-Exclude en listas normalizadas (sin colapsar espacios dobles)
$doOnlyList  = @()
$excludeList = @()
if ($DoOnly)  { $doOnlyList  = ($DoOnly  -split ';') | ForEach-Object { Normalize-Name $_ } | Where-Object { $_ -ne '' } }
if ($Exclude) { $excludeList = ($Exclude -split ';') | ForEach-Object { Normalize-Name $_ } | Where-Object { $_ -ne '' } }

# Si hay DoOnly: seleccionar SOLO esas carpetas por igualdad exacta
if ($doOnlyList.Count -gt 0) {
  $selected = New-Object System.Collections.Generic.List[object]
  $missing  = New-Object System.Collections.Generic.List[string]
  foreach ($wanted in $doOnlyList) {
    if ($index.ContainsKey($wanted)) {
      $selected.Add($index[$wanted]) | Out-Null
    } else {
      $missing.Add($wanted) | Out-Null
    }
  }
  $folders = $selected.ToArray()
  if ($missing.Count -gt 0) {
    Warn ("-DoOnly: carpetas no encontradas -> {0}" -f (($missing | ForEach-Object { $_ }) -join '; '))
  }
}

# Si hay Exclude: quitar EXACTAMENTE esas carpetas
if ($excludeList.Count -gt 0 -and $folders.Count -gt 0) {
  $excludeSet = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
  foreach ($e in $excludeList) { [void]$excludeSet.Add($e) }

  $kept = New-Object System.Collections.Generic.List[object]
  $foundExcluded = New-Object System.Collections.Generic.List[string]
  foreach ($d in $folders) {
    $k = Normalize-Name $d.Name
    if ($excludeSet.Contains($k)) { $foundExcluded.Add($d.Name) | Out-Null }
    else                          { $kept.Add($d) | Out-Null }
  }
  $folders = $kept.ToArray()

  if ($foundExcluded.Count -gt 0) {
    Info ("-Exclude: excluidas -> {0}" -f (($foundExcluded | Select-Object -Unique) -join '; '))
  }

  # (Opcional) Aviso sobre excluidas indicadas que no existen
  $allNorm = $index.Keys
  $notFoundEx = $excludeList | Where-Object { $_ -notin $allNorm }
  if ($notFoundEx) {
    Warn ("-Exclude: indicadas pero no encontradas -> {0}" -f ($notFoundEx -join '; '))
  }
}

# Validación final
if ($folders.Count -eq 0) {
  Warn "Después de aplicar filtros (-DoOnly/-Exclude) no hay carpetas para procesar."
  return
}

Info ("Carpetas a procesar (tras filtros): {0}" -f $folders.Count)



# ---------------- Modo Ventanas ----------------
if ($OpenNewWindows) {
  $pwshExe = (Get-Command pwsh -ErrorAction SilentlyContinue)?.Source; if (-not $pwshExe) { $pwshExe='pwsh' }
  foreach($dir in $folders){
    $name=$dir.Name; $safe=Sanitize-Name $name
    $invLog=Join-Path $InventoryLogRoot $safe; $upLog=Join-Path $UploadLogRoot $safe
    Ensure-Dir $invLog; Ensure-Dir $upLog
    $destSub = ($DestBaseSubPath -replace '\\','/').Trim('/')

$cmd = @"
`$ErrorActionPreference='Continue';
$(if($DoInventory){
  "Write-Host '=== ($name) INVENTARIO -> $($dir.FullName) ===';
   & '$InventoryScript' -Path '$($dir.FullName)' -LogDir '$invLog' $(if($ComputeRootSize){'-ComputeRootSize'}) -InventorySummaryCsv '$InventorySummaryCsv';
"
})
$(if($DoUpload){
  "Write-Host '=== ($name) SUBIDA -> $($dir.FullName) ===';
   & '$UploadScript' -SourceRoot '$($dir.FullName)' -StorageAccount '$StorageAccount' -ShareName '$ShareName' -DestSubPath '$destSub' -Sas '$Sas' -ServiceType $ServiceType -Overwrite $Overwrite $(if($PreservePermissions){'-PreservePermissions'}) -AzCopyPath '$AzCopyPath' -LogDir '$upLog' -MaxLogSizeMB $MaxLogSizeMB -UploadSummaryCsv '$UploadSummaryCsv' " +
   $(if($PSBoundParameters.ContainsKey('AzConcurrency')){ " -AzConcurrency $AzConcurrency" } else { "" }) +
   $(if($PSBoundParameters.ContainsKey('AzBufferGB'))   { " -AzBufferGB $AzBufferGB"     } else { "" }) +
   ";
"
})

Write-Host '=== ($name) FINALIZADO ===';
"@

# Codificar el script en UTF-16LE 
$enc = [Convert]::ToBase64String([System.Text.Encoding]::Unicode.GetBytes($cmd))



    Start-Process -FilePath $pwshExe `
  -ArgumentList @(
    '-NoLogo',
    '-NoExit',
    '-NoProfile',
    '-ExecutionPolicy','Bypass',
    '-EncodedCommand', $enc
  ) | Out-Null

    Info "Ventana lanzada para: $name"
    if ($WindowLaunchDelaySeconds -gt 0) { Start-Sleep -Seconds $WindowLaunchDelaySeconds }
  }
  Info "Se lanzaron $($folders.Count) ventanas."
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
    [int]$MaxLogSizeMB = 64,
    [switch]$DoInventory,
    [switch]$DoUpload,
    [string]$InventorySummaryCsv,
    [string]$UploadSummaryCsv,
    [Nullable[int]]$AzConcurrency,
    [Nullable[int]]$AzBufferGB
  )

  $folderName = Split-Path $Folder -Leaf
  $invLogDir  = Join-Path $InventoryLogRoot $folderName
  $uplLogDir  = Join-Path $UploadLogRoot    $folderName
  New-Item -ItemType Directory -Force -Path $invLogDir,$uplLogDir | Out-Null

  if ($DoInventory) {
  Write-Host "[INFO] [$folderName] Inventario -> $invLogDir"
  $invArgs = @{ Path = $Folder; LogDir = $invLogDir }
  if ($ComputeRootSize) { $invArgs.ComputeRootSize = $true }
  $invArgs.InventorySummaryCsv = $InventorySummaryCsv
  & $InventoryScript @invArgs 2>&1 | ForEach-Object { Write-Host $_ }
}


  if ($DoUpload) {
  Write-Host "[INFO] [$folderName] Subida -> $uplLogDir"
  $destUrlSub = ($DestBaseSubPath -replace '\\','/').Trim('/')

  $uplArgs = @{
    SourceRoot     = $Folder
    StorageAccount = $StorageAccount
    ShareName      = $ShareName
    DestSubPath    = $destUrlSub
    Sas            = $Sas
    ServiceType    = $ServiceType
    LogDir         = $uplLogDir
    AzCopyPath     = $AzCopyPath
    Overwrite      = $Overwrite
    MaxLogSizeMB   = $MaxLogSizeMB
    UploadSummaryCsv = $UploadSummaryCsv
  }
  if ($PreservePermissions) { $uplArgs.PreservePermissions = $true }

  if ($PSBoundParameters.ContainsKey('AzConcurrency')) { $uplArgs.AzConcurrency = $AzConcurrency }
    if ($PSBoundParameters.ContainsKey('AzBufferGB'))    { $uplArgs.AzBufferGB    = $AzBufferGB }

  & $UploadScript @uplArgs 2>&1 | ForEach-Object { Write-Host $_ }
}


  [pscustomobject]@{ Folder = $Folder; Status = 'Done' }
}


# ---------------- Modo Paralelo (sin ventanas) ----------------
$throttle = [Math]::Max(1,$MaxParallel)
Info "Procesando en paralelo con Throttle=$throttle (inventario y subida en paralelo por carpeta)."

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
  DoInventory         = $DoInventory
  DoUpload            = $DoUpload
  InventorySummaryCsv = $InventorySummaryCsv
  UploadSummaryCsv  = $UploadSummaryCsv
  AzConcurrency       = $AzConcurrency      # <-- NUEVO
  AzBufferGB          = $AzBufferGB         # <-- NUEVO

}


if ($PSVersionTable.PSVersion.Major -ge 7) {
  $funcDef = ${function:Invoke-FolderWork}.Ast.Extent.Text
  $folders | ForEach-Object -Parallel {
    Invoke-Expression $using:funcDef
    Invoke-FolderWork @using:common -Folder $_.FullName
  } -ThrottleLimit $throttle
}
else {
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

# ---------------- Utilidades de parsing/diff ----------------
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
      if     ($l -match '^\s*JobID:\s*(.+)$')                    { $h['Az_JobID']=$matches[1].Trim() }
      elseif ($l -match '^\s*Estado:\s*(.+)$')                   { $h['Az_Status']=$matches[1].Trim() }
      elseif ($l -match '^\s*Total transfers:\s*(\d+)')          { $h['Az_Total']=[int]$matches[1] }
      elseif ($l -match '^\s*Completados:\s*(\d+)')              { $h['Az_Completed']=[int]$matches[1] }
      elseif ($l -match '^\s*Fallidos:\s*(\d+)')                 { $h['Az_Failed']=[int]$matches[1] }
      elseif ($l -match '^\s*Saltados:\s*(\d+)')                 { $h['Az_Skipped']=[int]$matches[1] }
      elseif ($l -match '^\s*Bytes transferidos:\s*(\d+)')       { $h['Az_Bytes']=[int64]$matches[1] }
      elseif ($l -match '^\s*Duración:\s*([0-9\.\:]+.*)$')       { $h['Az_DurationSec']=$matches[1].Trim() }  # dejamos el string tal cual
    }
  }
  return $h
}


function Get-FailedCount([string]$uploadFolder){
  $p = Join-Path $uploadFolder 'failed-transfers.csv'
  if (-not (Test-Path -LiteralPath $p)) { return 0 }
  # cuenta filas de datos (si está vacío, 0)
  $lines = Get-Content -LiteralPath $p -ErrorAction SilentlyContinue
  if (-not $lines -or $lines.Count -eq 0) { return 0 }
  # si tiene cabecera, réstala
  return [Math]::Max(0, $lines.Count - 1)
}

function Compare-SourceDest([string]$invFolder,[string]$uploadFolder){
  $srcCsv  = Join-Path $invFolder   'inventory.csv'
  $dstCsv  = Join-Path $uploadFolder 'dest-inventory.csv'
  $outMiss = Join-Path $uploadFolder 'diff_missing_in_dest.csv'
  $outExtra= Join-Path $uploadFolder 'diff_extra_in_dest.csv'

  if (-not (Test-Path -LiteralPath $srcCsv) -or -not (Test-Path -LiteralPath $dstCsv)) {
    return @{ Missing=0; Extra=0 }
  }

  $src  = Import-Csv -LiteralPath $srcCsv  -ErrorAction SilentlyContinue | Where-Object { $_.Type -eq 'File' }
  $dest = Import-Csv -LiteralPath $dstCsv  -ErrorAction SilentlyContinue | Where-Object { $_.EntityType -eq 'File' }

  # normaliza rutas
  $srcFiles  = $src  | ForEach-Object { [pscustomobject]@{ Rel = ($_.Path -replace '\\','/'); Size = $_.FileSize } }
  $destFiles = $dest | ForEach-Object { [pscustomobject]@{ Rel = ($_.Path -replace '\\','/'); Size = $_.Bytes } }

  $srcSet  = $srcFiles.Rel
  $destSet = $destFiles.Rel

  $missing = $srcFiles  | Where-Object { $_.Rel -notin $destSet }
  $extra   = $destFiles | Where-Object { $_.Rel -notin $srcSet  }

  $missing | Export-Csv -LiteralPath $outMiss -NoTypeInformation -Encoding UTF8
  $extra   | Export-Csv -LiteralPath $outExtra -NoTypeInformation -Encoding UTF8

  return @{ Missing = @($missing).Count; Extra = @($extra).Count }
}

# ---------------- Resumen final ----------------
$summaryRows = New-Object System.Collections.Generic.List[object]
foreach($dir in $folders){
  $name  = $dir.Name
  $invLog = Join-Path $InventoryLogRoot $name
  $upLog  = Join-Path $UploadLogRoot    $name

  $inv = Parse-FolderInfo $invLog
  $azp = Get-LatestUploadLog $upLog
  $az  = Parse-AzCopySummary $azp

  # NUEVO: failed count y diff (source vs dest)
  $failedCount = Get-FailedCount $upLog
  $diff = Compare-SourceDest -invFolder $invLog -uploadFolder $upLog

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
    FailedCount            = $failedCount
    Diff_MissingCount      = $diff.Missing
    Diff_ExtraCount        = $diff.Extra
    UploadLog              = $azp
  }) | Out-Null
}

$summaryCsv = Join-Path $UploadLogRoot 'summary.csv'
$summaryRows | Export-Csv -LiteralPath $summaryCsv -NoTypeInformation -Encoding UTF8

$summaryRows |
  Select-Object Folder,Inv_TotalFiles,Inv_AccessibleFiles,Inv_InaccessibleFiles,Inv_TotalBytes,
                Az_Status,Az_Total,Az_Completed,Az_Failed,Az_Skipped,Az_Bytes,Az_DurationSec,
                FailedCount,Diff_MissingCount,Diff_ExtraCount |
  Format-Table -AutoSize

Info "Resumen CSV -> $summaryCsv"
