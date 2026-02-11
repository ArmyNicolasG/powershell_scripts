param(
  [Parameter(Mandatory)] [string] $SourceRoot,
  [Parameter(Mandatory)] [string] $StorageAccount,
  [Parameter(Mandatory)] [string] $ShareName,
  [Parameter(Mandatory)] [string] $DestBaseSubPath,
  [Parameter(Mandatory)] [string] $Sas,

  [string] $AzCopyPath = "azcopy",
  [string] $LogFile = ".\azcopy-sync.log",

  [switch] $PreservePermissions,

  [int] $AzConcurrency = 16,
  [int] $AzBufferGB = 1
)

function Normalize-Sas([string]$s) {
  $t = $s.Trim()
  if (-not $t.StartsWith("?")) { $t = "?" + $t }
  $t
}

function Normalize-SubPath([string]$p) {
  $t = ($p -replace "\\","/").Trim()
  $t = $t.Trim("/")
  $t
}

$src  = (Resolve-Path $SourceRoot).Path
$leaf = Split-Path $src -Leaf

$base = Normalize-SubPath $DestBaseSubPath

# replica el comportamiento de azcopy copy
# destino final siempre termina en el nombre de la carpeta origen
# pero sin duplicarlo si ya está incluido
$destSub = $base
if ([string]::IsNullOrWhiteSpace($destSub)) {
  $destSub = $leaf
} else {
  $last = $destSub.Split("/")[-1]
  if ($last -ne $leaf) { $destSub = ($destSub + "/" + $leaf).Trim("/") }
}

$sasT = Normalize-Sas $Sas
$destUrl = "https://$StorageAccount.file.core.windows.net/$ShareName/$destSub$sasT"

$prevConc = $env:AZCOPY_CONCURRENCY_VALUE
$prevBuf  = $env:AZCOPY_BUFFER_GB
try {
  $env:AZCOPY_CONCURRENCY_VALUE = [string]$AzConcurrency
  $env:AZCOPY_BUFFER_GB         = [string]$AzBufferGB

  $args = @(
    "sync", $src, $destUrl,
    "--recursive=true",
    "--delete-destination=false",
    "--output-level=essential",
    "--log-level=ERROR"
  )

  if ($PreservePermissions) {
    $args += @("--preserve-smb-permissions=true","--preserve-smb-info=true")
  }

  & $AzCopyPath @args 2>&1 | Tee-Object -FilePath $LogFile | Out-Null
}
finally {
  $env:AZCOPY_CONCURRENCY_VALUE = $prevConc
  $env:AZCOPY_BUFFER_GB         = $prevBuf
}