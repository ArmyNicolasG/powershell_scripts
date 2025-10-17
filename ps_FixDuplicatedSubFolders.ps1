param(
  [Parameter(Mandatory)] [string]$ShareRoot,   # Ej: \\cuenta.file.core.windows.net\disco-x\RootPath
  [switch]$WhatIf,                             # Prueba en seco (default on si no pasas -WhatIf:$false explícito)
  [switch]$OverwriteExisting                   # Si ya existe un nombre igual en el nivel superior, sobrescribe
)

# Si no especifica, hacemos dry-run por seguridad
if (-not $PSBoundParameters.ContainsKey('WhatIf')) { $WhatIf = $true }

Write-Host "Raíz a corregir: $ShareRoot"
$outerFolders = Get-ChildItem -LiteralPath $ShareRoot -Directory -Force -ErrorAction Stop

foreach ($outer in $outerFolders) {
  $name  = $outer.Name
  $inner = Join-Path $outer.FullName $name     # \\...\RootPath\SUBCARPETA\SUBCARPETA

  if (Test-Path -LiteralPath $inner) {
    Write-Host ">> Corrigiendo '$name': mover contenido de '$inner' -> '$($outer.FullName)'"

    # Enumerar todo lo que haya dentro del duplicado
    $children = Get-ChildItem -LiteralPath $inner -Force

    foreach ($c in $children) {
      $from = $c.FullName
      $to   = Join-Path $outer.FullName $c.Name

      if (Test-Path -LiteralPath $to) {
        if ($OverwriteExisting) {
          Write-Host "   - Sobrescribir existe: $to"
          if (-not $WhatIf) {
            # Si destino es carpeta y también carpeta, esto efectivamente "fusiona" (mueve dentro)
            Move-Item -LiteralPath $from -Destination $to -Force
          }
        } else {
          Write-Host "   - Ya existe en destino, OMITO: $to  (usa -OverwriteExisting para forzar)"
          continue
        }
      } else {
        Write-Host "   - Mover: $from  ->  $to"
        if (-not $WhatIf) {
          Move-Item -LiteralPath $from -Destination $to
        }
      }
    }

    if (-not $WhatIf) {
      $remains = Get-ChildItem -LiteralPath $inner -Force
      if ($remains.Count -eq 0) {
        Write-Host "   - Eliminando carpeta duplicada vacía: $inner"
        Remove-Item -LiteralPath $inner -Force
      } else {
        Write-Host "   - Ojo: '$inner' aún tiene elementos (nombres en conflicto). Revísalos."
      }
    }
  } else {
    Write-Host ">> '$name' OK (no tiene duplicado interno '$name')."
  }
}

Write-Host ""
Write-Host "Resumen:"
Write-Host "  * Si ejecutaste con -WhatIf (por defecto), NO se movió nada. Revisa y vuelve a ejecutar sin -WhatIf."
Write-Host "  * Para sobrescribir nombres ya existentes en el nivel superior, añade -OverwriteExisting."
