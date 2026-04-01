# README operativo y tecnico

## Contexto
Esta carpeta contiene scripts PowerShell para migracion y mantenimiento de archivos hacia Azure Storage usando AzCopy. El alcance de este README cubre solo los scripts `.ps1` ubicados en la raiz de `scripts/`.

Quedan fuera del alcance principal:
- scripts dentro de `upload/`
- scripts dentro de `recon/`
- scripts dentro de `modify/`

Objetivos cubiertos por estos scripts:
- inventariar estructuras de archivos y carpetas
- subir contenido a Azure File Share o Blob
- sincronizar cambios incrementales
- orquestar migraciones por subcarpetas
- corregir un problema puntual de subcarpetas duplicadas

## Mapa rapido de scripts

| Script | Tipo | Proposito | Cuando usarlo | Resultado principal |
| --- | --- | --- | --- | --- |
| `ps_GetFilesAndFoldersStructure_v2.ps1` | Inventario | Auditar estructura, accesos y tamano | Antes de migrar o para conciliacion | `inventory.csv`, `inventory-failed-or-denied.csv`, `folder-info.txt` |
| `ps_UploadToFileShareFromCsv_v2.ps1` | Subida inicial | Ejecutar `azcopy copy` a Azure Files o Blob | Cuando necesitas cargar una carpeta puntual | Logs de subida y `resumen-subidas.csv` |
| `ps_RunInventoryAndUploadFromRoot.ps1` | Orquestador | Ejecutar inventario y/o subida por cada subcarpeta inmediata | Cuando migras una raiz con muchas carpetas | Logs por carpeta y `summary.csv` consolidado |
| `ps_SyncAzureFiles.ps1` | Sync incremental | Ejecutar `azcopy sync` sin borrar destino | Cuando ya hiciste carga inicial y quieres mantener cambios | Logs de sync por corrida o por carpeta |
| `ps_SyncAzureFiles_ThirdLevel.ps1` | Sync incremental profundo | Ejecutar `azcopy sync` por cada carpeta de tercer nivel | Cuando unas pocas carpetas de segundo nivel concentran mucho peso y necesitas aislar mas el trabajo | Logs de sync por cada carpeta de tercer nivel |
| `ps_CopyAzureFiles_ThirdLevel.ps1` | Copy profundo | Ejecutar `azcopy copy` por cada carpeta de tercer nivel | Cuando necesitas reintentar una carga grande por ramas profundas y `sync` no ha sido suficiente | Logs de copy por cada carpeta de tercer nivel |
| `Run_All_Syncs_Fonvalmed.ps1` | Runner especifico | Lanzar multiples syncs preconfigurados | Solo para el caso Fonvalmed o como plantilla de automatizacion | Una corrida completa con varios destinos |
| `ps_FixDuplicatedSubFolders.ps1` | Correctivo | Reparar estructuras `Carpeta\Carpeta` | Despues de detectar duplicacion de subcarpetas | Reubicacion del contenido al nivel correcto |

## Prerrequisitos
- PowerShell 7 recomendado. Algunos flujos degradan a alternativas si no esta disponible, pero el comportamiento esperado esta pensado para `pwsh`.
- Windows o un entorno con acceso a rutas UNC si el origen esta en shares de red.
- `azcopy.exe` disponible en PATH o accesible por ruta explicita.
- Token SAS vigente para el destino en Azure.
- Permisos de lectura sobre el origen y permisos de escritura en el destino.
- Memoria suficiente si usas ejecucion paralela o apertura de multiples ventanas.

## Dependencia: `azcopy.exe`
`azcopy.exe` es una dependencia binaria, no un script funcional del flujo. Los scripts raiz lo usan de dos formas:
- mediante `AzCopyPath` si se pasa ruta explicita
- mediante `azcopy` si el binario esta en PATH

## Flujo sugerido de uso
Orden recomendado:

1. Ejecutar `ps_GetFilesAndFoldersStructure_v2.ps1` para conocer volumen, accesos denegados, nombres problematicos y tamano del arbol.
2. Si vas a procesar una raiz con muchas subcarpetas, usar `ps_RunInventoryAndUploadFromRoot.ps1` como script principal de operacion.
3. Si solo necesitas cargar una carpeta puntual, usar `ps_UploadToFileShareFromCsv_v2.ps1`.
4. Despues de la carga inicial, usar `ps_SyncAzureFiles.ps1` para sincronizaciones incrementales.
5. Si necesitas abrir mas granularidad porque el peso esta concentrado en pocas ramas profundas, usar `ps_SyncAzureFiles_ThirdLevel.ps1`.
6. Si necesitas reintentar una carga profunda por ramas con `copy`, usar `ps_CopyAzureFiles_ThirdLevel.ps1`.
7. Si el proyecto corresponde al lote Fonvalmed ya parametrizado, usar `Run_All_Syncs_Fonvalmed.ps1`.
8. Si despues de migrar aparece una estructura duplicada tipo `Carpeta\Carpeta`, revisar primero con `ps_FixDuplicatedSubFolders.ps1` en modo `-WhatIf`.

Notas:
- `ps_RunInventoryAndUploadFromRoot.ps1` puede ejecutar solo inventario, solo subida o ambos.
- `ps_SyncAzureFiles.ps1` no reemplaza una carga inicial completa cuando necesitas trazabilidad detallada de migracion; su objetivo es sincronizacion incremental.

## Guia rapida por escenarios

### Escenario 1: auditar antes de migrar
Usa `ps_GetFilesAndFoldersStructure_v2.ps1`.

### Escenario 2: migrar una carpeta puntual
Usa `ps_UploadToFileShareFromCsv_v2.ps1`.

### Escenario 3: migrar una raiz con muchas subcarpetas
Usa `ps_RunInventoryAndUploadFromRoot.ps1`.

### Escenario 4: mantener sincronizado despues de la carga inicial
Usa `ps_SyncAzureFiles.ps1`.

### Escenario 5: mantener sincronizado por carpetas de tercer nivel
Usa `ps_SyncAzureFiles_ThirdLevel.ps1`.

### Escenario 6: cargar por carpetas de tercer nivel usando copy
Usa `ps_CopyAzureFiles_ThirdLevel.ps1`.

### Escenario 7: ejecutar un lote ya definido para Fonvalmed
Usa `Run_All_Syncs_Fonvalmed.ps1`.

### Escenario 8: corregir duplicacion de carpetas en destino
Usa `ps_FixDuplicatedSubFolders.ps1`.

## Interfaces publicas y convenciones
En este repositorio, la interfaz publica de cada script son sus parametros de entrada, sus archivos de salida y su comportamiento visible en consola/logs.

Puntos importantes:
- algunos parametros no estan marcados como `Mandatory` pero son requeridos en la practica segun el modo de uso
- varios scripts generan CSV maestros centralizados y snapshots deduplicados
- los filtros `DoOnly` y `Exclude` usan listas separadas por `;`
- en general, los paths aceptan rutas locales y UNC

---

## `ps_GetFilesAndFoldersStructure_v2.ps1`

### Objetivo
Generar un inventario de archivos y carpetas desde una raiz local o UNC, validando accesibilidad real y dejando evidencia en CSV, TXT y log.

### Cuando usarlo
- antes de una migracion
- para detectar archivos o carpetas inaccesibles
- para medir tamano total de una raiz
- para producir un resumen centralizado de conciliacion

### Que hace internamente
- recorre archivos y carpetas desde `Path`
- intenta listar carpetas para verificar acceso real
- intenta abrir archivos para verificar lectura real
- escribe un inventario completo y otro exclusivo para accesos fallidos o denegados
- opcionalmente sanea nombres invalidos con `-SanitizeNames`
- opcionalmente calcula bytes totales del arbol con `-ComputeRootSize`
- actualiza un CSV maestro de inventarios si se configura `-InventorySummaryCsv`

### Parametros

| Parametro | Tipo | Default | Uso |
| --- | --- | --- | --- |
| `Path` | `string` | n/a | Raiz a inventariar. Obligatorio. |
| `LogDir` | `string` | n/a | Carpeta local donde se escriben CSV, log y TXT. Obligatorio. |
| `Depth` | `int` | `-1` | Profundidad maxima. `-1` significa sin limite. |
| `IncludeFiles` | `switch` | `true` | Incluye archivos en el inventario. |
| `IncludeFolders` | `switch` | `true` | Incluye carpetas en el inventario. |
| `SkipReparsePoints` | `switch` | `true` | Evita reparse points. |
| `Utc` | `switch` | `false` | Emite fechas en UTC. |
| `ComputeRootSize` | `switch` | `false` | Calcula bytes totales del arbol raiz. |
| `SanitizeNames` | `switch` | `false` | Renombra solo nombres invalidos. |
| `MaxNameLength` | `int` | `255` | Longitud maxima permitida para el nombre, no para la ruta completa. |
| `ReplacementChar` | `string` | `"_"` | Caracter de reemplazo al sanear nombres. |
| `InventorySummaryCsv` | `string` | vacio | Ruta del CSV maestro de conciliaciones. |

### Parametros obligatorios
- `Path`
- `LogDir`

### Parametros opcionales
- `Depth`
- `IncludeFiles`
- `IncludeFolders`
- `SkipReparsePoints`
- `Utc`
- `ComputeRootSize`
- `SanitizeNames`
- `MaxNameLength`
- `ReplacementChar`
- `InventorySummaryCsv`

### Archivos y logs que genera
- `inventory.csv`: inventario principal
- `inventory-failed-or-denied.csv`: solo rutas sin acceso o con fallo de enumeracion/lectura
- `inventory.log`: log detallado
- `folder-info.txt`: resumen de contadores y, si aplica, `TotalBytes`
- `resumen-conciliaciones.csv`: CSV maestro centralizado
- `resumen-conciliaciones_dedup.csv`: snapshot deduplicado del maestro

### Ejemplo de uso
```powershell
.\ps_GetFilesAndFoldersStructure_v2.ps1 `
  -Path "\\SERVIDOR\Share\Area" `
  -LogDir "C:\Logs\inventario\area" `
  -ComputeRootSize `
  -InventorySummaryCsv "C:\Logs\inventario\resumen-conciliaciones.csv"
```

### Riesgos y advertencias
- `-SanitizeNames` modifica nombres en origen. Usalo solo si esa correccion es intencional.
- `MaxNameLength` aplica al nombre del item, no a la ruta completa.
- El script verifica acceso real, por lo que puede exponer diferencias entre permisos aparentes y acceso efectivo.

---

## `ps_UploadToFileShareFromCsv_v2.ps1`

### Objetivo
Subir una carpeta local o UNC a Azure File Share o Azure Blob usando `azcopy copy`, con logs rotativos y resumen legible de la corrida.

### Cuando usarlo
- para una carga inicial de una carpeta puntual
- cuando necesitas logs claros de AzCopy
- cuando quieres centralizar resumentes de subidas

### Que hace internamente
- construye la URL destino desde `StorageAccount`, `ShareName`, `DestSubPath` y `Sas`
- ejecuta `azcopy copy --recursive=true`
- soporta `FileShare` y `Blob`
- opcionalmente preserva metadatos/permisos SMB en File Share
- parsea la salida de AzCopy para construir un resumen legible
- actualiza `resumen-subidas.csv` y su snapshot deduplicado

### Parametros

| Parametro | Tipo | Default | Uso |
| --- | --- | --- | --- |
| `SourceRoot` | `string` | n/a | Carpeta origen. Obligatorio. |
| `StorageAccount` | `string` | n/a | Nombre de la storage account. Obligatorio. |
| `ShareName` | `string` | n/a | File share o contenedor. Obligatorio. |
| `DestSubPath` | `string` | n/a | Ruta interna destino dentro del share o contenedor. Puede ser vacia para usar la raiz del share o contenedor. |
| `Sas` | `string` | n/a | Token SAS. Obligatorio. |
| `ServiceType` | `string` | `FileShare` | `FileShare` o `Blob`. |
| `Overwrite` | `string` | `ifSourceNewer` | Politica de sobreescritura. |
| `IncludePaths` | `string[]` | vacio | Lista de rutas relativas a incluir. |
| `AzCopyPath` | `string` | `azcopy` | Ruta del ejecutable AzCopy o nombre en PATH. |
| `PreservePermissions` | `switch` | `false` | Preserva permisos e info SMB en File Share. |
| `LogDir` | `string` | n/a | Carpeta de logs. Obligatorio. |
| `MaxLogSizeMB` | `int` | `8` | Tamano maximo por archivo de log wrapper. |
| `GenerateStatusReports` | `switch` | `false` | Genera `summary.txt`. |
| `NativeLogLevel` | `string` | `ERROR` | Nivel de log nativo de AzCopy. |
| `ConsoleOutputLevel` | `string` | `essential` | Nivel de salida en consola. |
| `UploadSummaryCsv` | `string` | vacio | Ruta del CSV maestro de subidas. |
| `AzConcurrency` | `Nullable[int]` | vacio | Override de `AZCOPY_CONCURRENCY_VALUE`. |
| `AzBufferGB` | `Nullable[int]` | vacio | Override de `AZCOPY_BUFFER_GB`. |

### Parametros obligatorios
- `SourceRoot`
- `StorageAccount`
- `ShareName`
- `DestSubPath`
- `Sas`
- `LogDir`

### Parametros opcionales
- `ServiceType`
- `Overwrite`
- `IncludePaths`
- `AzCopyPath`
- `PreservePermissions`
- `MaxLogSizeMB`
- `GenerateStatusReports`
- `NativeLogLevel`
- `ConsoleOutputLevel`
- `UploadSummaryCsv`
- `AzConcurrency`
- `AzBufferGB`

### Archivos y logs que genera
- `upload-logs-*.txt`: logs wrapper con rotacion
- `azcopy\`: carpeta de logs nativos de AzCopy
- `summary.txt`: resumen compatible, solo si usas `-GenerateStatusReports`
- `resumen-subidas.csv`: CSV maestro append-only
- `resumen-subidas_dedup.csv`: snapshot deduplicado del maestro

### Ejemplo de uso
```powershell
.\ps_UploadToFileShareFromCsv_v2.ps1 `
  -SourceRoot "\\SERVIDOR\Share\Area" `
  -StorageAccount "mystorageaccount" `
  -ShareName "documentos" `
  -DestSubPath "area/finanzas" `
  -Sas "?sv=..." `
  -ServiceType FileShare `
  -PreservePermissions `
  -AzCopyPath "C:\Tools\azcopy.exe" `
  -LogDir "C:\Logs\upload\area" `
  -UploadSummaryCsv "C:\Logs\upload\resumen-subidas.csv"
```

### Riesgos y advertencias
- `PreservePermissions` solo aplica a `FileShare`.
- `IncludePaths` limita la copia a rutas especificas relativas al origen.
- Si cambias `NativeLogLevel` a `INFO`, el volumen de logs puede crecer bastante.

---

## `ps_RunInventoryAndUploadFromRoot.ps1`

### Objetivo
Orquestar inventario y/o subida por cada subcarpeta inmediata de una raiz. Es el script principal para operacion masiva.

### Cuando usarlo
- cuando necesitas procesar una raiz con muchas subcarpetas
- cuando quieres ejecutar inventario y subida en un mismo flujo
- cuando necesitas filtros por nombre de carpeta
- cuando necesitas un resumen consolidado final

### Que hace internamente
- resuelve `RootPath` y enumera subcarpetas inmediatas
- opcionalmente copia archivos sueltos de la raiz a `Archivos sueltos pre-migracion`
- aplica filtros exactos con `DoOnly` y `Exclude`
- ejecuta inventario, subida o ambos por carpeta
- soporta dos modos:
  - paralelo sin abrir ventanas
  - apertura de ventanas nuevas con control de RAM y limite de ventanas
- centraliza CSV de inventario y de subida
- genera `summary.csv` final con metricas de inventario y AzCopy
- intenta comparar origen y destino si existe `dest-inventory.csv` en la carpeta de logs de subida

### Parametros

| Parametro | Tipo | Default | Uso |
| --- | --- | --- | --- |
| `RootPath` | `string` | n/a | Raiz que contiene las subcarpetas a procesar. Obligatorio. |
| `InventoryScript` | `string` | vacio | Script de inventario a invocar. Requerido si usas `-DoInventory`. |
| `UploadScript` | `string` | vacio | Script de subida a invocar. Requerido si usas `-DoUpload`. |
| `InventoryLogRoot` | `string` | vacio | Carpeta base de logs de inventario. Requerido si usas `-DoInventory`. |
| `UploadLogRoot` | `string` | vacio | Carpeta base de logs de subida. Requerido si usas `-DoUpload`. |
| `StorageAccount` | `string` | vacio | Cuenta de almacenamiento. Requerido si usas `-DoUpload`. |
| `ShareName` | `string` | vacio | Share o contenedor destino. Requerido si usas `-DoUpload`. |
| `DestBaseSubPath` | `string` | vacio | Ruta base destino. Requerido si usas `-DoUpload`. |
| `Sas` | `string` | vacio | Token SAS. Requerido si usas `-DoUpload`. |
| `ServiceType` | `string` | `FileShare` | Tipo de destino del upload. |
| `Overwrite` | `string` | `ifSourceNewer` | Politica de sobreescritura para el upload. |
| `PreservePermissions` | `switch` | `false` | Preserva permisos SMB en el upload. |
| `AzCopyPath` | `string` | `azcopy` | Ejecutable de AzCopy. |
| `MaxLogSizeMB` | `int` | `64` | Tamano maximo de logs del uploader. |
| `MaxParallel` | `int` | `2` | Throttle en modo paralelo. |
| `OpenNewWindows` | `switch` | `false` | Usa ventanas separadas por carpeta. |
| `WindowLaunchDelaySeconds` | `int` | `15` | Espera entre lanzamientos de ventana. |
| `IncludeLooseFilesAsFolder` | `switch` | `true` | Copia archivos sueltos de la raiz a una carpeta auxiliar. |
| `ComputeRootSize` | `switch` | `false` | Pasa `-ComputeRootSize` al inventario. |
| `DoInventory` | `switch` | `false` | Ejecuta inventario. |
| `DoUpload` | `switch` | `false` | Ejecuta subida. |
| `InventorySummaryCsv` | `string` | `<InventoryLogRoot>\resumen-conciliaciones.csv` | CSV maestro de inventario. |
| `UploadSummaryCsv` | `string` | `<UploadLogRoot>\resumen-subidas.csv` | CSV maestro de subida. |
| `DoOnly` | `string` | vacio | Lista `;` de carpetas a incluir exclusivamente. |
| `Exclude` | `string` | vacio | Lista `;` de carpetas a excluir. |
| `AzConcurrency` | `Nullable[int]` | vacio | Override de concurrencia AzCopy. |
| `AzBufferGB` | `Nullable[int]` | vacio | Override de buffer AzCopy. |
| `RamSafeLimit` | `int` | `65` | Limite de RAM para modo ventanas. `0` desactiva el control. |
| `MaxOpenWindows` | `int` | `4` | Maximo de ventanas activas en modo ventanas. `0` es sin limite. |
| `LaunchPollSeconds` | `int` | `10` | Intervalo de sondeo para relanzamiento. |
| `HoldOnError` | `switch` | `false` | Existe como parametro, pero en la version actual no tiene efecto observable. |

### Parametros obligatorios
- `RootPath`

### Parametros condicionales
Si usas `-DoInventory`:
- `InventoryScript`
- `InventoryLogRoot`

Si usas `-DoUpload`:
- `UploadScript`
- `UploadLogRoot`
- `StorageAccount`
- `ShareName`
- `DestBaseSubPath`
- `Sas`

Si usas modo ventanas:
- `OpenNewWindows`
- opcionalmente `MaxOpenWindows`, `RamSafeLimit`, `LaunchPollSeconds`, `WindowLaunchDelaySeconds`

Si usas upload:
- aplican `AzCopyPath`, `Overwrite`, `PreservePermissions`, `AzConcurrency`, `AzBufferGB`

### Archivos y logs que genera
- una carpeta por subcarpeta procesada dentro de `InventoryLogRoot`
- una carpeta por subcarpeta procesada dentro de `UploadLogRoot`
- `resumen-conciliaciones.csv`
- `resumen-subidas.csv`
- `summary.csv` final dentro de `UploadLogRoot`
- `diff_missing_in_dest.csv` y `diff_extra_in_dest.csv` cuando existe `dest-inventory.csv`

### Ejemplo de uso
```powershell
.\ps_RunInventoryAndUploadFromRoot.ps1 `
  -RootPath "\\SERVIDOR\Share\RaizMigracion" `
  -InventoryScript ".\ps_GetFilesAndFoldersStructure_v2.ps1" `
  -UploadScript ".\ps_UploadToFileShareFromCsv_v2.ps1" `
  -InventoryLogRoot "C:\Logs\inventario" `
  -UploadLogRoot "C:\Logs\upload" `
  -StorageAccount "mystorageaccount" `
  -ShareName "documentos" `
  -DestBaseSubPath "raiz-migracion" `
  -Sas "?sv=..." `
  -DoInventory `
  -DoUpload `
  -ComputeRootSize `
  -MaxParallel 2 `
  -DoOnly "Finanzas;Legal"
```

### Riesgos y advertencias
- Si no pasas `-DoInventory` ni `-DoUpload`, el script no hace nada y termina con advertencia.
- `IncludeLooseFilesAsFolder` copia archivos sueltos de la raiz a una carpeta auxiliar llamada `Archivos sueltos pre-migracion`.
- El script espera que `InventoryScript` y `UploadScript` apunten a scripts existentes y compatibles.
- La comparacion origen/destino solo funciona si existe `dest-inventory.csv` en el directorio de logs de subida; este archivo no lo genera el propio flujo raiz.
- `HoldOnError` esta declarado pero no esta conectado a la logica actual.

---

## `ps_SyncAzureFiles.ps1`

### Objetivo
Sincronizar cambios incrementales con `azcopy sync` desde una carpeta origen hacia un Azure File Share.

### Cuando usarlo
- despues de una carga inicial
- cuando necesitas repetir sincronizaciones de cambios
- cuando quieres dividir la sincronizacion por subcarpetas

### Que hace internamente
- normaliza SAS y subpaths
- crea una carpeta de logs con timestamp a partir de `LogFile`
- ejecuta `azcopy sync`
- usa `--delete-destination=false`, asi que no elimina lo que ya existe en destino
- puede ejecutarse sobre toda la raiz o lanzar una ventana por cada subcarpeta inmediata
- en modo ventanas, puede dejar la ventana abierta si falla y usas `-HoldOnError`

### Parametros

| Parametro | Tipo | Default | Uso |
| --- | --- | --- | --- |
| `SourceRoot` | `string` | n/a | Carpeta origen. Obligatorio. |
| `StorageAccount` | `string` | n/a | Storage account. Obligatorio. |
| `ShareName` | `string` | n/a | Share destino. Obligatorio. |
| `DestBaseSubPath` | `string` | n/a | Ruta base destino. Obligatorio. |
| `Sas` | `string` | n/a | Token SAS. Obligatorio. |
| `AzCopyPath` | `string` | `azcopy` | Ruta del ejecutable o nombre en PATH. |
| `LogFile` | `string` | `.\azcopy-sync.log` | Nombre base del log. El script crea una carpeta con timestamp. |
| `PreservePermissions` | `switch` | `false` | Preserva permisos e info SMB. |
| `AzConcurrency` | `int` | `16` | Concurrencia de AzCopy. |
| `AzBufferGB` | `int` | `1` | Buffer de AzCopy en GB. |
| `OpenNewWindows` | `switch` | `false` | Abre una ventana por subcarpeta inmediata. |
| `MaxOpenWindows` | `int` | `3` | Limite de ventanas simultaneas. |
| `LaunchPollSeconds` | `int` | `10` | Intervalo de sondeo. |
| `WindowLaunchDelaySeconds` | `int` | `15` | Pausa entre ventanas. |
| `RamSafeLimit` | `int` | `65` | Limite de RAM. `0` lo desactiva. |
| `DoOnly` | `string` | vacio | Lista `;` de carpetas a incluir. |
| `Exclude` | `string` | vacio | Lista `;` de carpetas a excluir. |
| `HoldOnError` | `switch` | `false` | En modo ventanas, mantiene la consola abierta si falla. |

### Parametros obligatorios
- `SourceRoot`
- `StorageAccount`
- `ShareName`
- `DestBaseSubPath`
- `Sas`

### Parametros opcionales
- `AzCopyPath`
- `LogFile`
- `PreservePermissions`
- `NativeLogLevel`
- `AzCopyErrorLogSuffix`
- `AzConcurrency`
- `AzBufferGB`
- `OpenNewWindows`
- `MaxOpenWindows`
- `LaunchPollSeconds`
- `WindowLaunchDelaySeconds`
- `RamSafeLimit`
- `DoOnly`
- `Exclude`
- `FallbackToSecondLevel`
- `HoldOnError`

### Archivos y logs que genera
- una carpeta de corrida con timestamp derivada de `LogFile`
- un log principal si ejecutas sync directo
- logs `sync-<carpeta>.log` por cada subcarpeta cuando usas `-OpenNewWindows`

### Ejemplo de uso
```powershell
.\ps_SyncAzureFiles.ps1 `
  -SourceRoot "\\SERVIDOR\Share\Area" `
  -StorageAccount "mystorageaccount" `
  -ShareName "documentos" `
  -DestBaseSubPath "area" `
  -Sas "?sv=..." `
  -AzCopyPath "C:\Tools\azcopy.exe" `
  -LogFile "C:\Logs\sync\sync.log" `
  -OpenNewWindows `
  -MaxOpenWindows 3 `
  -DoOnly "Finanzas;Legal" `
  -HoldOnError
```

### Riesgos y advertencias
- Este script usa `sync`, no `copy`.
- `--delete-destination=false` evita borrados en destino; si necesitas otro comportamiento, este script no lo implementa.
- En modo ventanas, la unidad de trabajo es cada subcarpeta inmediata de `SourceRoot`.

---

## `ps_SyncAzureFiles_ThirdLevel.ps1`

### Objetivo
Sincronizar cambios incrementales con `azcopy sync` lanzando una unidad de trabajo por cada carpeta de tercer nivel bajo una raiz.

### Cuando usarlo
- cuando unas pocas carpetas de segundo nivel contienen la mayor parte del volumen
- cuando necesitas mas aislamiento que el script de sync normal
- cuando quieres mantener la jerarquia `SegundoNivel/TercerNivel` en el destino

### Que hace internamente
- enumera carpetas de segundo nivel desde `SourceRoot`
- aplica `DoOnly` y `Exclude` por nombre exacto de segundo nivel
- dentro de cada carpeta de segundo nivel, enumera sus carpetas de tercer nivel
- lanza una ventana o proceso por cada carpeta de tercer nivel
- construye el destino conservando `SegundoNivel/TercerNivel`
- usa `--delete-destination=false`, asi que no elimina contenido existente en destino

### Parametros
Usa la misma interfaz publica que `ps_SyncAzureFiles.ps1`:
- `SourceRoot`
- `StorageAccount`
- `ShareName`
- `DestBaseSubPath`
- `Sas`
- `AzCopyPath`
- `LogFile`
- `PreservePermissions`
- `AzConcurrency`
- `AzBufferGB`
- `OpenNewWindows`
- `MaxOpenWindows`
- `LaunchPollSeconds`
- `WindowLaunchDelaySeconds`
- `RamSafeLimit`
- `DoOnly`
- `Exclude`
- `FallbackToSecondLevel`
- `HoldOnError`

### Comportamiento clave
- si `DestBaseSubPath = ""`, una carpeta local `Segundo\Tercero` termina en `share/Segundo/Tercero`
- si `DestBaseSubPath = "base"`, esa carpeta termina en `share/base/Segundo/Tercero`
- si una carpeta de segundo nivel no tiene carpetas de tercer nivel, se omite y se reporta; con `-FallbackToSecondLevel` se sincroniza ese segundo nivel como unidad de trabajo
- si detecta archivos sueltos en el segundo nivel, crea una unidad adicional hacia `SegundoNivel/archivos_sueltos`
- los nombres se sanean solo para el nombre del log, no para origen ni destino
- crea una carpeta de logs nativos de AzCopy por unidad y anexa su contenido a un archivo `*-errors.log`

### Archivos y logs que genera
- una carpeta de corrida con timestamp derivada de `LogFile`
- un log maestro de corrida en la ruta derivada de `LogFile`
- un log por unidad de trabajo, con formato `sync-<segundo>--<tercero>.log`
- un log de errores nativos de AzCopy por unidad, con formato `sync-<...>-errors.log`

### Ejemplo de uso
```powershell
.\ps_SyncAzureFiles_ThirdLevel.ps1 `
  -SourceRoot "\\10.1.1.32\14. Fotos e inventarios Servicios Marinos" `
  -StorageAccount "ofimaticacontent" `
  -ShareName "servma" `
  -DestBaseSubPath "" `
  -Sas "?sv=..." `
  -AzCopyPath "C:\source\scripts\azcopy.exe" `
  -OpenNewWindows `
  -MaxOpenWindows 2 `
  -FallbackToSecondLevel `
  -LogFile "C:\source\upload\ServiciosMarinos-thirdlevel\sync.log"
```

### Riesgos y advertencias
- este script no filtra por tercer nivel en esta primera version
- si lo ejecutas sin `-OpenNewWindows`, hace un sync directo de toda la raiz
- si no usas `-FallbackToSecondLevel`, las carpetas de segundo nivel sin hijos se omiten
- al abrir muchas ventanas, conviene limitar `MaxOpenWindows` y `RamSafeLimit`

---

## `ps_CopyAzureFiles_ThirdLevel.ps1`

### Objetivo
Ejecutar una carga por ramas profundas usando `azcopy copy`, lanzando una unidad de trabajo por cada carpeta de tercer nivel bajo una raiz.

### Cuando usarlo
- cuando necesitas reintentar una carga grande y `sync` no ha subido suficiente informacion
- cuando quieres aislar el trabajo por ramas `SegundoNivel/TercerNivel`
- cuando prefieres `copy` para una nueva pasada de carga por profundidad

### Que hace internamente
- enumera carpetas de segundo nivel desde `SourceRoot`
- aplica `DoOnly` y `Exclude` por nombre exacto de segundo nivel
- dentro de cada carpeta de segundo nivel, enumera sus carpetas de tercer nivel
- si detecta archivos sueltos en el segundo nivel, crea una unidad adicional hacia `SegundoNivel/archivos_sueltos`
- lanza una ventana o proceso por cada carpeta de tercer nivel
- construye el destino conservando `SegundoNivel/TercerNivel`
- opcionalmente hace fallback al segundo nivel si una carpeta no tiene hijos y usas `-FallbackToSecondLevel`
- usa `azcopy copy --recursive=true`

### Parametros
Usa casi la misma interfaz publica que `ps_SyncAzureFiles_ThirdLevel.ps1`, con un parametro adicional propio de `copy`:
- `SourceRoot`
- `StorageAccount`
- `ShareName`
- `DestBaseSubPath`
- `Sas`
- `AzCopyPath`
- `LogFile`
- `PreservePermissions`
- `NativeLogLevel`
- `AzCopyErrorLogSuffix`
- `AzConcurrency`
- `AzBufferGB`
- `Overwrite`
- `OpenNewWindows`
- `MaxOpenWindows`
- `LaunchPollSeconds`
- `WindowLaunchDelaySeconds`
- `RamSafeLimit`
- `DoOnly`
- `Exclude`
- `FallbackToSecondLevel`
- `HoldOnError`

### Comportamiento clave
- si `DestBaseSubPath = ""`, una carpeta local `Segundo\Tercero` termina en `share/Segundo/Tercero`
- si `DestBaseSubPath = "base"`, esa carpeta termina en `share/base/Segundo/Tercero`
- si una carpeta de segundo nivel no tiene carpetas de tercer nivel, se omite y se reporta; con `-FallbackToSecondLevel` se copia ese segundo nivel como unidad de trabajo
- si detecta archivos sueltos en el segundo nivel, crea una unidad adicional hacia `SegundoNivel/archivos_sueltos`
- escribe un log maestro de corrida y un log por unidad de trabajo
- crea una carpeta de logs nativos de AzCopy por unidad y anexa su contenido a un archivo `*-errors.log`
- `Overwrite` controla la politica de sobreescritura de AzCopy; default `ifSourceNewer`
- el script usa `ruta\*` como origen real de `azcopy copy` para copiar solo el contenido de la carpeta y evitar duplicados tipo `Carpeta\Carpeta`

### Archivos y logs que genera
- una carpeta de corrida con timestamp derivada de `LogFile`
- un log maestro de corrida en la ruta derivada de `LogFile`
- un log por unidad de trabajo, con formato `copy-<segundo>--<tercero>.log`
- un log de errores nativos de AzCopy por unidad, con formato `copy-<...>-errors.log`

### Ejemplo de uso
```powershell
.\ps_CopyAzureFiles_ThirdLevel.ps1 `
  -SourceRoot "\\10.1.1.32\14. Fotos e inventarios Servicios Marinos" `
  -StorageAccount "ofimaticacontent" `
  -ShareName "servma" `
  -DestBaseSubPath "" `
  -Sas "?sv=..." `
  -AzCopyPath "C:\source\scripts\azcopy.exe" `
  -Overwrite ifSourceNewer `
  -OpenNewWindows `
  -MaxOpenWindows 2 `
  -FallbackToSecondLevel `
  -LogFile "C:\source\upload\ServiciosMarinos-copy-thirdlevel\copy.log"
```

### Riesgos y advertencias
- este script usa `copy`, no `sync`
- puede reprocesar mas informacion que el flujo con `sync`
- si no usas `-FallbackToSecondLevel`, las carpetas de segundo nivel sin hijos se omiten
- al abrir muchas ventanas, conviene limitar `MaxOpenWindows` y `RamSafeLimit`

---

## `Run_All_Syncs_Fonvalmed.ps1`

### Objetivo
Ejecutar multiples corridas de `ps_SyncAzureFiles.ps1` para un conjunto ya preconfigurado de rutas y destinos del proyecto Fonvalmed.

### Cuando usarlo
- cuando estas operando exactamente el lote Fonvalmed definido en el script
- cuando quieres usarlo como base para construir otro runner similar

### Que hace internamente
- prepara una carpeta de logs con timestamp bajo `LogRoot`
- pide o reutiliza los SAS de NAS1 y NAS2
- resuelve cual SAS usar segun el nombre de la storage account
- ejecuta varias llamadas a `ps_SyncAzureFiles.ps1` con rutas y destinos fijos
- mezcla filtros `DoOnly` y `Exclude` segun cada bloque del lote

### Parametros

| Parametro | Tipo | Default | Uso |
| --- | --- | --- | --- |
| `SyncScriptPath` | `string` | `C:\Source\scripts\ps_SyncAzureFiles.ps1` | Ruta al script de sync. |
| `AzCopyPath` | `string` | `C:\Source\scripts\azcopy.exe` | Ruta al binario AzCopy. |
| `SasNas1` | `string` | vacio | SAS para cuentas NAS1. Si no se pasa, se solicita por consola. |
| `SasNas2` | `string` | vacio | SAS para cuentas NAS2. Si no se pasa, se solicita por consola. |
| `LogRoot` | `string` | `C:\Source\logs\sync` | Carpeta base para la corrida. |
| `MaxOpenWindows` | `int` | `3` | Maximo de ventanas abiertas en los syncs lanzados. |
| `AzConcurrency` | `int` | `16` | Concurrencia de AzCopy. |
| `AzBufferGB` | `int` | `1` | Buffer de AzCopy en GB. |

### Parametros obligatorios
No tiene parametros obligatorios a nivel sintactico, pero en la practica requiere:
- un `SyncScriptPath` valido
- un `AzCopyPath` valido
- SAS validos para NAS1 y NAS2

### Archivos y logs que genera
- una carpeta de corrida con timestamp bajo `LogRoot`
- subcarpetas por bloque de sync
- logs de cada invocacion de `ps_SyncAzureFiles.ps1`

### Ejemplo de uso
```powershell
.\Run_All_Syncs_Fonvalmed.ps1 `
  -SyncScriptPath ".\ps_SyncAzureFiles.ps1" `
  -AzCopyPath ".\azcopy.exe" `
  -LogRoot "C:\Logs\fonvalmed-sync"
```

### Riesgos y advertencias
- Este script no es generico: contiene rutas y destinos codificados para un caso concreto.
- Si las cuentas no contienen `nas1` o `nas2` en el nombre, la seleccion automatica de SAS falla.
- Conviene tratarlo como plantilla de automatizacion o runner especifico del proyecto.

---

## `ps_FixDuplicatedSubFolders.ps1`

### Objetivo
Corregir una estructura duplicada del tipo `Raiz\Subcarpeta\Subcarpeta`, moviendo el contenido desde la carpeta interna hacia la externa.

### Cuando usarlo
- despues de detectar una anomalia de duplicacion de carpetas
- como correctivo post-migracion

### Que hace internamente
- enumera cada subcarpeta inmediata de `ShareRoot`
- verifica si dentro existe otra carpeta con el mismo nombre
- mueve el contenido de la carpeta interna hacia el nivel superior
- elimina la carpeta duplicada si queda vacia

### Parametros

| Parametro | Tipo | Default | Uso |
| --- | --- | --- | --- |
| `ShareRoot` | `string` | n/a | Ruta raiz en el share destino. Obligatorio. |
| `WhatIf` | `switch` | `true` efectivo | Dry-run por seguridad. |
| `OverwriteExisting` | `switch` | `false` | Fuerza movimiento si ya existe un nombre igual en destino. |

### Parametros obligatorios
- `ShareRoot`

### Parametros opcionales
- `WhatIf`
- `OverwriteExisting`

### Archivos y logs que genera
- no genera archivos de log dedicados
- escribe el detalle y el resumen por consola

### Ejemplo de uso
```powershell
.\ps_FixDuplicatedSubFolders.ps1 `
  -ShareRoot "\\mystorageaccount.file.core.windows.net\documentos\raiz-migracion" `
  -WhatIf
```

Ejecucion real:

```powershell
.\ps_FixDuplicatedSubFolders.ps1 `
  -ShareRoot "\\mystorageaccount.file.core.windows.net\documentos\raiz-migracion" `
  -WhatIf:$false
```

### Riesgos y advertencias
- `-WhatIf` queda activo por defecto aunque no lo pases.
- `-OverwriteExisting` puede fusionar o sobreescribir nombres existentes.
- No hace parte del flujo normal; usalo solo como correctivo despues de validar la estructura.

---

## Orden sugerido de uso
Resumen practico:

1. `ps_GetFilesAndFoldersStructure_v2.ps1`
   - Audita origen, permisos, tamano y nombres problematicos.
2. `ps_RunInventoryAndUploadFromRoot.ps1`
   - Para raiz con muchas subcarpetas; normalmente sera el script principal de operacion.
3. `ps_UploadToFileShareFromCsv_v2.ps1`
   - Para cargas puntuales o pruebas controladas.
4. `ps_SyncAzureFiles.ps1`
   - Para sincronizacion incremental luego de la carga inicial.
5. `Run_All_Syncs_Fonvalmed.ps1`
   - Solo cuando trabajas con ese lote ya preconfigurado.
6. `ps_FixDuplicatedSubFolders.ps1`
   - Solo si aparece la anomalia `Carpeta\Carpeta`.

## Outputs importantes del conjunto
Estos son los artefactos que conviene conocer en la operacion:
- `inventory.csv`
- `inventory-failed-or-denied.csv`
- `inventory.log`
- `folder-info.txt`
- `upload-logs-*.txt`
- carpeta `azcopy`
- `resumen-conciliaciones.csv`
- `resumen-conciliaciones_dedup.csv`
- `resumen-subidas.csv`
- `resumen-subidas_dedup.csv`
- `summary.csv`
- `diff_missing_in_dest.csv`
- `diff_extra_in_dest.csv`

## Observaciones operativas
- `DoOnly` y `Exclude` usan coincidencia por nombre de subcarpeta y esperan una lista separada por `;`.
- En `ps_RunInventoryAndUploadFromRoot.ps1`, si trabajas con muchas carpetas, empieza con `-DoInventory` solamente para detectar problemas antes de subir.
- Si vas a abrir muchas ventanas, ajusta `MaxOpenWindows` y `RamSafeLimit` para no saturar la maquina.
- La documentacion de este README describe el comportamiento actual observado en codigo, incluyendo limitaciones como el `HoldOnError` no implementado en el orquestador principal.
