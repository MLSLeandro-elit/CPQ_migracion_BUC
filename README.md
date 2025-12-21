# CPQ Migration Tool

Herramienta para procesar archivos Excel/CSV y subirlos al servidor AS400 vía FTP.

## Qué hace

1. Convierte archivos Excel (.xlsx) o CSV a formato estándar
2. Renombra columnas según esquemas configurados
3. Convierte fechas numéricas a formato YYYYMMDD
4. Reemplaza caracteres especiales (Ñ, acentos, etc.)
5. Sube los archivos procesados al servidor AS400 por FTP

## Instalación

```bash
git clone <repository-url>
cd cpq-migration
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Uso

```bash
python migration.py
```

El script pregunta interactivamente:
- Tipo de archivo (Excel o CSV)
- Separador de entrada (para CSV)

### Modo prueba (sin FTP)

```bash
SKIP_FTP=true python migration.py
```

## Estructura

```
cpq-migration/
├── migration.py            # Orquestador principal
├── processor.py            # Conversión y procesamiento
├── ftp_uploader.py         # Subida FTP
├── config/
│   ├── esquemas.json       # Definición de columnas por archivo
│   └── reemplazos.json     # Caracteres a reemplazar
├── entrada_xlsx/           # Archivos Excel de entrada
├── entrada_csv/            # Archivos CSV de entrada
├── salida/                 # Archivos procesados
└── logs/                   # Registros de ejecución
```

## Configuración

### Variables de entorno (.env)

```bash
# FTP
FTP_HOST=10.238.60.3
FTP_USER=usuario
FTP_PASSWORD=password
FTP_CARPETA_REMOTA=/MIGCPQBUC

# Procesamiento
SEPARADOR_SALIDA=|
SEPARADOR_DECIMAL=.
CONSERVAR_ENTRADA=true
SKIP_FTP=false
```

### Esquemas (config/esquemas.json)

Cada tipo de archivo se define con sus columnas y opciones:

```json
{
  "CPQMIGPN": {
    "fila_nombres_columna": 2,
    "filas_omitir": [1],
    "columnas": ["COL1", "COL2", "COL3", "*"],
    "fechas_numericas": ["FECNAC", "FECEXP"]
  }
}
```

- `fila_nombres_columna`: Fila donde están los nombres de columna (default: 1)
- `filas_omitir`: Filas a eliminar antes de procesar
- `columnas`: Nombres de columnas en orden (`*` permite columnas adicionales)
- `fechas_numericas`: Columnas con fechas en formato numérico de Excel

### Detección de archivos

El sistema detecta el tipo de archivo en dos pasadas:
1. Por nombre (si el archivo empieza con el nombre del esquema)
2. Por estructura (compara columnas con los esquemas disponibles)

## Logs

Los logs se guardan en `logs/` con el detalle de cada ejecución, incluyendo el mapeo de archivos procesados:

```
FASE 1 - CONVERSIÓN XLSX → CSV:
  ✓ V2_BENEFICIARIOS OCT_25.xlsx → HOMOBENEF.csv

FASE 2 - PROCESAMIENTO CSV:
  ✓ HOMOBENEF.csv (origen: V2_BENEFICIARIOS OCT_25.xlsx)
```