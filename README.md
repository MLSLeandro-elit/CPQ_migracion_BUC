# CPQ Migration Tool

Herramienta de migración para procesar archivos Excel/CSV, aplicar transformaciones y subirlos al servidor AS400 vía FTP.

## Tabla de Contenidos

- [Descripción](#descripción)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Uso](#uso)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Esquemas de Archivos](#esquemas-de-archivos)
- [Variables de Entorno](#variables-de-entorno)
- [Logs](#logs)
- [Solución de Problemas](#solución-de-problemas)

## Descripción

Este script automatiza el proceso de migración de datos desde archivos Excel (.xlsx) o CSV hacia el servidor AS400. El proceso incluye:

- Conversión de archivos Excel a CSV
- Validación de estructura según esquemas configurados
- Transformación de caracteres especiales
- Cambio de separadores
- Subida automática por FTP

## Requisitos

- Python 3.8 o superior
- Acceso al servidor FTP AS400
- Archivos de configuración (esquemas, reemplazos)

## Instalación

1. Clonar el repositorio:

```bash
git clone <repository-url>
cd cpq-migration
```

2. Crear y activar entorno virtual:

```bash
python3 -m venv .venv
source .venv/bin/activate  # Linux/macOS
```

3. Instalar dependencias:

```bash
pip install -r requirements.txt
```

4. Configurar variables de entorno:

```bash
cp .env.example .env
# Editar .env con los valores correspondientes
```

## Configuración

### Archivos de Configuración

| Archivo | Descripción |
|---------|-------------|
| `.env` | Variables de entorno (FTP, rutas, opciones) |
| `config/esquemas.json` | Definición de estructura por tipo de archivo |
| `config/reemplazos.json` | Mapeo de caracteres especiales |

### Configuración de Esquemas

Cada tipo de archivo se configura en `config/esquemas.json`:

```json
{
  "NOMBRE_ARCHIVO": {
    "filas_omitir": [1, 3],
    "columnas": ["COL1", "COL2", "COL3"],
    "fechas_numericas": ["FECHA1", "FECHA2"]
  }
}
```

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `filas_omitir` | array | Filas a eliminar antes de procesar (numeración desde 1) |
| `columnas` | array | Lista ordenada de nombres de columnas |
| `fechas_numericas` | array | Columnas que contienen fechas en formato numérico |

#### Comodín para columnas adicionales

Usar `*` al final del array de columnas para permitir columnas adicionales:

```json
{
  "ARCHIVO_FLEXIBLE": {
    "columnas": ["COL1", "COL2", "COL3", "*"],
    "fechas_numericas": []
  }
}
```

### Configuración de Reemplazos

Definir caracteres a reemplazar en `config/reemplazos.json`:

```json
{
  "Ñ": "||||",
  "ñ": "|||",
  "Á": "'A'",
  "á": "'a'"
}
```

## Uso

### Ejecución

```bash
python migration.py
```

### Flujo Interactivo

1. **Selección de tipo de archivo:**

```
¿Qué tipo de archivos desea procesar?
  1) Excel (.xlsx) desde entrada_xlsx/
  2) CSV (.csv) desde entrada_csv/
```

2. **Selección de separador:**

```
¿Qué separador tienen los archivos CSV?
  1) Punto y coma (;)
  2) Pipe (|)
  3) Coma (,)
  4) Tabulador (\t)
  5) Otro
```

3. **Procesamiento automático:**
   - Validación de estructura
   - Transformación de datos
   - Subida FTP

### Modo de Prueba

Para ejecutar sin subir al servidor FTP:

```bash
SKIP_FTP=true python migration.py
```

## Estructura del Proyecto

```
cpq-migration/
├── migration.py            # Script principal
├── requirements.txt        # Dependencias Python
├── .env                    # Variables de entorno (no versionado)
├── .env.example            # Plantilla de variables
├── config/
│   ├── esquemas.json       # Definición de estructuras
│   └── reemplazos.json     # Mapeo de caracteres
├── entrada_xlsx/           # Archivos Excel de entrada
├── entrada_csv/            # Archivos CSV de entrada
├── salida/                 # Archivos procesados
└── logs/                   # Registros de ejecución
```

### Carpetas de Entrada

| Carpeta | Descripción |
|---------|-------------|
| `entrada_xlsx/` | Colocar archivos Excel (.xlsx) a procesar |
| `entrada_csv/` | Colocar archivos CSV (.csv) a procesar |

### Carpetas de Salida

| Carpeta | Descripción |
|---------|-------------|
| `salida/` | Archivos procesados listos para FTP |
| `logs/` | Registros detallados de cada ejecución |

## Esquemas de Archivos

### Tipos de Archivo Soportados

| Archivo | Descripción |
|---------|-------------|
| CPQMIGPN | Personas naturales |
| CPQMIGPJ | Personas jurídicas |
| HOMOBENEF | Beneficiarios |
| HOMOROLESF | Roles y relaciones |

### Validaciones Aplicadas

- Cantidad de columnas según esquema
- Orden de columnas
- Formato de fechas numéricas

## Variables de Entorno

### Conexión FTP

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `FTP_HOST` | Servidor FTP | `10.238.60.3` |
| `FTP_USER` | Usuario FTP | `USUARIO` |
| `FTP_PASSWORD` | Contraseña FTP | `********` |
| `FTP_CARPETA_REMOTA` | Carpeta destino | `/MIGCPQBUC` |

### Rutas y Archivos

| Variable | Descripción | Default |
|----------|-------------|---------|
| `CARPETA_ENTRADA_XLSX` | Carpeta de Excel | `entrada_xlsx` |
| `CARPETA_ENTRADA_CSV` | Carpeta de CSV | `entrada_csv` |
| `CARPETA_SALIDA` | Carpeta de salida | `salida` |
| `CARPETA_LOGS` | Carpeta de logs | `logs` |
| `ARCHIVO_ESQUEMAS` | Ruta esquemas | `config/esquemas.json` |
| `ARCHIVO_REEMPLAZOS` | Ruta reemplazos | `config/reemplazos.json` |

### Opciones de Procesamiento

| Variable | Descripción | Default |
|----------|-------------|---------|
| `SEPARADOR_SALIDA` | Separador del archivo final | `\|` |
| `CONSERVAR_ENTRADA` | No eliminar archivos de entrada | `false` |
| `SKIP_FTP` | Omitir subida FTP | `false` |

## Logs

Los registros se guardan en `logs/` con formato:

```
migracion_YYYYMMDD_HHMMSS.log
```

### Contenido del Log

- Inicio y fin de proceso
- Archivos procesados
- Validaciones realizadas
- Errores encontrados
- Resultado de subida FTP

### Ejemplo de Log

```
================================================================================
2025-01-15 10:30:00 - INICIO PROCESO DE MIGRACIÓN
================================================================================

FASE 1: CONVERSIÓN XLSX → CSV
--------------------------------------------------------------------------------
  [OK] CPQMIGPN.xlsx → CPQMIGPN.csv (150 filas, 96 columnas)

FASE 2: VALIDACIÓN Y PROCESAMIENTO CSV
--------------------------------------------------------------------------------
ARCHIVO: CPQMIGPN.csv
[OK] Validación de columnas exitosa
[OK] Reemplazo de caracteres completado
[OK] Archivo guardado en salida/CPQMIGPN.csv

FASE 3: SUBIDA FTP AL SERVIDOR AS400
================================================================================
  ✓ CPQMIGPN.csv (45 KB)

RESUMEN FINAL
================================================================================
Archivos procesados: 1
Archivos subidos: 1
```

## Solución de Problemas

### Error: Columnas insuficientes

**Causa:** El archivo no tiene la cantidad de columnas esperada.

**Solución:** Verificar que el archivo corresponda al tipo correcto y que el esquema esté actualizado.

### Error: Archivo no parametrizado

**Causa:** El nombre del archivo no coincide con ningún tipo en `esquemas.json`.

**Solución:** Agregar la configuración del archivo en `esquemas.json` o renombrar el archivo.

### Error: Conexión FTP fallida

**Causa:** Credenciales incorrectas o servidor inaccesible.

**Solución:** Verificar variables de entorno FTP y conectividad de red.

## Licencia

Uso interno - Taylor & Johnson