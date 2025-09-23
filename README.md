# Sistema de Migración AS400

Script Python para procesar archivos CSV, validar estructura de columnas, aplicar transformaciones de caracteres y subirlos por FTP a servidor AS400.

## 📋 Características

- **Validación de columnas** según esquemas configurables
- **Detección automática de encoding** (UTF-8, Windows-1252, ISO-8859-1)
- **Reemplazo de caracteres especiales** parametrizable
- **Soporte para columnas opcionales** mediante comodín (`*`)
- **Logging detallado** de todo el proceso
- **Subida FTP** con limpieza automática del directorio remoto
- **Modo desarrollo** para pruebas sin conexión FTP

## 🚀 Instalación

### 1. Clonar el repositorio

```bash
git clone [url-del-repositorio]
cd migracion-as400
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Configurar variables de entorno

```bash
cp .env.example .env
```

Editar `.env` con las credenciales reales:

```bash
FTP_USER=tu_usuario
FTP_PASSWORD=tu_password
```

## 📁 Estructura del Proyecto

```
migracion-as400/
├── config/
│   ├── esquemas.json       # Definición de columnas por tipo de archivo
│   └── reemplazos.json     # Mapeo de caracteres especiales
├── raw/                     # Carpeta de archivos de entrada
├── processed/               # Carpeta de archivos procesados
├── logs/                    # Logs de cada ejecución
├── .env                     # Variables de entorno (no se sube a Git)
├── .env.example            # Plantilla de configuración
├── migration.py            # Script principal
├── requirements.txt        # Dependencias Python
└── README.md              # Este archivo
```

## ⚙️ Configuración

### Esquemas de columnas (`config/esquemas.json`)

Define las columnas esperadas para cada tipo de archivo:

```json
{
  "CPQMIGPN": [
    "FECING",
    "AGCVIN",
    "...",
    "ESTADOT",
    "*"
  ]
}
```

El `*` al final indica que se permiten columnas adicionales.

### Reemplazos de caracteres (`config/reemplazos.json`)

```json
{
  "Ñ": "||||",
  "ñ": "|||",
  "Á": "'A'",
  "á": "'a'"
}
```

## 🎮 Uso

### Ejecución normal

```bash
python migration.py
```

El script:

1. Solicita el separador de los archivos de entrada
2. Valida la estructura de columnas según esquemas
3. Rechaza archivos no parametrizados o con caracteres corruptos
4. Procesa y transforma los archivos válidos
5. Sube los archivos al servidor FTP
6. Genera log detallado en `logs/`

### Modo desarrollo (sin FTP)

```bash
SKIP_FTP=true python migration.py
```

### Modo prueba completo

```bash
MODO_DESARROLLO=true python migration.py
```

## 📝 Validaciones

### Archivos aceptados

- ✅ Archivos parametrizados en `esquemas.json`
- ✅ Encodings válidos (UTF-8, Windows-1252, ISO-8859-1)
- ✅ Estructura de columnas correcta

### Archivos rechazados

- ❌ No configurados en esquemas
- ❌ Con caracteres corruptos (`�`, `ï¿½`)
- ❌ Estructura de columnas incorrecta
- ❌ Faltan columnas obligatorias

## 📊 Logs

Los logs se generan en `logs/migracion_YYYYMMDD_HHMMSS.log` con:

- Estado de cada archivo procesado
- Errores de validación detallados
- Proceso FTP completo
- Resumen final de la ejecución

## 🔧 Solución de Problemas

### Error: "Archivo contiene caracteres corruptos"

El archivo tiene caracteres mal codificados. Solución:

1. Abrir el archivo en Excel
2. Guardar como → CSV UTF-8 (delimitado por comas)

### Error: "Archivo no parametrizado en esquemas"

Agregar la configuración del archivo en `config/esquemas.json`

### Error: "Columnas no coinciden"

Verificar que:

- No haya columnas vacías extras (doble coma `,,`)
- Los nombres coincidan en los primeros 6 caracteres
- La cantidad de columnas sea correcta

## 🤝 Contribuir

1. Crear rama para cambios: `git checkout -b feature/nueva-funcionalidad`
2. Hacer commit: `git commit -m "Descripción del cambio"`
3. Push: `git push origin feature/nueva-funcionalidad`
4. Crear Pull Request
