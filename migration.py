"""Orquestador de migración CPQ.

Este script coordina el proceso completo de migración:
1. Procesar archivos (XLSX/CSV → CSV transformado en SALIDA)
2. Subir archivos a AS400 vía FTP

Uso:
    python migration.py

Componentes:
    - processor.py: Procesamiento de archivos
    - ftp_uploader.py: Subida FTP
"""

import os
from datetime import datetime
from dotenv import load_dotenv

from processor import (
    ProcessConfig, 
    MigrationLogger, 
    SchemaLoader, 
    ExcelConverter,
    ColumnValidator,
    CharacterProcessor,
    SmartFileHandler,
    procesar,
    _procesar_archivos_csv
)
from ftp_uploader import FTPConfig, FTPUploader, DummyUploader, subir


# =============================================================================
# CARGA DE CONFIGURACIÓN
# =============================================================================

load_dotenv()


# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def preguntar_tipo_entrada() -> str:
    """Pregunta al usuario qué tipo de archivos procesar."""
    print("\n" + "=" * 60)
    print("TIPO DE ARCHIVOS A PROCESAR")
    print("=" * 60)
    print("¿Qué tipo de archivos desea procesar?")
    print("  1) Excel (.xlsx) desde entrada_xlsx/")
    print("  2) CSV (.csv) desde entrada_csv/")
    
    while True:
        opcion = input("\nSeleccione una opción (1-2): ").strip()
        if opcion == '1':
            print("Seleccionado: Excel (.xlsx)")
            return 'xlsx'
        elif opcion == '2':
            print("Seleccionado: CSV (.csv)")
            return 'csv'
        else:
            print("Opción inválida. Ingrese 1 o 2.")


def preguntar_separador_csv() -> str:
    """Pregunta al usuario el separador del CSV de entrada."""
    print("\n" + "=" * 60)
    print("CONFIGURACIÓN DE SEPARADOR CSV")
    print("=" * 60)
    print("¿Qué separador tienen los archivos CSV de entrada?")
    print("  1) Punto y coma (;)  ← Recomendado")
    print("  2) Pipe (|)")
    print("  3) Coma (,)")
    print("  4) Tabulador (\\t)")
    print("  5) Otro")
    
    separadores = {
        '1': ';',
        '2': '|',
        '3': ',',
        '4': '\t'
    }
    nombres = {
        '1': 'Punto y coma (;)',
        '2': 'Pipe (|)',
        '3': 'Coma (,)',
        '4': 'Tabulador (\\t)'
    }
    
    while True:
        opcion = input("\nSeleccione una opción (1-5) [1]: ").strip() or '1'
        
        if opcion in separadores:
            sep = separadores[opcion]
            print(f"Separador seleccionado: {nombres[opcion]}")
            return sep
        elif opcion == '5':
            sep = input("Ingrese el carácter separador: ")
            if sep:
                print(f"Separador seleccionado: '{sep}'")
                return sep
            print("Separador inválido")
        else:
            print("Opción inválida")


def crear_configuracion(tipo_entrada: str, separador_csv: str = ';') -> ProcessConfig:
    """Crea configuración del proceso."""
    return ProcessConfig(
        carpeta_entrada_xlsx=os.getenv('CARPETA_ENTRADA_XLSX', 'entrada_xlsx'),
        carpeta_entrada_csv=os.getenv('CARPETA_ENTRADA_CSV', 'entrada_csv'),
        carpeta_temporal=os.getenv('CARPETA_TEMPORAL', 'temporal'),
        carpeta_salida=os.getenv('CARPETA_SALIDA', 'salida'),
        carpeta_logs=os.getenv('CARPETA_LOGS', 'logs'),
        separador_salida=os.getenv('SEPARADOR_SALIDA', '|'),
        separador_decimal=os.getenv('SEPARADOR_DECIMAL', '.'),
        archivo_esquemas=os.getenv('ARCHIVO_ESQUEMAS', 'config/esquemas.json'),
        archivo_reemplazos=os.getenv('ARCHIVO_REEMPLAZOS', 'config/reemplazos.json'),
        conservar_entrada=os.getenv('CONSERVAR_ENTRADA', 'true').lower() == 'true',
        tipo_entrada=tipo_entrada,
        separador_entrada_csv=separador_csv
    )


def crear_ftp_config() -> FTPConfig:
    """Crea configuración FTP."""
    return FTPConfig(
        host=os.getenv('FTP_HOST', ''),
        user=os.getenv('FTP_USER', ''),
        password=os.getenv('FTP_PASSWORD', ''),
        carpeta_remota=os.getenv('FTP_CARPETA_REMOTA', '/ruta/remota'),
        modo_pasivo=os.getenv('FTP_MODO_PASIVO', 'true').lower() == 'true',
        timeout=int(os.getenv('FTP_TIMEOUT', '30'))
    )


def mostrar_configuracion(config: ProcessConfig, logger: MigrationLogger):
    """Muestra la configuración del proceso."""
    logger.log(f"\n[CONFIG] Configuración del proceso:")
    logger.log(f"  - Tipo de entrada: {config.tipo_entrada.upper()}")
    
    if config.tipo_entrada == 'xlsx':
        logger.log(f"  - Carpeta entrada: {config.carpeta_entrada_xlsx}/")
    else:
        logger.log(f"  - Carpeta entrada: {config.carpeta_entrada_csv}/")
        logger.log(f"  - Separador CSV entrada: '{config.separador_entrada_csv}'")
    
    logger.log(f"  - Carpeta salida: {config.carpeta_salida}/")
    logger.log(f"  - Separador salida: '{config.separador_salida}'")
    logger.log(f"  - Separador decimal: '{config.separador_decimal}'")
    logger.log(f"  - Conservar entrada: {'Sí' if config.conservar_entrada else 'No'}")


# =============================================================================
# PROCESO PRINCIPAL
# =============================================================================

def ejecutar():
    """Ejecuta el proceso completo de migración."""
    
    # Preguntar tipo de entrada
    tipo_entrada = preguntar_tipo_entrada()
    
    # Preguntar separador solo si es CSV
    separador_csv = ';'
    if tipo_entrada == 'csv':
        separador_csv = preguntar_separador_csv()
    
    # Crear configuración
    config = crear_configuracion(tipo_entrada, separador_csv)
    
    # Crear logger unificado
    logger = MigrationLogger(config.carpeta_logs, "migracion")
    logger.inicio_proceso()
    
    # Mostrar configuración
    mostrar_configuracion(config, logger)
    
    # Cargar componentes
    schema_loader = SchemaLoader(config.archivo_esquemas)
    excel_converter = ExcelConverter(
        schema_loader, 
        config.separador_salida,
        config.separador_decimal
    )
    validator = ColumnValidator(schema_loader, config.separador_salida)
    processor = CharacterProcessor(config.archivo_reemplazos)
    file_handler = SmartFileHandler()
    
    # Limpiar directorio de salida
    if os.path.exists(config.carpeta_salida):
        for archivo in os.listdir(config.carpeta_salida):
            os.remove(os.path.join(config.carpeta_salida, archivo))
    os.makedirs(config.carpeta_salida, exist_ok=True)
    logger.log(f"\n[CLEAN] Directorio limpio: {config.carpeta_salida}")
    
    # === FASE 1 y 2: Procesamiento ===
    import shutil
    
    if config.tipo_entrada == 'xlsx':
        # Convertir XLSX → CSV temporal
        archivos_csv, archivos_xlsx_exitosos = excel_converter.convertir_carpeta(
            config.carpeta_entrada_xlsx, config.carpeta_temporal, logger
        )
        
        # Procesar CSV (reemplazos de caracteres)
        archivos_a_eliminar = _procesar_archivos_csv(
            config.carpeta_temporal, config.carpeta_salida,
            config.separador_salida, config.separador_salida,
            validator, processor, file_handler, logger
        )
        
        # Eliminar archivos originales si está configurado
        if not config.conservar_entrada:
            for archivo in archivos_xlsx_exitosos:
                ruta = os.path.join(config.carpeta_entrada_xlsx, archivo)
                if os.path.exists(ruta):
                    os.remove(ruta)
                    logger.log(f"[CLEAN] Eliminado: {archivo}")
        else:
            logger.log(f"\n[INFO] CONSERVAR_ENTRADA=true - Archivos de entrada no eliminados")
        
        # Limpiar temporales
        if os.path.exists(config.carpeta_temporal):
            shutil.rmtree(config.carpeta_temporal)
    else:
        # Procesar CSV directamente con pandas
        archivos_procesados, archivos_csv_exitosos = excel_converter.convertir_carpeta_csv(
            config.carpeta_entrada_csv, config.carpeta_salida,
            config.separador_entrada_csv, logger
        )
        
        # Procesar reemplazos de caracteres
        archivos_a_eliminar = _procesar_archivos_csv(
            config.carpeta_salida, config.carpeta_salida,
            config.separador_salida, config.separador_salida,
            validator, processor, file_handler, logger
        )
        
        # Eliminar archivos originales si está configurado
        if not config.conservar_entrada:
            for archivo in archivos_csv_exitosos:
                ruta = os.path.join(config.carpeta_entrada_csv, archivo)
                if os.path.exists(ruta):
                    os.remove(ruta)
                    logger.log(f"[CLEAN] Eliminado: {archivo}")
        else:
            logger.log(f"\n[INFO] CONSERVAR_ENTRADA=true - Archivos de entrada no eliminados")
    
    # === FASE 3: Subida FTP ===
    skip_ftp = os.getenv('SKIP_FTP', 'false').lower() == 'true'
    
    if skip_ftp:
        logger.log("\n[INFO] SKIP_FTP=true - Omitiendo subida FTP")
    else:
        ftp_config = crear_ftp_config()
        
        if ftp_config.host and ftp_config.user:
            uploader = FTPUploader(ftp_config)
            uploader.subir(config.carpeta_salida, logger)
        else:
            dummy = DummyUploader()
            dummy.subir(config.carpeta_salida, logger)
    
    # === Resumen Final ===
    logger.resumen_final(config)


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

def main():
    """Punto de entrada principal."""
    try:
        ejecutar()
    except KeyboardInterrupt:
        print("\n\n[CANCELADO] Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"\n[ERROR FATAL] {e}")
        raise


if __name__ == "__main__":
    main()