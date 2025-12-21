"""Módulo de subida FTP para migración CPQ.

Este módulo se encarga de:
- Conectar al servidor AS400 vía FTP
- Eliminar solo los archivos que se van a subir (no todo el IFS)
- Subir archivos desde carpeta SALIDA
- Registrar resultados en log

Uso independiente:
    python ftp_uploader.py

Uso como módulo:
    from ftp_uploader import subir
    subir(logger=mi_logger)
"""

import os
import logging
from dataclasses import dataclass
from typing import List, Optional
from ftplib import FTP
from datetime import datetime
from dotenv import load_dotenv


# =============================================================================
# CARGA DE CONFIGURACIÓN EXTERNA
# =============================================================================

load_dotenv()


@dataclass
class FTPConfig:
    """Configuración de conexión FTP."""
    host: str
    user: str
    password: str
    carpeta_remota: str
    modo_pasivo: bool = True
    timeout: int = 30


# =============================================================================
# SISTEMA DE LOGGING
# =============================================================================

class FTPLogger:
    """Manejador de logs para el proceso de FTP."""

    def __init__(self, carpeta_logs: str, prefijo: str = "ftp"):
        os.makedirs(carpeta_logs, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.archivo_log = os.path.join(carpeta_logs, f"{prefijo}_{timestamp}.log")

        self.logger = logging.getLogger(f'{prefijo}_{timestamp}')
        self.logger.setLevel(logging.INFO)
        self.logger.handlers = []

        file_handler = logging.FileHandler(self.archivo_log, encoding='utf-8')
        console_handler = logging.StreamHandler()

        formatter = logging.Formatter('%(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.archivos_ftp_ok = []
        self.archivos_ftp_error = []

    def linea_separadora(self, char='=', length=80):
        return char * length

    def log(self, mensaje: str):
        self.logger.info(mensaje)

    def inicio_ftp(self, config: FTPConfig):
        self.log("")
        self.log(self.linea_separadora('='))
        self.log("SUBIDA FTP AL SERVIDOR AS400")
        self.log(self.linea_separadora('='))
        self.log(f"INICIO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(f"Host: {config.host}")
        self.log(f"Usuario: {config.user}")
        self.log(f"Carpeta remota: {config.carpeta_remota}")
        self.log(self.linea_separadora('-'))

    def fin_ftp(self, exitoso: bool):
        if exitoso:
            self.log(f"\n[FTP] Resumen de subida:")
            self.log(f"  - Archivos subidos: {len(self.archivos_ftp_ok)}")
            for archivo in self.archivos_ftp_ok:
                self.log(f"    ✓ {archivo}")
            if self.archivos_ftp_error:
                self.log(f"  - Archivos con error: {len(self.archivos_ftp_error)}")
                for archivo in self.archivos_ftp_error:
                    self.log(f"    ✗ {archivo}")
        else:
            self.log("\n[FTP] Subida fallida")
        self.log(self.linea_separadora('='))

    def resumen_final(self):
        self.log("")
        self.log(self.linea_separadora('='))
        self.log(f"RESUMEN FINAL - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(self.linea_separadora('='))
        self.log(f"\nArchivos subidos: {len(self.archivos_ftp_ok)}")
        if self.archivos_ftp_ok:
            for archivo in self.archivos_ftp_ok:
                self.log(f"  ✓ {archivo}")
        self.log(f"Errores de subida: {len(self.archivos_ftp_error)}")
        if self.archivos_ftp_error:
            for archivo in self.archivos_ftp_error:
                self.log(f"  ✗ {archivo}")
        self.log(f"\nLog guardado en: {self.archivo_log}")
        self.log(self.linea_separadora('='))


# =============================================================================
# SUBIDOR FTP
# =============================================================================

class FTPUploader:
    """Sube archivos al servidor AS400 via FTP."""

    def __init__(self, config: FTPConfig):
        self.config = config
        self.ftp: Optional[FTP] = None

    def conectar(self, logger) -> bool:
        """Establece conexión FTP."""
        try:
            self.ftp = FTP()
            self.ftp.connect(self.config.host, timeout=self.config.timeout)
            self.ftp.login(self.config.user, self.config.password)

            if self.config.modo_pasivo:
                self.ftp.set_pasv(True)

            logger.log(f"[OK] Conexión FTP establecida")
            return True

        except Exception as e:
            logger.log(f"[ERROR] Error de conexión FTP: {e}")
            return False

    def cambiar_directorio(self, logger) -> bool:
        """Cambia al directorio remoto."""
        try:
            self.ftp.cwd(self.config.carpeta_remota)
            logger.log(f"[OK] Directorio cambiado a: {self.config.carpeta_remota}")
            return True
        except Exception as e:
            logger.log(f"[ERROR] Error cambiando directorio: {e}")
            return False

    def eliminar_archivo_remoto(self, nombre_archivo: str, logger) -> bool:
        """Elimina un archivo específico del servidor remoto."""
        try:
            self.ftp.delete(nombre_archivo)
            logger.log(f"  [OK] Eliminado del servidor: {nombre_archivo}")
            return True
        except Exception as e:
            # Si el archivo no existe, no es error
            if "550" in str(e):
                logger.log(f"  [INFO] No existe en servidor: {nombre_archivo}")
                return True
            logger.log(f"  [WARN] Error eliminando {nombre_archivo}: {e}")
            return False

    def subir_archivo(self, ruta_local: str, nombre_remoto: str, logger) -> bool:
        """Sube un archivo al servidor."""
        try:
            tamaño = os.path.getsize(ruta_local)
            tamaño_kb = tamaño / 1024

            with open(ruta_local, 'rb') as f:
                self.ftp.storbinary(f'STOR {nombre_remoto}', f)

            logger.log(f"  ✓ {nombre_remoto} ({tamaño_kb:.1f} KB)")
            logger.archivos_ftp_ok.append(nombre_remoto)
            return True

        except Exception as e:
            logger.log(f"  ✗ {nombre_remoto}: {e}")
            logger.archivos_ftp_error.append(nombre_remoto)
            return False

    def desconectar(self, logger):
        """Cierra la conexión FTP."""
        try:
            if self.ftp:
                self.ftp.quit()
                logger.log("[OK] Conexión FTP cerrada")
        except:
            pass

    def subir(self, carpeta_local: str, logger) -> bool:
        """
        Sube todos los archivos de una carpeta al servidor.
        Solo elimina del servidor los archivos que se van a subir.
        """
        logger.inicio_ftp(self.config)

        if not self.conectar(logger):
            logger.fin_ftp(False)
            return False

        if not self.cambiar_directorio(logger):
            self.desconectar(logger)
            logger.fin_ftp(False)
            return False

        # Listar archivos a subir
        archivos = [f for f in os.listdir(carpeta_local)
                    if os.path.isfile(os.path.join(carpeta_local, f))]

        if not archivos:
            logger.log("[INFO] No hay archivos para subir")
            self.desconectar(logger)
            logger.fin_ftp(True)
            return True

        logger.log(f"\n[INFO] Archivos a subir: {len(archivos)}")
        logger.log("")

        # Eliminar solo los archivos que se van a subir
        logger.log("[PASO 1] Eliminando archivos anteriores del servidor...")
        for archivo in archivos:
            self.eliminar_archivo_remoto(archivo, logger)

        # Subir archivos
        logger.log("\n[PASO 2] Subiendo archivos nuevos...")
        for archivo in archivos:
            ruta_local = os.path.join(carpeta_local, archivo)
            self.subir_archivo(ruta_local, archivo, logger)

        self.desconectar(logger)
        logger.fin_ftp(True)
        return True


class DummyUploader:
    """Uploader de prueba que no hace nada (modo test)."""

    def subir(self, carpeta_local: str, logger) -> bool:
        logger.log("\n[INFO] Subida FTP omitida (modo prueba)")
        return True


# =============================================================================
# FUNCIÓN PRINCIPAL DE SUBIDA
# =============================================================================

def subir(carpeta_salida: str = None, logger = None) -> bool:
    """
    Sube archivos de carpeta SALIDA al servidor AS400.
    
    Args:
        carpeta_salida: Carpeta con archivos a subir (default: 'salida')
        logger: Logger a usar (si None, crea uno nuevo)
    
    Returns:
        True si la subida fue exitosa
    """
    # Configuración por defecto
    if carpeta_salida is None:
        carpeta_salida = os.getenv('CARPETA_SALIDA', 'salida')
    
    carpeta_logs = os.getenv('CARPETA_LOGS', 'logs')
    
    # Verificar configuración FTP
    ftp_host = os.getenv('FTP_HOST', '')
    ftp_user = os.getenv('FTP_USER', '')
    ftp_password = os.getenv('FTP_PASSWORD', '')
    ftp_carpeta = os.getenv('FTP_CARPETA_REMOTA', '/ruta/remota')
    
    if not ftp_host or not ftp_user:
        print("[INFO] Configuración FTP incompleta - omitiendo subida")
        return True
    
    # Logger
    crear_logger = logger is None
    if crear_logger:
        logger = FTPLogger(carpeta_logs, "ftp")
    
    # Configuración FTP
    ftp_config = FTPConfig(
        host=ftp_host,
        user=ftp_user,
        password=ftp_password,
        carpeta_remota=ftp_carpeta,
        modo_pasivo=os.getenv('FTP_MODO_PASIVO', 'true').lower() == 'true',
        timeout=int(os.getenv('FTP_TIMEOUT', '30'))
    )
    
    # Subir
    uploader = FTPUploader(ftp_config)
    resultado = uploader.subir(carpeta_salida, logger)
    
    # Resumen si creamos el logger
    if crear_logger:
        logger.resumen_final()
    
    return resultado


# =============================================================================
# EJECUCIÓN INDEPENDIENTE
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("SUBIDA FTP - CPQ Migración")
    print("=" * 60)
    print("")
    
    carpeta = os.getenv('CARPETA_SALIDA', 'salida')
    
    if not os.path.exists(carpeta):
        print(f"[ERROR] Carpeta {carpeta} no existe")
        exit(1)
    
    archivos = [f for f in os.listdir(carpeta) if f.endswith('.csv')]
    
    if not archivos:
        print(f"[INFO] No hay archivos CSV en {carpeta}/")
        exit(0)
    
    print(f"Archivos encontrados en {carpeta}/:")
    for archivo in archivos:
        print(f"  - {archivo}")
    print("")
    
    respuesta = input("¿Desea subir estos archivos al AS400? (s/N): ")
    
    if respuesta.lower() == 's':
        subir(carpeta)
    else:
        print("Subida cancelada")