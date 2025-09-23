"""Script para procesar archivos aplicando reemplazos de caracteres,
validar estructura de columnas y subirlos por FTP al servidor AS400.
Versión 8: Con soporte para comodín y rechazo de archivos no parametrizados.
"""

import os
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from ftplib import FTP
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv


# =============================================================================
# CARGA DE CONFIGURACIÓN EXTERNA
# =============================================================================

# Cargar variables de entorno desde .env
load_dotenv()


@dataclass
class FTPConfig:
    """Configuración FTP desde variables de entorno"""
    host: str = os.getenv('FTP_HOST', '10.238.60.3')
    user: str = os.getenv('FTP_USER', 'TYJLMO')
    password: str = os.getenv('FTP_PASSWORD', 'TYJLMO')
    carpeta_remota: str = os.getenv('FTP_CARPETA_REMOTA', '/MIGCPQBUC')


@dataclass
class ProcessConfig:
    """Configuración de procesamiento desde variables de entorno"""
    carpeta_origen: str = os.getenv('CARPETA_ORIGEN', 'raw')
    carpeta_destino: str = os.getenv('CARPETA_DESTINO', 'processed')
    carpeta_logs: str = os.getenv('CARPETA_LOGS', 'logs')
    archivo_esquemas: str = os.getenv(
        'ARCHIVO_ESQUEMAS', 'config/esquemas.json')
    archivo_reemplazos: str = os.getenv(
        'ARCHIVO_REEMPLAZOS', 'config/reemplazos.json')
    separador_salida: str = os.getenv('SEPARADOR_SALIDA', '|')

    def cargar_reemplazos(self) -> List[Tuple[str, str]]:
        """Carga los reemplazos desde archivo JSON o usa valores por defecto"""
        try:
            with open(self.archivo_reemplazos, 'r', encoding='utf-8') as f:
                reemplazos_dict = json.load(f)
                return [(k, v) for k, v in reemplazos_dict.items()]
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"[INFO] No se pudo cargar {self.archivo_reemplazos}: {e}")
            print("[INFO] Usando reemplazos por defecto")
            return [
                ("Ñ", "||||"),
                ("ñ", "|||"),
                ("Á", "'A'"),
                ("á", "'a'"),
                ("É", "'E'"),
                ("é", "'e'"),
                ("Í", "'I'"),
                ("í", "'i'"),
                ("Ó", "'O'"),
                ("ó", "'o'"),
                ("Ú", "'U'"),
                ("ú", "'u'"),
                ("Ü", "U"),
                ("ü", "u"),
                ("–", "-")
            ]


# =============================================================================
# SISTEMA DE LOGGING
# =============================================================================

class MigrationLogger:
    """Manejador de logs para el proceso de migración"""

    def __init__(self, carpeta_logs: str):
        self.carpeta_logs = carpeta_logs
        os.makedirs(carpeta_logs, exist_ok=True)

        # Crear nombre de archivo con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.archivo_log = os.path.join(
            carpeta_logs, f"migracion_{timestamp}.log")

        # Configurar logger
        self.logger = logging.getLogger('MigrationLogger')
        self.logger.setLevel(logging.DEBUG)

        # Handler para archivo
        file_handler = logging.FileHandler(self.archivo_log, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)

        # Handler para consola
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Formato simple (el formato visual lo manejamos nosotros)
        formatter = logging.Formatter('%(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # Listas para el resumen
        self.archivos_procesados = []
        self.archivos_con_error = []
        self.archivos_ftp_ok = []
        self.archivos_ftp_error = []

    def linea_separadora(self, char='=', length=80):
        """Genera una línea separadora"""
        return char * length

    def log(self, mensaje: str):
        """Log simple"""
        self.logger.info(mensaje)

    def inicio_proceso(self):
        """Log de inicio del proceso completo"""
        self.log(self.linea_separadora('='))
        self.log(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INICIO PROCESO DE MIGRACIÓN")
        self.log(self.linea_separadora('='))

    def inicio_archivo(self, nombre_archivo: str):
        """Log de inicio de procesamiento de archivo"""
        self.log("")
        self.log(self.linea_separadora('-'))
        self.log(f"ARCHIVO: {nombre_archivo}")
        self.log(f"INICIO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(self.linea_separadora('-'))

    def fin_archivo(self, nombre_archivo: str, exitoso: bool):
        """Log de fin de procesamiento de archivo"""
        if exitoso:
            self.log("\n[RESULTADO] ARCHIVO PROCESADO EXITOSAMENTE")
            self.archivos_procesados.append(nombre_archivo)
        else:
            self.log("\n[RESULTADO] ARCHIVO RECHAZADO - No procesado")
            self.archivos_con_error.append(nombre_archivo)

        self.log(self.linea_separadora('-'))
        self.log(
            f"FIN: {nombre_archivo} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(self.linea_separadora('-'))

    def error_validacion(self, esperadas: int, encontradas: int, diferencias: List[str]):
        """Log de errores de validación"""
        self.log("[ERROR] Validación de columnas fallida")
        self.log(f"  - Columnas esperadas: {esperadas}")
        self.log(f"  - Columnas encontradas: {encontradas}")

        if diferencias:
            self.log("\n[ERROR] Diferencias encontradas:")
            for diff in diferencias[:10]:  # Máximo 10 diferencias
                self.log(f"  - {diff}")
            if len(diferencias) > 10:
                self.log(f"  ... y {len(diferencias) - 10} diferencias más")

    def inicio_ftp(self, config: FTPConfig):
        """Log de inicio de proceso FTP"""
        self.log("")
        self.log(self.linea_separadora('='))
        self.log("PROCESO FTP - SUBIDA AL SERVIDOR AS400")
        self.log(self.linea_separadora('='))
        self.log(f"INICIO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(f"Host: {config.host}")
        self.log(f"Usuario: {config.user}")
        self.log(f"Carpeta remota: {config.carpeta_remota}")
        self.log(self.linea_separadora('-'))

    def fin_ftp(self, exitoso: bool):
        """Log de fin de proceso FTP"""
        if exitoso:
            self.log(f"\n[FTP] Resumen de subida:")
            self.log(
                f"  - Total archivos: {len(self.archivos_ftp_ok) + len(self.archivos_ftp_error)}")
            self.log(f"  - Exitosos: {len(self.archivos_ftp_ok)}")
            self.log(f"  - Fallidos: {len(self.archivos_ftp_error)}")
            self.log("\n[FTP] Conexión cerrada correctamente")
        else:
            self.log(
                "\n[RESULTADO] SUBIDA FTP FALLIDA - Archivos no subidos al servidor")
            self.log("  Los archivos procesados están disponibles en: processed/")

        self.log(self.linea_separadora('-'))
        self.log(
            f"FIN PROCESO FTP: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(self.linea_separadora('='))

    def resumen_final(self, separador_entrada: str, separador_salida: str):
        """Log del resumen final"""
        self.log("")
        self.log(self.linea_separadora('='))
        self.log(
            f"RESUMEN FINAL - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(self.linea_separadora('='))

        self.log("PROCESAMIENTO LOCAL:")
        total = len(self.archivos_procesados) + len(self.archivos_con_error)
        self.log(f"  Total archivos encontrados: {total}")

        self.log(f"  Procesados exitosamente: {len(self.archivos_procesados)}")
        for archivo in self.archivos_procesados:
            self.log(f"    ✓ {archivo}")

        self.log(
            f"  Con errores (no procesados): {len(self.archivos_con_error)}")
        for archivo in self.archivos_con_error:
            self.log(f"    ✗ {archivo} - Error en validación de columnas")

        self.log("\nSUBIDA FTP:")
        self.log(f"  Archivos subidos: {len(self.archivos_ftp_ok)}")
        self.log(f"  Errores de subida: {len(self.archivos_ftp_error)}")

        self.log(f"\nSeparador entrada usado: {repr(separador_entrada)}")
        self.log(f"Separador salida usado: {repr(separador_salida)}")

        self.log(f"\nLog guardado en: {self.archivo_log}")
        self.log(self.linea_separadora('='))


# =============================================================================
# INTERFACES Y CLASES BASE
# =============================================================================

class IValidator(ABC):
    """Interface para validadores"""
    @abstractmethod
    def validar(self, archivo: str, contenido: str, logger: MigrationLogger) -> Tuple[bool, Optional[str]]:
        """Valida el contenido del archivo.
        Returns: (es_valido, mensaje_error)
        """
        pass


class IProcessor(ABC):
    """Interface para procesadores de texto"""
    @abstractmethod
    def procesar(self, contenido: str, separador_entrada: str, separador_salida: str) -> Tuple[str, int]:
        """Procesa el contenido del archivo.
        Returns: (contenido_procesado, numero_de_reemplazos)
        """
        pass


class IFileHandler(ABC):
    """Interface para manejadores de archivos"""
    @abstractmethod
    def leer(self, ruta: str, logger: MigrationLogger) -> str:
        """Lee un archivo"""
        pass

    @abstractmethod
    def escribir(self, ruta: str, contenido: str) -> None:
        """Escribe un archivo"""
        pass


class IUploader(ABC):
    """Interface para cargadores"""
    @abstractmethod
    def subir(self, carpeta_local: str, logger: MigrationLogger) -> bool:
        """Sube archivos a destino.
        Returns: True si exitoso, False si falló
        """
        pass


# =============================================================================
# IMPLEMENTACIONES
# =============================================================================

class ColumnValidator(IValidator):
    """Validador de columnas para archivos CSV/delimitados con soporte para comodín"""

    def __init__(self, archivo_esquemas: str, separador: str = '|'):
        self.separador = separador
        self.esquemas = self._cargar_esquemas(archivo_esquemas)

    def _cargar_esquemas(self, archivo_esquemas: str) -> Dict[str, List[str]]:
        """Carga esquemas desde archivo JSON"""
        try:
            with open(archivo_esquemas, 'r', encoding='utf-8') as f:
                esquemas = json.load(f)
                print(f"[CONFIG] Esquemas cargados desde {archivo_esquemas}")
                print(
                    f"[CONFIG] Tipos de archivo configurados: {', '.join(esquemas.keys())}")
                return esquemas
        except FileNotFoundError:
            print(f"[WARN] Archivo {archivo_esquemas} no encontrado")
            print("[WARN] Continuando sin validación de columnas")
            return {}
        except json.JSONDecodeError as e:
            print(f"[ERROR] Error al parsear {archivo_esquemas}: {e}")
            print("[WARN] Continuando sin validación de columnas")
            return {}

    def obtener_tipo_archivo(self, nombre_archivo: str) -> Optional[str]:
        """Extrae el tipo de archivo del nombre (sin extensión)"""
        nombre_base = Path(nombre_archivo).stem
        for tipo in self.esquemas.keys():
            if nombre_base.startswith(tipo):
                return tipo
        return None

    def validar(self, archivo: str, contenido: str, logger: MigrationLogger) -> Tuple[bool, Optional[str]]:
        """Valida que las columnas del archivo coincidan con el esquema esperado.
        Soporta comodín '*' para permitir columnas adicionales al final.
        """
        if not self.esquemas:
            logger.log(
                "[INFO] No hay esquemas definidos - Continuando sin validación")
            return True, None

        tipo_archivo = self.obtener_tipo_archivo(archivo)

        # Rechazar archivos no parametrizados
        if tipo_archivo is None:
            logger.log("[ERROR] Archivo no parametrizado en esquemas")
            logger.log(
                f"  - No existe configuración para archivos tipo '{Path(archivo).stem}'")
            return False, "Archivo no configurado en esquemas"

        columnas_esquema = self.esquemas.get(tipo_archivo)
        if not columnas_esquema:
            return True, None

        # Verificar integridad del contenido (no caracteres corruptos)
        caracteres_corruptos = ['�', '\ufffd', 'ï¿½',
                                'Ã­', 'Ã±', 'Ã¡', 'Ã©', 'Ã³', 'Ãº']

        for caracter in caracteres_corruptos:
            if caracter in contenido:
                logger.log("[ERROR] Archivo contiene caracteres corruptos")
                logger.log(f"  - Se detectó: '{caracter}' en el contenido")
                return False, "Archivo corrupto - debe ser regenerado"

        # Detectar si hay comodín
        tiene_comodin = False
        columnas_obligatorias = columnas_esquema

        if "*" in columnas_esquema:
            tiene_comodin = True
            indice_comodin = columnas_esquema.index("*")
            columnas_obligatorias = columnas_esquema[:indice_comodin]

        try:
            lineas = contenido.strip().split('\n')
            if not lineas:
                logger.log("[ERROR] Archivo vacío")
                return False, "Archivo vacío"

            # Primera línea = headers
            headers = lineas[0].split(self.separador)
            headers = [h.strip() for h in headers]

            logger.log("[OK] Validación de columnas iniciada")

            cantidad_obligatorias = len(columnas_obligatorias)
            cantidad_encontrada = len(headers)

            # CAMBIO 3: Validación diferenciada con/sin comodín
            if tiene_comodin:
                # Validación CON comodín
                if cantidad_encontrada < cantidad_obligatorias:
                    logger.log(f"\n[ERROR] Columnas insuficientes")
                    logger.log(
                        f"  - Columnas obligatorias requeridas: {cantidad_obligatorias}")
                    logger.log(
                        f"  - Columnas recibidas: {cantidad_encontrada}")
                    logger.log(
                        f"  - Faltan {cantidad_obligatorias - cantidad_encontrada} columna(s) obligatoria(s)")
                    return False, "Faltan columnas obligatorias"

                # Validar solo las columnas obligatorias
                diferencias = []
                es_desplazamiento = False
                primera_diferencia = -1

                for i in range(cantidad_obligatorias):
                    esperada = columnas_obligatorias[i]
                    encontrada = headers[i] if i < cantidad_encontrada else ""

                    # Comparar primeros 6 caracteres después de trim
                    esperada_6 = esperada[:6] if len(
                        esperada) >= 6 else esperada
                    encontrada_6 = encontrada[:6] if len(
                        encontrada) >= 6 else encontrada

                    if esperada_6 != encontrada_6:
                        if primera_diferencia == -1:
                            primera_diferencia = i
                            # Verificar si es un desplazamiento
                            if i + 1 < cantidad_encontrada:
                                siguiente = headers[i + 1]
                                siguiente_6 = siguiente[:6] if len(
                                    siguiente) >= 6 else siguiente
                                if esperada_6 == siguiente_6:
                                    es_desplazamiento = True

                        diferencias.append(
                            f"Posición {i+1}: esperaba '{esperada}', encontró '{encontrada}'")

                if diferencias:
                    logger.log(
                        f"\n[ERROR] Error en columnas obligatorias (1-{cantidad_obligatorias})")

                    if es_desplazamiento:
                        columna_problema = headers[primera_diferencia] if primera_diferencia < cantidad_encontrada else ""
                        mensaje_vacio = " (columna vacía)" if columna_problema == "" else ""
                        logger.log(
                            f"  - Posición {primera_diferencia + 1}: esperaba '{columnas_obligatorias[primera_diferencia]}', encontró '{columna_problema}'{mensaje_vacio}")
                        logger.log(
                            f"  - Desplazamiento detectado desde posición {primera_diferencia + 1}")
                    else:
                        # Mostrar solo primeros errores
                        for diff in diferencias[:5]:
                            logger.log(f"  - {diff}")
                        if len(diferencias) > 5:
                            logger.log(
                                f"  ... y {len(diferencias) - 5} diferencia(s) más")

                    # CAMBIO 4: Columnas adicionales son INFO, no ERROR
                    if cantidad_encontrada > cantidad_obligatorias:
                        logger.log(
                            f"\n[INFO] Columnas adicionales ({cantidad_obligatorias + 1}-{cantidad_encontrada}): Permitidas por configuración")

                    return False, "Error en columnas obligatorias"

                # Si llegamos aquí, las columnas obligatorias están bien
                logger.log("[OK] Validación de columnas exitosa")
                logger.log(
                    f"  - Columnas obligatorias (1-{cantidad_obligatorias}): Validadas correctamente")
                if cantidad_encontrada > cantidad_obligatorias:
                    logger.log(
                        f"  - Columnas adicionales ({cantidad_obligatorias + 1}-{cantidad_encontrada}): Permitidas por configuración")

                return True, None

            # Validación SIN comodín (estricta)
            else:
                if cantidad_encontrada != cantidad_obligatorias:
                    logger.log(f"\n[ERROR] Validación de columnas fallida")
                    logger.log(
                        f"  - Columnas esperadas: {cantidad_obligatorias}")
                    logger.log(
                        f"  - Columnas encontradas: {cantidad_encontrada}")

                    if cantidad_encontrada < cantidad_obligatorias:
                        logger.log(
                            f"  - Faltan {cantidad_obligatorias - cantidad_encontrada} columna(s)")
                    else:
                        # Detectar tipo de problema
                        es_desplazamiento = False
                        primera_diferencia = -1

                        for i in range(min(cantidad_obligatorias, cantidad_encontrada)):
                            esperada = columnas_obligatorias[i]
                            encontrada = headers[i]

                            esperada_6 = esperada[:6] if len(
                                esperada) >= 6 else esperada
                            encontrada_6 = encontrada[:6] if len(
                                encontrada) >= 6 else encontrada

                            if esperada_6 != encontrada_6:
                                if primera_diferencia == -1:
                                    primera_diferencia = i
                                    if i + 1 < cantidad_encontrada:
                                        siguiente = headers[i + 1]
                                        siguiente_6 = siguiente[:6] if len(
                                            siguiente) >= 6 else siguiente
                                        if esperada_6 == siguiente_6:
                                            es_desplazamiento = True
                                break

                        if es_desplazamiento:
                            logger.log(
                                f"\n[ERROR] Estructura incorrecta - Columna extra detectada")
                            columna_problema = headers[primera_diferencia]
                            mensaje_vacio = " (columna vacía)" if columna_problema == "" else ""
                            logger.log(
                                f"  - Posición {primera_diferencia + 1}: esperaba '{columnas_obligatorias[primera_diferencia]}', encontró '{columna_problema}'{mensaje_vacio}")
                            logger.log(
                                f"  - Desplazamiento detectado desde posición {primera_diferencia + 1}")

                        # Columnas no permitidas (sin comodín)
                        logger.log(f"\n[ERROR] Columnas no permitidas:")
                        for i in range(cantidad_obligatorias, cantidad_encontrada):
                            logger.log(
                                f"  - Posición {i+1}: '{headers[i]}' (columna extra)")

                    return False, "Cantidad de columnas incorrecta"

                # Validar nombres si la cantidad es correcta
                diferencias = []
                for i, (esperada, encontrada) in enumerate(zip(columnas_obligatorias, headers)):
                    esperada_6 = esperada[:6] if len(
                        esperada) >= 6 else esperada
                    encontrada_6 = encontrada[:6] if len(
                        encontrada) >= 6 else encontrada

                    if esperada_6 != encontrada_6:
                        diferencias.append(
                            f"Posición {i+1}: esperaba '{esperada}', encontró '{encontrada}'")

                if diferencias:
                    logger.log(f"\n[ERROR] Errores detectados:")
                    for diff in diferencias[:10]:
                        logger.log(f"  - {diff}")
                    if len(diferencias) > 10:
                        logger.log(
                            f"  ... y {len(diferencias) - 10} diferencia(s) más")
                    return False, "Columnas no coinciden"

                logger.log("[OK] Validación de columnas exitosa")
                return True, None

        except Exception as e:
            logger.log(f"[ERROR] Error durante validación: {str(e)}")
            return False, f"Error validando: {str(e)}"


class CharacterProcessor(IProcessor):
    """Procesador de caracteres especiales y separadores"""

    def __init__(self, reemplazos: List[Tuple[str, str]]):
        self.reemplazos = reemplazos
        print(
            f"[CONFIG] Cargados {len(self.reemplazos)} reemplazos de caracteres")

    def procesar(self, contenido: str, separador_entrada: str, separador_salida: str) -> Tuple[str, int]:
        """Aplica reemplazos y cambia el separador si es necesario"""
        contador_reemplazos = 0

        # Elimina BOM si está presente
        if contenido.startswith('\ufeff'):
            contenido = contenido.replace('\ufeff', '')

        # Aplica reemplazos de caracteres
        for original, nuevo in self.reemplazos:
            ocurrencias = contenido.count(original)
            if ocurrencias > 0:
                contenido = contenido.replace(original, nuevo)
                contador_reemplazos += ocurrencias

        # Cambia el separador si es diferente
        if separador_entrada != separador_salida:
            contenido = contenido.replace(separador_entrada, separador_salida)

        return contenido, contador_reemplazos


class SmartFileHandler(IFileHandler):
    """Manejador inteligente de archivos con detección automática de encoding"""

    def leer(self, ruta: str, logger: MigrationLogger) -> str:
        """Lee un archivo detectando automáticamente su encoding"""
        nombre_archivo = os.path.basename(ruta)

        with open(ruta, 'rb') as f:
            contenido_raw = f.read()

        if not contenido_raw:
            return ""

        # Probar Windows-1252 primero (más común para CSVs de Excel)
        encodings_a_probar = [
            'windows-1252',   # Primero Windows (más común para CSVs de Excel)
            'cp1252',         # Alias de windows-1252
            'iso-8859-1',     # Latin-1
            'latin-1',        # Alias de iso-8859-1
            'utf-8-sig',      # UTF-8 con BOM
            'utf-8',          # UTF-8 estándar
            'cp850',          # DOS Latin-1
            'cp437',          # DOS US
        ]

        mejor_encoding = None
        mejor_contenido = None

        for encoding in encodings_a_probar:
            try:
                contenido = contenido_raw.decode(encoding)

                # Rechazar si hay caracteres de reemplazo (indica mal encoding)
                if '�' in contenido:
                    continue

                # Buscar caracteres españoles como validación
                caracteres_espanol = 'ÑñáéíóúÁÉÍÓÚüÜ'

                if any(c in contenido for c in caracteres_espanol):
                    logger.log(f"[ENCODING] Detectado como {encoding} ✓")
                    return contenido

                # Guardar el primer encoding que funcione sin errores
                if mejor_encoding is None and ('\n' in contenido or '\r' in contenido):
                    mejor_encoding = encoding
                    mejor_contenido = contenido

            except (UnicodeDecodeError, AttributeError):
                continue

        # Si encontramos un encoding que funcionó, usarlo
        if mejor_contenido:
            logger.log(f"[ENCODING] Usando {mejor_encoding}")
            return mejor_contenido

        # Fallback: forzar Windows-1252 (más común para archivos de Excel en español)
        logger.log(
            f"[WARN] No se pudo detectar encoding, forzando windows-1252")
        return contenido_raw.decode('windows-1252', errors='replace')

    def escribir(self, ruta: str, contenido: str) -> None:
        """Escribe siempre en UTF-8"""
        with open(ruta, 'w', encoding='utf-8') as f:
            f.write(contenido)


class FTPUploader(IUploader):
    """Cargador FTP para AS400"""

    def __init__(self, config: FTPConfig):
        self.config = config

    def _limpiar_remoto(self, ftp: FTP, logger: MigrationLogger) -> None:
        """Limpia la carpeta remota"""
        logger.log("\n[FTP] Limpiando carpeta remota...")
        try:
            archivos_remotos = ftp.nlst()
            for remoto in archivos_remotos:
                try:
                    ftp.delete(remoto)
                    logger.log(f"  - Eliminado: {remoto}")
                except Exception as exc:
                    logger.log(f"  - Error eliminando {remoto}: {exc}")
            logger.log("[FTP] Carpeta remota limpia")
        except Exception as exc:
            logger.log(f"[FTP WARN] No se pudo limpiar carpeta remota: {exc}")

    def subir(self, carpeta_local: str, logger: MigrationLogger) -> bool:
        """Sube archivos al servidor FTP"""
        logger.inicio_ftp(self.config)

        try:
            logger.log("\n[FTP] Conectando al servidor...")
            ftp = FTP(self.config.host)
            ftp.login(self.config.user, self.config.password)
            logger.log("[FTP] Conexión establecida exitosamente")

            ftp.cwd(self.config.carpeta_remota)
            ftp.set_pasv(True)
            logger.log("[FTP] Modo PASV activado")

            self._limpiar_remoto(ftp, logger)

            archivos = [f for f in os.listdir(carpeta_local)
                        if os.path.isfile(os.path.join(carpeta_local, f))]

            logger.log(f"\n[FTP] Subiendo archivos procesados...")
            for archivo in archivos:
                ruta_archivo = os.path.join(carpeta_local, archivo)

                try:
                    size = os.path.getsize(ruta_archivo)
                    size_mb = size / (1024 * 1024)

                    with open(ruta_archivo, 'rb') as f:
                        ftp.storlines(f"STOR {archivo}", f)

                    if size_mb > 1:
                        logger.log(
                            f"  ✓ {archivo} - Subido exitosamente ({size_mb:.1f} MB)")
                    else:
                        size_kb = size / 1024
                        logger.log(
                            f"  ✓ {archivo} - Subido exitosamente ({size_kb:.0f} KB)")

                    logger.archivos_ftp_ok.append(archivo)

                except Exception as exc:
                    logger.log(f"  ✗ {archivo} - Error: {exc}")
                    logger.archivos_ftp_error.append(archivo)

            ftp.quit()
            logger.fin_ftp(True)
            return True

        except Exception as e:
            logger.log(f"\n[FTP ERROR] No se pudo conectar al servidor")
            logger.log(f"  Detalles: {str(e)}")
            logger.fin_ftp(False)
            return False


class DummyUploader(IUploader):
    """Uploader de prueba que no sube nada"""

    def subir(self, carpeta_local: str, logger: MigrationLogger) -> bool:
        """Simula subir archivos sin conexión real"""
        logger.log("\n" + "=" * 80)
        logger.log("MODO DUMMY FTP - SIMULACIÓN")
        logger.log("=" * 80)

        archivos = [f for f in os.listdir(carpeta_local)
                    if os.path.isfile(os.path.join(carpeta_local, f))]

        logger.log(f"[DUMMY FTP] Simulando subida de {len(archivos)} archivos")
        for archivo in archivos:
            logger.log(f"  ✓ {archivo} - Subido (simulado)")
            logger.archivos_ftp_ok.append(archivo)

        logger.log("[DUMMY FTP] Simulación completada")
        return True


# =============================================================================
# ORQUESTADOR
# =============================================================================

class MigrationOrchestrator:
    """Orquesta el proceso completo de migración"""

    def __init__(self,
                 process_config: ProcessConfig,
                 validator: IValidator,
                 processor: IProcessor,
                 file_handler: IFileHandler,
                 uploader: IUploader,
                 logger: MigrationLogger):
        self.process_config = process_config
        self.validator = validator
        self.processor = processor
        self.file_handler = file_handler
        self.uploader = uploader
        self.logger = logger
        self.separador_entrada = '|'  # Se actualizará con input del usuario

    def solicitar_separador(self) -> str:
        """Solicita al usuario el separador de los archivos de entrada"""
        print("\n" + "=" * 60)
        print("CONFIGURACIÓN DE SEPARADOR")
        print("=" * 60)

        separadores_comunes = {
            '1': '|',
            '2': ';',
            '3': ',',
            '4': '\t',
            '5': 'otro'
        }

        print("¿Cuál es el separador de los archivos de entrada?")
        print("  1) Pipe (|)")
        print("  2) Punto y coma (;)")
        print("  3) Coma (,)")
        print("  4) Tabulador (\\t)")
        print("  5) Otro")

        while True:
            opcion = input("\nSeleccione una opción (1-5): ").strip()

            if opcion in separadores_comunes:
                if opcion == '5':
                    separador = input("Ingrese el separador personalizado: ")
                    if separador:
                        print(f"Separador seleccionado: '{separador}'")
                        return separador
                else:
                    separador = separadores_comunes[opcion]
                    nombre = {
                        '|': 'Pipe (|)',
                        ';': 'Punto y coma (;)',
                        ',': 'Coma (,)',
                        '\t': 'Tabulador'
                    }.get(separador, separador)
                    print(f"Separador seleccionado: {nombre}")
                    return separador
            else:
                print("Opción inválida. Por favor seleccione 1-5.")

    def limpiar_directorio(self, directorio: str) -> None:
        """Limpia o crea un directorio"""
        if os.path.exists(directorio):
            for archivo in os.listdir(directorio):
                ruta = os.path.join(directorio, archivo)
                if os.path.isfile(ruta):
                    os.remove(ruta)
            self.logger.log(f"[CLEAN] Directorio limpio: {directorio}")
        else:
            os.makedirs(directorio)
            self.logger.log(f"[CLEAN] Directorio creado: {directorio}")

    def procesar_archivos(self) -> None:
        """Procesa todos los archivos del directorio origen"""
        origen = self.process_config.carpeta_origen
        destino = self.process_config.carpeta_destino

        if not os.path.exists(origen):
            self.logger.log(f"[ERROR] La carpeta origen '{origen}' no existe")
            return

        os.makedirs(destino, exist_ok=True)
        archivos = os.listdir(origen)

        if not archivos:
            self.logger.log(f"[WARN] No hay archivos en '{origen}'")
            return

        self.logger.log(
            f"\n[PROCESO] Encontrados {len(archivos)} archivos para procesar")

        # Actualizar el validador con el separador correcto
        self.validator.separador = self.separador_entrada

        for archivo in archivos:
            ruta_origen = os.path.join(origen, archivo)
            ruta_destino = os.path.join(destino, archivo)

            if os.path.isfile(ruta_origen):
                self.logger.inicio_archivo(archivo)
                exitoso = False

                try:
                    # Lee el archivo
                    contenido = self.file_handler.leer(
                        ruta_origen, self.logger)

                    if not contenido.strip():
                        self.logger.log("[WARN] Archivo vacío o ilegible")
                        self.logger.fin_archivo(archivo, False)
                        continue

                    # Valida estructura de columnas
                    es_valido, mensaje_error = self.validator.validar(
                        archivo, contenido, self.logger)
                    if not es_valido:
                        self.logger.fin_archivo(archivo, False)
                        continue

                    # Procesa el contenido
                    contenido_modificado, num_reemplazos = self.processor.procesar(
                        contenido,
                        self.separador_entrada,
                        self.process_config.separador_salida
                    )

                    self.logger.log("[OK] Reemplazo de caracteres completado")
                    if num_reemplazos > 0:
                        self.logger.log(
                            f"  - Caracteres especiales procesados: {num_reemplazos} reemplazos")

                    # Escribe el resultado
                    self.file_handler.escribir(
                        ruta_destino, contenido_modificado)
                    self.logger.log(
                        f"[OK] Archivo guardado en {destino}/{archivo}")

                    exitoso = True

                except Exception as exc:
                    self.logger.log(f"[ERROR] Error procesando archivo: {exc}")

                self.logger.fin_archivo(archivo, exitoso)

    def ejecutar(self, skip_ftp: bool = False) -> None:
        """Ejecuta el proceso completo de migración"""
        self.logger.inicio_proceso()

        # Solicitar separador al usuario
        self.separador_entrada = self.solicitar_separador()

        print("\n[CONFIG] Configuración del proceso:")
        print(f"  - Carpeta origen: {self.process_config.carpeta_origen}")
        print(f"  - Carpeta destino: {self.process_config.carpeta_destino}")
        print(f"  - Separador entrada: {repr(self.separador_entrada)}")
        print(
            f"  - Separador salida: {repr(self.process_config.separador_salida)}")
        print()

        # 1. Limpia directorio destino
        self.limpiar_directorio(self.process_config.carpeta_destino)

        # 2. Procesa archivos
        self.logger.log("\n[PROCESO] Validando y procesando archivos...")
        self.procesar_archivos()

        # 3. Sube por FTP (opcional)
        if not skip_ftp and self.logger.archivos_procesados:
            self.uploader.subir(
                self.process_config.carpeta_destino, self.logger)
        elif skip_ftp:
            self.logger.log("\n[INFO] Subida FTP omitida (modo prueba)")
        elif not self.logger.archivos_procesados:
            self.logger.log("\n[INFO] No hay archivos para subir por FTP")

        # 4. Resumen final
        self.logger.resumen_final(
            self.separador_entrada, self.process_config.separador_salida)


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

def main():
    """Función principal - configura e inicia el proceso"""

    # Verificar si estamos en modo desarrollo
    modo_desarrollo = os.getenv('MODO_DESARROLLO', 'false').lower() == 'true'
    skip_ftp = os.getenv('SKIP_FTP', 'false').lower() == 'true'

    if modo_desarrollo:
        print("[MODO] Ejecutando en modo desarrollo")

    # Configuraciones
    process_config = ProcessConfig()
    ftp_config = FTPConfig()

    # Crear logger
    logger = MigrationLogger(process_config.carpeta_logs)

    # Cargar reemplazos desde archivo o usar por defecto
    reemplazos = process_config.cargar_reemplazos()

    # Crear implementaciones (Dependency Injection)
    validator = ColumnValidator(
        archivo_esquemas=process_config.archivo_esquemas,
        separador='|'  # Se actualizará con input del usuario
    )
    processor = CharacterProcessor(reemplazos)
    file_handler = SmartFileHandler()

    # Seleccionar uploader según modo
    if modo_desarrollo or skip_ftp:
        uploader = DummyUploader()
    else:
        uploader = FTPUploader(ftp_config)

    # Crear y ejecutar orquestador
    orchestrator = MigrationOrchestrator(
        process_config=process_config,
        validator=validator,
        processor=processor,
        file_handler=file_handler,
        uploader=uploader,
        logger=logger
    )

    try:
        orchestrator.ejecutar(skip_ftp=skip_ftp)
    except KeyboardInterrupt:
        print("\n\n[CANCELADO] Proceso interrumpido por el usuario")
        logger.log("\n[CANCELADO] Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"\n[ERROR FATAL] {str(e)}")
        logger.log(f"\n[ERROR FATAL] {str(e)}")
        raise


if __name__ == "__main__":
    main()
