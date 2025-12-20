"""Script para procesar archivos aplicando reemplazos de caracteres,
validar estructura de columnas y subirlos por FTP al servidor AS400.

Versión 11: Renombrado por posición, fechas numéricas, CONSERVAR_ENTRADA.
- entrada_xlsx/  → Usuario coloca archivos .xlsx aquí
- entrada_csv/   → Usuario coloca archivos .csv aquí
- salida/        → Archivos procesados listos para FTP
- logs/          → Registro detallado de cada ejecución
"""

import os
import json
import logging
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional, Any
from ftplib import FTP
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Para conversión de Excel
import pandas as pd


# =============================================================================
# CARGA DE CONFIGURACIÓN EXTERNA
# =============================================================================

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
    carpeta_entrada_xlsx: str = os.getenv('CARPETA_ENTRADA_XLSX', 'entrada_xlsx')
    carpeta_entrada_csv: str = os.getenv('CARPETA_ENTRADA_CSV', 'entrada_csv')
    carpeta_salida: str = os.getenv('CARPETA_SALIDA', 'salida')
    carpeta_logs: str = os.getenv('CARPETA_LOGS', 'logs')
    archivo_esquemas: str = os.getenv('ARCHIVO_ESQUEMAS', 'config/esquemas.json')
    archivo_reemplazos: str = os.getenv('ARCHIVO_REEMPLAZOS', 'config/reemplazos.json')
    separador_salida: str = os.getenv('SEPARADOR_SALIDA', '|')
    conservar_entrada: bool = os.getenv('CONSERVAR_ENTRADA', 'false').lower() == 'true'

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
# CARGADOR DE ESQUEMAS
# =============================================================================

class SchemaLoader:
    """Carga y gestiona esquemas de archivos"""

    def __init__(self, archivo_esquemas: str):
        self.archivo_esquemas = archivo_esquemas
        self.esquemas = self._cargar_esquemas()

    def _cargar_esquemas(self) -> Dict[str, Any]:
        """Carga esquemas desde archivo JSON"""
        try:
            with open(self.archivo_esquemas, 'r', encoding='utf-8') as f:
                esquemas = json.load(f)
                print(f"[CONFIG] Esquemas cargados desde {self.archivo_esquemas}")
                print(f"[CONFIG] Tipos de archivo configurados: {', '.join(esquemas.keys())}")
                return esquemas
        except FileNotFoundError:
            print(f"[WARN] Archivo {self.archivo_esquemas} no encontrado")
            print("[WARN] Continuando sin validación de columnas")
            return {}
        except json.JSONDecodeError as e:
            print(f"[ERROR] Error al parsear {self.archivo_esquemas}: {e}")
            print("[WARN] Continuando sin validación de columnas")
            return {}

    def obtener_tipo_archivo(self, nombre_archivo: str) -> Optional[str]:
        """Determina el tipo de archivo basado en el nombre"""
        nombre_base = Path(nombre_archivo).stem
        for tipo in self.esquemas.keys():
            if nombre_base.startswith(tipo):
                return tipo
        return None

    def obtener_columnas(self, tipo_archivo: str) -> List[str]:
        """Obtiene lista de columnas para un tipo de archivo"""
        if tipo_archivo not in self.esquemas:
            return []

        esquema = self.esquemas[tipo_archivo]

        # Soportar formato nuevo (objeto) y viejo (array)
        if isinstance(esquema, dict):
            return esquema.get('columnas', [])
        elif isinstance(esquema, list):
            return esquema
        return []

    def obtener_fechas_numericas(self, tipo_archivo: str) -> List[str]:
        """Obtiene lista de columnas que son fechas numéricas"""
        if tipo_archivo not in self.esquemas:
            return []

        esquema = self.esquemas[tipo_archivo]

        if isinstance(esquema, dict):
            return esquema.get('fechas_numericas', [])
        return []

    def tiene_comodin(self, tipo_archivo: str) -> bool:
        """Verifica si el esquema tiene comodín (*)"""
        columnas = self.obtener_columnas(tipo_archivo)
        return '*' in columnas

    def columnas_obligatorias(self, tipo_archivo: str) -> List[str]:
        """Obtiene columnas obligatorias (sin el comodín)"""
        columnas = self.obtener_columnas(tipo_archivo)
        if '*' in columnas:
            return columnas[:columnas.index('*')]
        return columnas

    def obtener_filas_omitir(self, tipo_archivo: str) -> List[int]:
        """Obtiene lista de filas a omitir (numeración desde 1)"""
        if tipo_archivo not in self.esquemas:
            return []

        esquema = self.esquemas[tipo_archivo]

        if isinstance(esquema, dict):
            return esquema.get('filas_omitir', [])
        return []


# =============================================================================
# SISTEMA DE LOGGING
# =============================================================================

class MigrationLogger:
    """Manejador de logs para el proceso de migración"""

    def __init__(self, carpeta_logs: str):
        self.carpeta_logs = carpeta_logs
        os.makedirs(carpeta_logs, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.archivo_log = os.path.join(carpeta_logs, f"migracion_{timestamp}.log")

        self.logger = logging.getLogger('MigrationLogger')
        self.logger.setLevel(logging.DEBUG)

        # Limpiar handlers anteriores
        self.logger.handlers = []

        file_handler = logging.FileHandler(self.archivo_log, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # Listas para el resumen
        self.archivos_convertidos = []
        self.archivos_conversion_error = []
        self.archivos_procesados = []
        self.archivos_con_error = []
        self.archivos_ftp_ok = []
        self.archivos_ftp_error = []

    def linea_separadora(self, char='=', length=80):
        return char * length

    def log(self, mensaje: str):
        self.logger.info(mensaje)

    def inicio_proceso(self):
        self.log(self.linea_separadora('='))
        self.log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INICIO PROCESO DE MIGRACIÓN")
        self.log(self.linea_separadora('='))

    def inicio_conversion(self):
        self.log("")
        self.log(self.linea_separadora('-'))
        self.log("FASE 1: CONVERSIÓN XLSX → CSV")
        self.log(self.linea_separadora('-'))

    def fin_conversion(self):
        total = len(self.archivos_convertidos) + len(self.archivos_conversion_error)
        self.log("")
        self.log(f"[CONVERSIÓN] Resumen:")
        self.log(f"  - Total archivos .xlsx: {total}")
        self.log(f"  - Convertidos: {len(self.archivos_convertidos)}")
        self.log(f"  - Con errores: {len(self.archivos_conversion_error)}")
        self.log(self.linea_separadora('-'))

    def inicio_procesamiento(self):
        self.log("")
        self.log(self.linea_separadora('-'))
        self.log("FASE 2: VALIDACIÓN Y PROCESAMIENTO CSV")
        self.log(self.linea_separadora('-'))

    def inicio_archivo(self, nombre_archivo: str):
        self.log("")
        self.log(f"ARCHIVO: {nombre_archivo}")
        self.log(f"INICIO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def fin_archivo(self, nombre_archivo: str, exitoso: bool):
        if exitoso:
            self.log("[RESULTADO] ARCHIVO PROCESADO EXITOSAMENTE")
            self.archivos_procesados.append(nombre_archivo)
        else:
            self.log("[RESULTADO] ARCHIVO RECHAZADO - No procesado")
            self.archivos_con_error.append(nombre_archivo)

    def error_validacion(self, esperadas: int, encontradas: int, diferencias: List[str]):
        self.log("[ERROR] Validación de columnas fallida")
        self.log(f"  - Columnas esperadas: {esperadas}")
        self.log(f"  - Columnas encontradas: {encontradas}")

        if diferencias:
            self.log("\n[ERROR] Diferencias encontradas:")
            for diff in diferencias[:10]:
                self.log(f"  - {diff}")
            if len(diferencias) > 10:
                self.log(f"  ... y {len(diferencias) - 10} diferencias más")

    def inicio_ftp(self, config: FTPConfig):
        self.log("")
        self.log(self.linea_separadora('='))
        self.log("FASE 3: SUBIDA FTP AL SERVIDOR AS400")
        self.log(self.linea_separadora('='))
        self.log(f"INICIO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(f"Host: {config.host}")
        self.log(f"Usuario: {config.user}")
        self.log(f"Carpeta remota: {config.carpeta_remota}")
        self.log(self.linea_separadora('-'))

    def fin_ftp(self, exitoso: bool):
        if exitoso:
            self.log(f"\n[FTP] Resumen de subida:")
            self.log(f"  - Total archivos: {len(self.archivos_ftp_ok) + len(self.archivos_ftp_error)}")
            self.log(f"  - Exitosos: {len(self.archivos_ftp_ok)}")
            self.log(f"  - Fallidos: {len(self.archivos_ftp_error)}")
            self.log("\n[FTP] Conexión cerrada correctamente")
        else:
            self.log("\n[RESULTADO] SUBIDA FTP FALLIDA")
            self.log("  Los archivos procesados están disponibles en: salida/")

        self.log(self.linea_separadora('-'))
        self.log(f"FIN PROCESO FTP: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(self.linea_separadora('='))

    def resumen_final(self, tipo_entrada: str, separador_csv: str, separador_salida: str, conservar_entrada: bool):
        self.log("")
        self.log(self.linea_separadora('='))
        self.log(f"RESUMEN FINAL - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(self.linea_separadora('='))

        self.log(f"\nTipo de entrada: {tipo_entrada}")

        if tipo_entrada == 'xlsx':
            self.log("\nFASE 1 - CONVERSIÓN XLSX → CSV:")
            total_xlsx = len(self.archivos_convertidos) + len(self.archivos_conversion_error)
            self.log(f"  Total archivos .xlsx: {total_xlsx}")
            self.log(f"  Convertidos exitosamente: {len(self.archivos_convertidos)}")
            for archivo in self.archivos_convertidos:
                self.log(f"    ✓ {archivo}")
            self.log(f"  Con errores (permanecen en entrada_xlsx/): {len(self.archivos_conversion_error)}")
            for archivo in self.archivos_conversion_error:
                self.log(f"    ✗ {archivo}")

        self.log("\nFASE 2 - PROCESAMIENTO CSV:")
        total_csv = len(self.archivos_procesados) + len(self.archivos_con_error)
        self.log(f"  Total archivos CSV: {total_csv}")
        self.log(f"  Procesados exitosamente: {len(self.archivos_procesados)}")
        for archivo in self.archivos_procesados:
            self.log(f"    ✓ {archivo}")

        carpeta_error = "entrada_xlsx/" if tipo_entrada == 'xlsx' else "entrada_csv/"
        self.log(f"  Con errores (permanecen en {carpeta_error}): {len(self.archivos_con_error)}")
        for archivo in self.archivos_con_error:
            self.log(f"    ✗ {archivo}")

        self.log("\nFASE 3 - SUBIDA FTP:")
        self.log(f"  Archivos subidos: {len(self.archivos_ftp_ok)}")
        self.log(f"  Errores de subida: {len(self.archivos_ftp_error)}")

        self.log(f"\nSeparador CSV usado: {repr(separador_csv)}")
        self.log(f"Separador salida: {repr(separador_salida)}")
        self.log(f"Conservar entrada: {'Sí' if conservar_entrada else 'No'}")

        self.log(f"\nLog guardado en: {self.archivo_log}")
        self.log(self.linea_separadora('='))


# =============================================================================
# INTERFACES Y CLASES BASE
# =============================================================================

class IValidator(ABC):
    @abstractmethod
    def validar(self, archivo: str, contenido: str, logger: MigrationLogger) -> Tuple[bool, Optional[str]]:
        pass


class IProcessor(ABC):
    @abstractmethod
    def procesar(self, contenido: str, separador_entrada: str, separador_salida: str) -> Tuple[str, int]:
        pass


class IFileHandler(ABC):
    @abstractmethod
    def leer(self, ruta: str, logger: MigrationLogger) -> str:
        pass

    @abstractmethod
    def escribir(self, ruta: str, contenido: str) -> None:
        pass


class IUploader(ABC):
    @abstractmethod
    def subir(self, carpeta_local: str, logger: MigrationLogger) -> bool:
        pass


# =============================================================================
# CONVERSOR XLSX A CSV
# =============================================================================

class ExcelConverter:
    """Convierte archivos Excel (.xlsx) a CSV con renombrado de columnas y fechas numéricas."""

    def __init__(self, schema_loader: SchemaLoader, separador: str = ';'):
        self.schema_loader = schema_loader
        self.separador = separador

    def _formatear_entero(self, valor) -> str:
        """Convierte floats que son enteros a string sin .0"""
        if pd.isna(valor):
            return ''
        
        # Si es float y no tiene decimales reales, quitar .0
        if isinstance(valor, float):
            if valor == int(valor):
                return str(int(valor))
            return str(valor)
        
        return str(valor) if not isinstance(valor, str) else valor

    def _formatear_fecha_numerica(self, valor) -> str:
        """Convierte un valor de fecha a formato YYYYMMDD"""
        if pd.isna(valor):
            return ''
        
        # Si ya es string numérico, retornar como está
        if isinstance(valor, str):
            # Limpiar espacios
            valor = valor.strip()
            # Si ya está en formato numérico (solo dígitos)
            if valor.isdigit() and len(valor) == 8:
                return valor
            # Si tiene formato fecha con separadores, intentar parsear
            try:
                fecha = pd.to_datetime(valor)
                return fecha.strftime('%Y%m%d')
            except:
                return valor
        
        # Si es un número (Excel guarda fechas como números)
        if isinstance(valor, (int, float)):
            # Si es un número de 8 dígitos, probablemente ya es YYYYMMDD
            if 10000101 <= valor <= 99991231:
                return str(int(valor))
            # Si no, intentar convertir desde número de serie de Excel
            try:
                fecha = pd.to_datetime(valor, unit='D', origin='1899-12-30')
                return fecha.strftime('%Y%m%d')
            except:
                return str(int(valor)) if valor == int(valor) else str(valor)
        
        # Si es datetime
        if isinstance(valor, (pd.Timestamp, datetime)):
            return valor.strftime('%Y%m%d')
        
        return str(valor)

    def convertir(self, ruta_xlsx: str, ruta_csv: str, logger: MigrationLogger) -> bool:
        """
        Convierte un archivo XLSX a CSV.
        - Renombra columnas según esquema (por posición)
        - Convierte fechas numéricas a formato YYYYMMDD
        
        Returns:
            True si la conversión fue exitosa
        """
        nombre = os.path.basename(ruta_xlsx)

        try:
            # Obtener tipo de archivo y esquema ANTES de leer
            tipo_archivo = self.schema_loader.obtener_tipo_archivo(nombre)
            filas_omitir = []
            
            if tipo_archivo:
                filas_omitir = self.schema_loader.obtener_filas_omitir(tipo_archivo)
            
            # Leer Excel SIN header (todas las filas como datos)
            df = pd.read_excel(ruta_xlsx, engine='openpyxl', header=None)

            if df.empty:
                logger.log(f"  [ERROR] Archivo vacío: {nombre}")
                return False

            # Eliminar filas a omitir PRIMERO (numeración desde 1, índice desde 0)
            if filas_omitir:
                indices_a_eliminar = [f - 1 for f in filas_omitir]  # Fila 1 = índice 0
                indices_validos = [i for i in indices_a_eliminar if 0 <= i < len(df)]
                
                if indices_validos:
                    df = df.drop(df.index[indices_validos]).reset_index(drop=True)
                    logger.log(f"  [OK] Filas omitidas: {filas_omitir} ({len(indices_validos)} filas eliminadas)")

            # Ahora la primera fila es el encabezado
            df.columns = df.iloc[0]  # Primera fila como nombres de columnas
            df = df.iloc[1:].reset_index(drop=True)  # Resto como datos

            if tipo_archivo:
                columnas_esquema = self.schema_loader.columnas_obligatorias(tipo_archivo)
                fechas_numericas = self.schema_loader.obtener_fechas_numericas(tipo_archivo)
                tiene_comodin = self.schema_loader.tiene_comodin(tipo_archivo)

                # Validar cantidad de columnas
                num_columnas_excel = len(df.columns)
                num_columnas_esquema = len(columnas_esquema)

                if tiene_comodin:
                    if num_columnas_excel < num_columnas_esquema:
                        logger.log(f"  [ERROR] {nombre}: Columnas insuficientes")
                        logger.log(f"          Excel tiene {num_columnas_excel}, esquema requiere mínimo {num_columnas_esquema}")
                        return False
                else:
                    if num_columnas_excel != num_columnas_esquema:
                        logger.log(f"  [ERROR] {nombre}: Cantidad de columnas no coincide")
                        logger.log(f"          Excel tiene {num_columnas_excel}, esquema requiere {num_columnas_esquema}")
                        return False

                # Renombrar columnas por posición
                nuevos_nombres = []
                for i in range(num_columnas_excel):
                    if i < num_columnas_esquema:
                        nuevos_nombres.append(columnas_esquema[i])
                    else:
                        # Columnas extra (después del comodín) - mantener nombre original
                        nuevos_nombres.append(df.columns[i])

                df.columns = nuevos_nombres
                logger.log(f"  [OK] Columnas renombradas según esquema ({num_columnas_esquema} columnas)")

                # Convertir fechas numéricas
                if fechas_numericas:
                    fechas_convertidas = 0
                    for col in fechas_numericas:
                        if col in df.columns:
                            df[col] = df[col].apply(self._formatear_fecha_numerica)
                            fechas_convertidas += 1
                    if fechas_convertidas > 0:
                        logger.log(f"  [OK] Fechas convertidas a YYYYMMDD ({fechas_convertidas} columnas)")

                # Quitar .0 de columnas numéricas (que no son fechas)
                columnas_no_fecha = [col for col in df.columns if col not in fechas_numericas]
                for col in columnas_no_fecha:
                    df[col] = df[col].apply(self._formatear_entero)
                logger.log(f"  [OK] Valores numéricos formateados (sin .0)")

            else:
                logger.log(f"  [WARN] {nombre}: Sin esquema definido, usando nombres originales")
                # Quitar .0 de columnas numéricas aunque no haya esquema
                for col in df.columns:
                    df[col] = df[col].apply(self._formatear_entero)

            # Guardar CSV
            df.to_csv(ruta_csv, sep=self.separador, index=False, encoding='utf-8')

            filas = len(df)
            columnas = len(df.columns)
            logger.log(f"  [OK] {nombre} → {os.path.basename(ruta_csv)} ({filas} filas, {columnas} columnas)")

            return True

        except Exception as e:
            logger.log(f"  [ERROR] {nombre}: {str(e)}")
            return False

    def convertir_carpeta(self, carpeta_entrada: str, carpeta_temporal: str,
                          logger: MigrationLogger) -> Tuple[List[str], List[str]]:
        """
        Convierte todos los archivos XLSX de una carpeta.
        
        Returns:
            Tupla (archivos_csv_generados, archivos_xlsx_exitosos)
        """
        logger.inicio_conversion()

        os.makedirs(carpeta_temporal, exist_ok=True)

        # Ignorar archivos temporales de Excel (~$)
        archivos_xlsx = [f for f in os.listdir(carpeta_entrada)
                         if f.lower().endswith('.xlsx') and not f.startswith('~$')]

        if not archivos_xlsx:
            logger.log("[INFO] No hay archivos .xlsx en entrada_xlsx/")
            logger.fin_conversion()
            return [], []

        logger.log(f"[INFO] Encontrados {len(archivos_xlsx)} archivos .xlsx")
        logger.log("")

        archivos_csv = []
        archivos_xlsx_exitosos = []

        for archivo_xlsx in archivos_xlsx:
            ruta_xlsx = os.path.join(carpeta_entrada, archivo_xlsx)
            nombre_csv = archivo_xlsx.rsplit('.', 1)[0] + '.csv'
            ruta_csv = os.path.join(carpeta_temporal, nombre_csv)

            if self.convertir(ruta_xlsx, ruta_csv, logger):
                archivos_csv.append(nombre_csv)
                archivos_xlsx_exitosos.append(archivo_xlsx)
                logger.archivos_convertidos.append(archivo_xlsx)
            else:
                logger.archivos_conversion_error.append(archivo_xlsx)
                logger.log(f"       {archivo_xlsx} permanece en entrada_xlsx/ (revisar error)")

        logger.fin_conversion()
        return archivos_csv, archivos_xlsx_exitosos


# =============================================================================
# IMPLEMENTACIONES
# =============================================================================

class ColumnValidator(IValidator):
    """Validador de columnas para archivos CSV/delimitados"""

    def __init__(self, schema_loader: SchemaLoader, separador: str = ';'):
        self.separador = separador
        self.schema_loader = schema_loader

    def validar(self, archivo: str, contenido: str, logger: MigrationLogger) -> Tuple[bool, Optional[str]]:
        if not self.schema_loader.esquemas:
            logger.log("[INFO] No hay esquemas definidos - Continuando sin validación")
            return True, None

        tipo_archivo = self.schema_loader.obtener_tipo_archivo(archivo)

        if tipo_archivo is None:
            logger.log("[ERROR] Archivo no parametrizado en esquemas")
            logger.log(f"  - No existe configuración para archivos tipo '{Path(archivo).stem}'")
            return False, "Archivo no configurado en esquemas"

        columnas_esquema = self.schema_loader.columnas_obligatorias(tipo_archivo)
        if not columnas_esquema:
            return True, None

        # Verificar integridad del contenido
        caracteres_corruptos = ['�', '\ufffd', 'ï¿½', 'Ã­', 'Ã±', 'Ã¡', 'Ã©', 'Ã³', 'Ãº']

        for caracter in caracteres_corruptos:
            if caracter in contenido:
                logger.log("[ERROR] Archivo contiene caracteres corruptos")
                logger.log(f"  - Se detectó: '{caracter}' en el contenido")
                return False, "Archivo corrupto - debe ser regenerado"

        tiene_comodin = self.schema_loader.tiene_comodin(tipo_archivo)

        try:
            lineas = contenido.strip().split('\n')
            if not lineas:
                logger.log("[ERROR] Archivo vacío")
                return False, "Archivo vacío"

            headers = lineas[0].split(self.separador)
            headers = [h.strip() for h in headers]

            logger.log("[OK] Validación de columnas iniciada")

            cantidad_obligatorias = len(columnas_esquema)
            cantidad_encontrada = len(headers)

            if tiene_comodin:
                if cantidad_encontrada < cantidad_obligatorias:
                    logger.log(f"\n[ERROR] Columnas insuficientes")
                    logger.log(f"  - Columnas obligatorias requeridas: {cantidad_obligatorias}")
                    logger.log(f"  - Columnas recibidas: {cantidad_encontrada}")
                    return False, "Faltan columnas obligatorias"

                diferencias = []
                for i in range(cantidad_obligatorias):
                    esperada = columnas_esquema[i]
                    encontrada = headers[i] if i < cantidad_encontrada else ""

                    esperada_6 = esperada[:6] if len(esperada) >= 6 else esperada
                    encontrada_6 = encontrada[:6] if len(encontrada) >= 6 else encontrada

                    if esperada_6 != encontrada_6:
                        diferencias.append(f"Posición {i+1}: esperaba '{esperada}', encontró '{encontrada}'")

                if diferencias:
                    logger.log(f"\n[ERROR] Error en columnas obligatorias")
                    for diff in diferencias[:5]:
                        logger.log(f"  - {diff}")
                    return False, "Error en columnas obligatorias"

                logger.log("[OK] Validación de columnas exitosa")
                logger.log(f"  - Columnas obligatorias (1-{cantidad_obligatorias}): Validadas correctamente")
                if cantidad_encontrada > cantidad_obligatorias:
                    logger.log(f"  - Columnas adicionales ({cantidad_obligatorias + 1}-{cantidad_encontrada}): Permitidas")

                return True, None

            else:
                if cantidad_encontrada != cantidad_obligatorias:
                    logger.log(f"\n[ERROR] Validación de columnas fallida")
                    logger.log(f"  - Columnas esperadas: {cantidad_obligatorias}")
                    logger.log(f"  - Columnas encontradas: {cantidad_encontrada}")
                    return False, "Cantidad de columnas incorrecta"

                diferencias = []
                for i, (esperada, encontrada) in enumerate(zip(columnas_esquema, headers)):
                    esperada_6 = esperada[:6] if len(esperada) >= 6 else esperada
                    encontrada_6 = encontrada[:6] if len(encontrada) >= 6 else encontrada

                    if esperada_6 != encontrada_6:
                        diferencias.append(f"Posición {i+1}: esperaba '{esperada}', encontró '{encontrada}'")

                if diferencias:
                    logger.log(f"\n[ERROR] Errores detectados:")
                    for diff in diferencias[:10]:
                        logger.log(f"  - {diff}")
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
        print(f"[CONFIG] Cargados {len(self.reemplazos)} reemplazos de caracteres")

    def procesar(self, contenido: str, separador_entrada: str, separador_salida: str) -> Tuple[str, int]:
        contador_reemplazos = 0

        if contenido.startswith('\ufeff'):
            contenido = contenido.replace('\ufeff', '')

        for original, nuevo in self.reemplazos:
            ocurrencias = contenido.count(original)
            if ocurrencias > 0:
                contenido = contenido.replace(original, nuevo)
                contador_reemplazos += ocurrencias

        if separador_entrada != separador_salida:
            contenido = contenido.replace(separador_entrada, separador_salida)

        return contenido, contador_reemplazos


class SmartFileHandler(IFileHandler):
    """Manejador inteligente de archivos con detección automática de encoding"""

    def leer(self, ruta: str, logger: MigrationLogger) -> str:
        with open(ruta, 'rb') as f:
            contenido_raw = f.read()

        if not contenido_raw:
            return ""

        encodings_a_probar = [
            'utf-8',
            'utf-8-sig',
            'windows-1252',
            'cp1252',
            'iso-8859-1',
            'latin-1',
        ]

        for encoding in encodings_a_probar:
            try:
                contenido = contenido_raw.decode(encoding)

                if '�' in contenido:
                    continue

                caracteres_espanol = 'ÑñáéíóúÁÉÍÓÚüÜ'

                if any(c in contenido for c in caracteres_espanol):
                    logger.log(f"[ENCODING] Detectado como {encoding}")
                    return contenido

                if '\n' in contenido or '\r' in contenido:
                    logger.log(f"[ENCODING] Usando {encoding}")
                    return contenido

            except (UnicodeDecodeError, AttributeError):
                continue

        logger.log(f"[WARN] No se pudo detectar encoding, forzando utf-8")
        return contenido_raw.decode('utf-8', errors='replace')

    def escribir(self, ruta: str, contenido: str) -> None:
        with open(ruta, 'w', encoding='utf-8') as f:
            f.write(contenido)


class FTPUploader(IUploader):
    """Cargador FTP para AS400"""

    def __init__(self, config: FTPConfig):
        self.config = config

    def _limpiar_remoto(self, ftp: FTP, logger: MigrationLogger) -> None:
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
                    size_kb = size / 1024

                    with open(ruta_archivo, 'rb') as f:
                        ftp.storlines(f"STOR {archivo}", f)

                    if size_kb > 1024:
                        size_mb = size_kb / 1024
                        logger.log(f"  ✓ {archivo} ({size_mb:.1f} MB)")
                    else:
                        logger.log(f"  ✓ {archivo} ({size_kb:.0f} KB)")

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
                 excel_converter: ExcelConverter,
                 validator: IValidator,
                 processor: IProcessor,
                 file_handler: IFileHandler,
                 uploader: IUploader,
                 logger: MigrationLogger):
        self.process_config = process_config
        self.excel_converter = excel_converter
        self.validator = validator
        self.processor = processor
        self.file_handler = file_handler
        self.uploader = uploader
        self.logger = logger
        self.carpeta_temporal = '.temp_csv'
        self.separador_csv = ';'
        self.tipo_entrada = 'xlsx'

    def solicitar_tipo_entrada(self) -> str:
        """Solicita al usuario el tipo de archivos a procesar"""
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
                print("Opción inválida. Por favor seleccione 1 o 2.")

    def solicitar_separador(self) -> str:
        """Solicita al usuario el separador para el CSV"""
        print("\n" + "=" * 60)
        print("CONFIGURACIÓN DE SEPARADOR CSV")
        print("=" * 60)

        if self.tipo_entrada == 'xlsx':
            print("¿Con qué separador desea generar los archivos CSV?")
        else:
            print("¿Qué separador tienen los archivos CSV de entrada?")

        print("  1) Punto y coma (;)  ← Recomendado")
        print("  2) Pipe (|)")
        print("  3) Coma (,)")
        print("  4) Tabulador (\\t)")
        print("  5) Otro")

        while True:
            opcion = input("\nSeleccione una opción (1-5) [1]: ").strip()

            if opcion == '':
                opcion = '1'

            separadores = {'1': ';', '2': '|', '3': ',', '4': '\t'}

            if opcion in separadores:
                separador = separadores[opcion]
                nombre = {';': 'Punto y coma (;)', '|': 'Pipe (|)',
                          ',': 'Coma (,)', '\t': 'Tabulador'}.get(separador)
                print(f"Separador seleccionado: {nombre}")
                return separador
            elif opcion == '5':
                separador = input("Ingrese el separador personalizado: ")
                if separador:
                    print(f"Separador seleccionado: '{separador}'")
                    return separador
            else:
                print("Opción inválida. Por favor seleccione 1-5.")

    def limpiar_directorio(self, directorio: str) -> None:
        if os.path.exists(directorio):
            for archivo in os.listdir(directorio):
                ruta = os.path.join(directorio, archivo)
                if os.path.isfile(ruta):
                    os.remove(ruta)
            self.logger.log(f"[CLEAN] Directorio limpio: {directorio}")
        else:
            os.makedirs(directorio)
            self.logger.log(f"[CLEAN] Directorio creado: {directorio}")

    def crear_carpetas_si_no_existen(self):
        """Crea las carpetas de entrada si no existen"""
        for carpeta in [self.process_config.carpeta_entrada_xlsx,
                        self.process_config.carpeta_entrada_csv,
                        self.process_config.carpeta_salida]:
            if not os.path.exists(carpeta):
                os.makedirs(carpeta)
                print(f"[INFO] Creada carpeta: {carpeta}/")

    def procesar_archivos_csv(self, carpeta_csv: str, archivos_origen: Dict[str, str]) -> List[str]:
        """
        Procesa todos los archivos CSV.
        
        Args:
            carpeta_csv: Carpeta con los CSV a procesar
            archivos_origen: Diccionario {nombre_csv: ruta_archivo_origen}
        
        Returns:
            Lista de archivos origen procesados exitosamente
        """
        self.logger.inicio_procesamiento()

        destino = self.process_config.carpeta_salida
        os.makedirs(destino, exist_ok=True)

        archivos = [f for f in os.listdir(carpeta_csv) if f.lower().endswith('.csv')]

        if not archivos:
            self.logger.log(f"[WARN] No hay archivos CSV para procesar")
            return []

        self.logger.log(f"[PROCESO] {len(archivos)} archivos CSV para procesar")

        archivos_exitosos = []

        for archivo in archivos:
            ruta_origen = os.path.join(carpeta_csv, archivo)
            ruta_destino = os.path.join(destino, archivo)

            if os.path.isfile(ruta_origen):
                self.logger.inicio_archivo(archivo)
                exitoso = False

                try:
                    contenido = self.file_handler.leer(ruta_origen, self.logger)

                    if not contenido.strip():
                        self.logger.log("[WARN] Archivo vacío o ilegible")
                        self.logger.fin_archivo(archivo, False)
                        continue

                    es_valido, mensaje_error = self.validator.validar(
                        archivo, contenido, self.logger)
                    if not es_valido:
                        self.logger.fin_archivo(archivo, False)
                        continue

                    contenido_modificado, num_reemplazos = self.processor.procesar(
                        contenido,
                        self.separador_csv,
                        self.process_config.separador_salida
                    )

                    self.logger.log("[OK] Reemplazo de caracteres completado")
                    if num_reemplazos > 0:
                        self.logger.log(f"  - Caracteres especiales procesados: {num_reemplazos}")

                    self.file_handler.escribir(ruta_destino, contenido_modificado)
                    self.logger.log(f"[OK] Archivo guardado en {destino}/{archivo}")

                    exitoso = True

                    # Guardar el archivo origen para eliminar después
                    if archivo in archivos_origen:
                        archivos_exitosos.append(archivos_origen[archivo])

                except Exception as exc:
                    self.logger.log(f"[ERROR] Error procesando archivo: {exc}")

                self.logger.fin_archivo(archivo, exitoso)

        return archivos_exitosos

    def limpiar_temporal(self):
        """Elimina la carpeta temporal"""
        if os.path.exists(self.carpeta_temporal):
            shutil.rmtree(self.carpeta_temporal)

    def ejecutar(self, skip_ftp: bool = False) -> None:
        """Ejecuta el proceso completo de migración"""
        self.logger.inicio_proceso()

        # Crear carpetas si no existen
        self.crear_carpetas_si_no_existen()

        # Solicitar tipo de entrada
        self.tipo_entrada = self.solicitar_tipo_entrada()

        # Solicitar separador
        self.separador_csv = self.solicitar_separador()

        # Actualizar separador en componentes
        self.excel_converter.separador = self.separador_csv
        self.validator.separador = self.separador_csv

        # Determinar carpeta de entrada según selección
        if self.tipo_entrada == 'xlsx':
            carpeta_entrada = self.process_config.carpeta_entrada_xlsx
        else:
            carpeta_entrada = self.process_config.carpeta_entrada_csv

        print("\n[CONFIG] Configuración del proceso:")
        print(f"  - Tipo de entrada: {self.tipo_entrada.upper()}")
        print(f"  - Carpeta entrada: {carpeta_entrada}/")
        print(f"  - Carpeta salida: {self.process_config.carpeta_salida}/")
        print(f"  - Separador CSV: {repr(self.separador_csv)}")
        print(f"  - Separador salida: {repr(self.process_config.separador_salida)}")
        print(f"  - Conservar entrada: {'Sí' if self.process_config.conservar_entrada else 'No'}")
        print()

        # 1. Limpiar directorio salida
        self.limpiar_directorio(self.process_config.carpeta_salida)

        archivos_a_eliminar = []

        if self.tipo_entrada == 'xlsx':
            # 2a. Convertir XLSX → CSV
            archivos_csv, archivos_xlsx_exitosos = self.excel_converter.convertir_carpeta(
                carpeta_entrada, self.carpeta_temporal, self.logger
            )

            # Mapear CSV a XLSX origen
            archivos_origen = {}
            for xlsx in archivos_xlsx_exitosos:
                csv_name = xlsx.rsplit('.', 1)[0] + '.csv'
                archivos_origen[csv_name] = os.path.join(carpeta_entrada, xlsx)

            # 3. Procesar archivos CSV
            if archivos_csv:
                archivos_a_eliminar = self.procesar_archivos_csv(
                    self.carpeta_temporal, archivos_origen
                )

        else:
            # 2b. Copiar CSV a carpeta temporal para procesar
            archivos_csv = [f for f in os.listdir(carpeta_entrada)
                            if f.lower().endswith('.csv')]

            if archivos_csv:
                os.makedirs(self.carpeta_temporal, exist_ok=True)

                archivos_origen = {}
                for csv_file in archivos_csv:
                    src = os.path.join(carpeta_entrada, csv_file)
                    dst = os.path.join(self.carpeta_temporal, csv_file)
                    shutil.copy2(src, dst)
                    archivos_origen[csv_file] = src

                # 3. Procesar archivos CSV
                archivos_a_eliminar = self.procesar_archivos_csv(
                    self.carpeta_temporal, archivos_origen
                )
            else:
                self.logger.log(f"[INFO] No hay archivos .csv en {carpeta_entrada}/")

        # 4. Limpiar carpeta temporal
        self.limpiar_temporal()

        # 5. Subir por FTP (opcional)
        ftp_exitoso = False
        if not skip_ftp and self.logger.archivos_procesados:
            ftp_exitoso = self.uploader.subir(self.process_config.carpeta_salida, self.logger)
        elif skip_ftp:
            self.logger.log("\n[INFO] Subida FTP omitida (modo prueba)")
            ftp_exitoso = True  # En modo prueba, consideramos exitoso
        elif not self.logger.archivos_procesados:
            self.logger.log("\n[INFO] No hay archivos para subir por FTP")

        # 6. Eliminar archivos origen SOLO si FTP fue exitoso y no está configurado CONSERVAR_ENTRADA
        if self.process_config.conservar_entrada:
            self.logger.log("\n[INFO] CONSERVAR_ENTRADA=true - Archivos de entrada no eliminados")
        elif ftp_exitoso and archivos_a_eliminar:
            self.logger.log("\n[CLEAN] Limpiando archivos de entrada procesados...")
            for archivo_origen in archivos_a_eliminar:
                if os.path.exists(archivo_origen):
                    os.remove(archivo_origen)
                    self.logger.log(f"  - Eliminado: {os.path.basename(archivo_origen)}")
        elif not ftp_exitoso and archivos_a_eliminar:
            self.logger.log("\n[WARN] Archivos NO eliminados de entrada (FTP falló)")
            self.logger.log("       Los archivos permanecen para reintentar")

        # 7. Resumen final
        self.logger.resumen_final(
            self.tipo_entrada,
            self.separador_csv,
            self.process_config.separador_salida,
            self.process_config.conservar_entrada
        )


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

def main():
    """Función principal"""

    skip_ftp = os.getenv('SKIP_FTP', 'false').lower() == 'true'

    # Configuraciones
    process_config = ProcessConfig()
    ftp_config = FTPConfig()

    # Crear logger
    logger = MigrationLogger(process_config.carpeta_logs)

    # Cargar reemplazos
    reemplazos = process_config.cargar_reemplazos()

    # Crear cargador de esquemas
    schema_loader = SchemaLoader(process_config.archivo_esquemas)

    # Crear implementaciones
    excel_converter = ExcelConverter(schema_loader=schema_loader, separador=';')

    validator = ColumnValidator(
        schema_loader=schema_loader,
        separador=';'
    )
    processor = CharacterProcessor(reemplazos)
    file_handler = SmartFileHandler()

    if skip_ftp:
        uploader = DummyUploader()
    else:
        uploader = FTPUploader(ftp_config)

    # Crear y ejecutar orquestador
    orchestrator = MigrationOrchestrator(
        process_config=process_config,
        excel_converter=excel_converter,
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