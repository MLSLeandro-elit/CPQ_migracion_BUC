"""Módulo de procesamiento de archivos para migración CPQ.

Este módulo se encarga de:
- Procesar archivos XLSX y CSV
- Detectar tipo de archivo por nombre o estructura
- Aplicar transformaciones (renombrar columnas, formatear fechas, etc.)
- Reemplazar caracteres especiales
- Generar archivos en carpeta SALIDA

Uso independiente:
    python processor.py

Uso como módulo:
    from processor import procesar
    procesar(logger=mi_logger)
"""

import os
import json
import logging
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional, Any
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

import pandas as pd


# =============================================================================
# CARGA DE CONFIGURACIÓN EXTERNA
# =============================================================================

load_dotenv()


@dataclass
class ProcessConfig:
    """Configuración del proceso de migración."""
    carpeta_entrada_xlsx: str
    carpeta_entrada_csv: str
    carpeta_temporal: str
    carpeta_salida: str
    carpeta_logs: str
    separador_salida: str
    separador_decimal: str
    archivo_esquemas: str
    archivo_reemplazos: str
    conservar_entrada: bool
    tipo_entrada: str = 'xlsx'
    separador_entrada_csv: str = ';'


# =============================================================================
# CARGADORES DE CONFIGURACIÓN
# =============================================================================

class SchemaLoader:
    """Carga y gestiona esquemas de archivos."""

    def __init__(self, archivo_esquemas: str):
        self.archivo_esquemas = archivo_esquemas
        self.esquemas = self._cargar_esquemas()

    def _cargar_esquemas(self) -> Dict[str, Any]:
        """Carga esquemas desde archivo JSON."""
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
        """Determina el tipo de archivo basado en el nombre."""
        nombre_base = Path(nombre_archivo).stem
        for tipo in self.esquemas.keys():
            if nombre_base.startswith(tipo):
                return tipo
        return None

    def obtener_columnas(self, tipo_archivo: str) -> List[str]:
        """Obtiene lista de columnas para un tipo de archivo."""
        if tipo_archivo not in self.esquemas:
            return []

        esquema = self.esquemas[tipo_archivo]

        if isinstance(esquema, dict):
            return esquema.get('columnas', [])
        elif isinstance(esquema, list):
            return esquema
        return []

    def obtener_fechas_numericas(self, tipo_archivo: str) -> List[str]:
        """Obtiene lista de columnas que son fechas numéricas."""
        if tipo_archivo not in self.esquemas:
            return []

        esquema = self.esquemas[tipo_archivo]

        if isinstance(esquema, dict):
            return esquema.get('fechas_numericas', [])
        return []

    def tiene_comodin(self, tipo_archivo: str) -> bool:
        """Verifica si el esquema tiene comodín (*)."""
        columnas = self.obtener_columnas(tipo_archivo)
        return '*' in columnas

    def columnas_obligatorias(self, tipo_archivo: str) -> List[str]:
        """Obtiene columnas obligatorias (sin el comodín)."""
        columnas = self.obtener_columnas(tipo_archivo)
        if '*' in columnas:
            return columnas[:columnas.index('*')]
        return columnas

    def obtener_filas_omitir(self, tipo_archivo: str) -> List[int]:
        """Obtiene lista de filas a omitir (numeración desde 1)."""
        if tipo_archivo not in self.esquemas:
            return []

        esquema = self.esquemas[tipo_archivo]

        if isinstance(esquema, dict):
            return esquema.get('filas_omitir', [])
        return []

    def obtener_fila_nombres_columna(self, tipo_archivo: str) -> int:
        """Obtiene la fila donde están los nombres de columna (default: 1)."""
        if tipo_archivo not in self.esquemas:
            return 1

        esquema = self.esquemas[tipo_archivo]

        if isinstance(esquema, dict):
            return esquema.get('fila_nombres_columna', 1)
        return 1

    def _comparar_nombres_columna(self, nombres_archivo: List, nombres_esquema: List[str]) -> bool:
        """Compara nombres de columna del archivo con el esquema."""
        tiene_comodin = '*' in nombres_esquema
        columnas_obligatorias = [c for c in nombres_esquema if c != '*']
        
        if tiene_comodin:
            if len(nombres_archivo) < len(columnas_obligatorias):
                return False
        else:
            if len(nombres_archivo) != len(columnas_obligatorias):
                return False
        
        for i, col_esquema in enumerate(columnas_obligatorias):
            if i >= len(nombres_archivo):
                return False
            
            val_archivo = nombres_archivo[i]
            if pd.isna(val_archivo):
                return False
            
            col_archivo = str(val_archivo).strip()
            n = min(6, len(col_esquema))
            if col_archivo[:n].upper() != col_esquema[:n].upper():
                return False
        
        return True

    def detectar_tipo_archivo(self, df: pd.DataFrame, esquemas_excluir: set = None) -> Optional[str]:
        """Detecta el tipo de archivo basándose en la estructura de columnas."""
        if esquemas_excluir is None:
            esquemas_excluir = set()
            
        for tipo_archivo in self.esquemas.keys():
            if tipo_archivo in esquemas_excluir:
                continue
                
            fila_nombres = self.obtener_fila_nombres_columna(tipo_archivo)
            columnas_esquema = self.obtener_columnas(tipo_archivo)
            
            if not columnas_esquema:
                continue
            
            indice_fila = fila_nombres - 1
            if indice_fila >= len(df):
                continue
            
            nombres_archivo = df.iloc[indice_fila].tolist()
            
            if self._comparar_nombres_columna(nombres_archivo, columnas_esquema):
                return tipo_archivo
        
        return None


# =============================================================================
# SISTEMA DE LOGGING
# =============================================================================

class MigrationLogger:
    """Manejador de logs para el proceso de migración."""

    def __init__(self, carpeta_logs: str, prefijo: str = "migracion"):
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

        self.archivos_convertidos = []  # Lista de tuplas (entrada, salida)
        self.archivos_conversion_error = []
        self.archivos_procesados = []
        self.archivos_con_error = []
        self.archivos_ftp_ok = []
        self.archivos_ftp_error = []
        self.mapeo_archivos = {}  # Diccionario salida → entrada para lookup rápido

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

    def registrar_conversion(self, archivo_entrada: str, archivo_salida: str):
        """Registra el mapeo de archivo entrada → salida."""
        self.archivos_convertidos.append((archivo_entrada, archivo_salida))
        self.mapeo_archivos[archivo_salida] = archivo_entrada

    def inicio_archivo(self, nombre_archivo: str, origen: str = None):
        self.log("")
        if origen:
            self.log(f"ARCHIVO: {nombre_archivo} (origen: {origen})")
        else:
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

    def inicio_ftp(self, config):
        """Inicio de fase FTP."""
        self.log("")
        self.log(self.linea_separadora('-'))
        self.log("FASE 3: SUBIDA FTP AL SERVIDOR AS400")
        self.log(self.linea_separadora('-'))
        self.log(f"Host: {config.host}")
        self.log(f"Usuario: {config.user}")
        self.log(f"Carpeta remota: {config.carpeta_remota}")

    def fin_ftp(self, exitoso: bool):
        """Fin de fase FTP."""
        if exitoso:
            self.log(f"\n[FTP] Archivos subidos: {len(self.archivos_ftp_ok)}")
            for archivo in self.archivos_ftp_ok:
                self.log(f"  ✓ {archivo}")
            if self.archivos_ftp_error:
                self.log(f"[FTP] Errores: {len(self.archivos_ftp_error)}")
                for archivo in self.archivos_ftp_error:
                    self.log(f"  ✗ {archivo}")
        self.log(self.linea_separadora('-'))

    def resumen_final(self, config: ProcessConfig):
        self.log("")
        self.log(self.linea_separadora('='))
        self.log(f"RESUMEN FINAL - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(self.linea_separadora('='))

        self.log(f"\nTipo de entrada: {config.tipo_entrada}")

        if config.tipo_entrada == 'xlsx':
            total_xlsx = len(self.archivos_convertidos) + len(self.archivos_conversion_error)
            self.log(f"\nFASE 1 - CONVERSIÓN XLSX → CSV:")
            self.log(f"  Total archivos XLSX: {total_xlsx}")
            self.log(f"  Convertidos exitosamente: {len(self.archivos_convertidos)}")
            if self.archivos_convertidos:
                for entrada, salida in self.archivos_convertidos:
                    self.log(f"    ✓ {entrada} → {salida}")
            self.log(f"  Con errores (permanecen en entrada_xlsx/): {len(self.archivos_conversion_error)}")
            if self.archivos_conversion_error:
                for archivo in self.archivos_conversion_error:
                    self.log(f"    ✗ {archivo}")

        total_csv = len(self.archivos_procesados) + len(self.archivos_con_error)
        self.log(f"\nFASE 2 - PROCESAMIENTO CSV:")
        self.log(f"  Total archivos CSV: {total_csv}")
        self.log(f"  Procesados exitosamente: {len(self.archivos_procesados)}")
        if self.archivos_procesados:
            for archivo in self.archivos_procesados:
                origen = self.mapeo_archivos.get(archivo)
                if origen:
                    self.log(f"    ✓ {archivo} (origen: {origen})")
                else:
                    self.log(f"    ✓ {archivo}")
        self.log(f"  Con errores (permanecen en entrada_csv/): {len(self.archivos_con_error)}")
        if self.archivos_con_error:
            for archivo in self.archivos_con_error:
                self.log(f"    ✗ {archivo}")

        self.log(f"\nFASE 3 - SUBIDA FTP:")
        self.log(f"  Archivos subidos: {len(self.archivos_ftp_ok)}")
        if self.archivos_ftp_ok:
            for archivo in self.archivos_ftp_ok:
                self.log(f"    ✓ {archivo}")
        self.log(f"  Errores de subida: {len(self.archivos_ftp_error)}")
        if self.archivos_ftp_error:
            for archivo in self.archivos_ftp_error:
                self.log(f"    ✗ {archivo}")

        conservar_entrada = config.conservar_entrada
        self.log(f"\nSeparador CSV usado: '{config.separador_entrada_csv}'")
        self.log(f"Separador salida: '{config.separador_salida}'")
        self.log(f"Conservar entrada: {'Sí' if conservar_entrada else 'No'}")

        self.log(f"\nLog guardado en: {self.archivo_log}")
        self.log(self.linea_separadora('='))


# =============================================================================
# INTERFACES
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


# =============================================================================
# CONVERSOR XLSX/CSV A CSV
# =============================================================================

class ExcelConverter:
    """Convierte archivos Excel (.xlsx) y CSV a CSV con transformaciones."""

    def __init__(self, schema_loader: SchemaLoader, separador: str = ';', separador_decimal: str = '.'):
        self.schema_loader = schema_loader
        self.separador = separador
        self.separador_decimal = separador_decimal

    def _formatear_entero(self, valor) -> str:
        """Convierte floats que son enteros a string sin .0.
        
        - Float 1234.0 → '1234'
        - Float 1234.56 → '1234,56' (si separador_decimal es ',')
        - String → se deja como está (no se convierte)
        """
        if pd.isna(valor):
            return ''
        
        if isinstance(valor, float):
            if valor == int(valor):
                return str(int(valor))
            resultado = str(valor)
            if self.separador_decimal == ',':
                resultado = resultado.replace('.', ',')
            return resultado
        
        if isinstance(valor, str):
            return valor  # No tocar strings, devolverlos como están
        
        return str(valor)

    def _formatear_fecha_numerica(self, valor) -> str:
        """Convierte fecha numérica de Excel a formato YYYYMMDD."""
        if pd.isna(valor):
            return ''
        
        if isinstance(valor, str):
            return valor.strip()
        
        if isinstance(valor, (int, float)):
            if 10000101 <= valor <= 99991231:
                return str(int(valor))
            try:
                fecha = pd.to_datetime(valor, unit='D', origin='1899-12-30')
                return fecha.strftime('%Y%m%d')
            except:
                return str(int(valor)) if valor == int(valor) else str(valor)
        
        if isinstance(valor, (pd.Timestamp, datetime)):
            return valor.strftime('%Y%m%d')
        
        return str(valor)

    def _procesar_dataframe_con_tipo(self, df: pd.DataFrame, nombre: str, 
                                      tipo_archivo: str, logger: MigrationLogger) -> Tuple[bool, pd.DataFrame, Optional[str]]:
        """Procesa DataFrame usando un tipo de archivo específico."""
        filas_omitir = self.schema_loader.obtener_filas_omitir(tipo_archivo)
        
        if filas_omitir:
            indices_a_eliminar = [f - 1 for f in filas_omitir]
            indices_validos = [i for i in indices_a_eliminar if 0 <= i < len(df)]
            
            if indices_validos:
                df = df.drop(df.index[indices_validos]).reset_index(drop=True)
                logger.log(f"  [OK] Filas omitidas: {filas_omitir} ({len(indices_validos)} filas eliminadas)")

        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)

        columnas_esquema = self.schema_loader.columnas_obligatorias(tipo_archivo)
        fechas_numericas = self.schema_loader.obtener_fechas_numericas(tipo_archivo)
        tiene_comodin = self.schema_loader.tiene_comodin(tipo_archivo)

        num_columnas_archivo = len(df.columns)
        num_columnas_esquema = len(columnas_esquema)

        if tiene_comodin:
            if num_columnas_archivo < num_columnas_esquema:
                logger.log(f"  [ERROR] {nombre}: Columnas insuficientes")
                logger.log(f"          Archivo tiene {num_columnas_archivo}, esquema requiere mínimo {num_columnas_esquema}")
                return False, df, None
        else:
            if num_columnas_archivo != num_columnas_esquema:
                logger.log(f"  [ERROR] {nombre}: Cantidad de columnas no coincide")
                logger.log(f"          Archivo tiene {num_columnas_archivo}, esquema requiere {num_columnas_esquema}")
                return False, df, None

        nuevos_nombres = []
        for i in range(num_columnas_archivo):
            if i < num_columnas_esquema:
                nuevos_nombres.append(columnas_esquema[i])
            else:
                nuevos_nombres.append(df.columns[i])

        df.columns = nuevos_nombres
        logger.log(f"  [OK] Columnas renombradas según esquema ({num_columnas_esquema} columnas)")

        if fechas_numericas:
            fechas_convertidas = 0
            for col in fechas_numericas:
                if col in df.columns:
                    df[col] = df[col].apply(self._formatear_fecha_numerica)
                    fechas_convertidas += 1
            if fechas_convertidas > 0:
                logger.log(f"  [OK] Fechas convertidas a YYYYMMDD ({fechas_convertidas} columnas)")

        # Formatear solo columnas numéricas (no fechas, no texto)
        columnas_numericas = [
            col for col in df.columns 
            if col not in fechas_numericas and df[col].dtype in ['float64', 'int64']
        ]
        for col in columnas_numericas:
            df[col] = df[col].apply(self._formatear_entero)
        if columnas_numericas:
            logger.log(f"  [OK] Valores numéricos formateados ({len(columnas_numericas)} columnas)")

        return True, df, tipo_archivo

    def convertir_carpeta(self, carpeta_entrada: str, carpeta_temporal: str,
                          logger: MigrationLogger) -> Tuple[List[str], List[str]]:
        """Convierte todos los archivos XLSX de una carpeta."""
        logger.inicio_conversion()

        os.makedirs(carpeta_temporal, exist_ok=True)

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
        esquemas_ocupados = set()
        archivos_pendientes = []

        # PASADA 1: Por nombre
        logger.log("[PASADA 1] Buscando coincidencias por nombre...")
        for archivo_xlsx in archivos_xlsx:
            tipo_por_nombre = self.schema_loader.obtener_tipo_archivo(archivo_xlsx)
            
            if tipo_por_nombre and tipo_por_nombre not in esquemas_ocupados:
                ruta_xlsx = os.path.join(carpeta_entrada, archivo_xlsx)
                logger.log(f"  {archivo_xlsx} → coincide con esquema {tipo_por_nombre}")
                
                exitoso, tipo_detectado = self._convertir_xlsx_con_tipo(
                    ruta_xlsx, carpeta_temporal, tipo_por_nombre, logger
                )
                
                if exitoso:
                    nombre_csv = f"{tipo_detectado}.csv"
                    archivos_csv.append(nombre_csv)
                    archivos_xlsx_exitosos.append(archivo_xlsx)
                    logger.registrar_conversion(archivo_xlsx, nombre_csv)
                    esquemas_ocupados.add(tipo_detectado)
                else:
                    logger.archivos_conversion_error.append(archivo_xlsx)
                    logger.log(f"       {archivo_xlsx} permanece en entrada_xlsx/ (revisar error)")
            else:
                archivos_pendientes.append(archivo_xlsx)

        # PASADA 2: Por estructura
        if archivos_pendientes:
            logger.log("")
            logger.log(f"[PASADA 2] Detectando {len(archivos_pendientes)} archivos por estructura...")
            
            for archivo_xlsx in archivos_pendientes:
                ruta_xlsx = os.path.join(carpeta_entrada, archivo_xlsx)
                
                exitoso, tipo_detectado = self._convertir_xlsx_detectando(
                    ruta_xlsx, carpeta_temporal, esquemas_ocupados, logger
                )
                
                if exitoso and tipo_detectado:
                    nombre_csv = f"{tipo_detectado}.csv"
                    archivos_csv.append(nombre_csv)
                    archivos_xlsx_exitosos.append(archivo_xlsx)
                    logger.registrar_conversion(archivo_xlsx, nombre_csv)
                    esquemas_ocupados.add(tipo_detectado)
                else:
                    logger.archivos_conversion_error.append(archivo_xlsx)
                    logger.log(f"       {archivo_xlsx} permanece en entrada_xlsx/ (revisar error)")

        logger.fin_conversion()
        return archivos_csv, archivos_xlsx_exitosos

    def _convertir_xlsx_con_tipo(self, ruta_xlsx: str, carpeta_temporal: str, 
                            tipo_archivo: str, logger: MigrationLogger) -> Tuple[bool, Optional[str]]:
        """Convierte archivo XLSX usando un tipo específico."""
        nombre = os.path.basename(ruta_xlsx)

        try:
            df = pd.read_excel(ruta_xlsx, engine='openpyxl', header=None)

            if df.empty:
                logger.log(f"  [ERROR] Archivo vacío: {nombre}")
                return False, None

            exitoso, df, tipo = self._procesar_dataframe_con_tipo(df, nombre, tipo_archivo, logger)
            if not exitoso:
                return False, None

            nombre_csv = f"{tipo}.csv"
            ruta_csv = os.path.join(carpeta_temporal, nombre_csv)
            df.to_csv(ruta_csv, sep=self.separador, index=False, encoding='utf-8')

            filas = len(df)
            columnas = len(df.columns)
            logger.log(f"  [OK] {nombre} → {nombre_csv} ({filas} filas, {columnas} columnas)")

            return True, tipo

        except Exception as e:
            logger.log(f"  [ERROR] {nombre}: {str(e)}")
            return False, None

    def _convertir_xlsx_detectando(self, ruta_xlsx: str, carpeta_temporal: str,
                              esquemas_ocupados: set, logger: MigrationLogger) -> Tuple[bool, Optional[str]]:
        """Convierte archivo XLSX detectando tipo por estructura."""
        nombre = os.path.basename(ruta_xlsx)

        try:
            df = pd.read_excel(ruta_xlsx, engine='openpyxl', header=None)

            if df.empty:
                logger.log(f"  [ERROR] Archivo vacío: {nombre}")
                return False, None

            tipo_archivo = self.schema_loader.detectar_tipo_archivo(df, esquemas_ocupados)
            
            if not tipo_archivo:
                logger.log(f"  [ERROR] {nombre}: No se pudo identificar el tipo de archivo")
                logger.log(f"          No coincide con ningún esquema disponible")
                return False, None

            logger.log(f"  [OK] {nombre} → Tipo detectado: {tipo_archivo}")

            exitoso, df, tipo = self._procesar_dataframe_con_tipo(df, nombre, tipo_archivo, logger)
            if not exitoso:
                return False, None

            nombre_csv = f"{tipo}.csv"
            ruta_csv = os.path.join(carpeta_temporal, nombre_csv)
            df.to_csv(ruta_csv, sep=self.separador, index=False, encoding='utf-8')

            filas = len(df)
            columnas = len(df.columns)
            logger.log(f"  [OK] {nombre} → {nombre_csv} ({filas} filas, {columnas} columnas)")

            return True, tipo

        except Exception as e:
            logger.log(f"  [ERROR] {nombre}: {str(e)}")
            return False, None

    def convertir_carpeta_csv(self, carpeta_entrada: str, carpeta_salida: str,
                              separador_entrada: str, logger: MigrationLogger) -> Tuple[List[str], List[str]]:
        """Procesa todos los archivos CSV de una carpeta."""
        logger.inicio_conversion()

        os.makedirs(carpeta_salida, exist_ok=True)

        archivos_csv = [f for f in os.listdir(carpeta_entrada)
                        if f.lower().endswith('.csv')]

        if not archivos_csv:
            logger.log("[INFO] No hay archivos .csv en entrada_csv/")
            logger.fin_conversion()
            return [], []

        logger.log(f"[INFO] Encontrados {len(archivos_csv)} archivos .csv")
        logger.log("")

        archivos_salida = []
        archivos_csv_exitosos = []
        esquemas_ocupados = set()
        archivos_pendientes = []

        # PASADA 1: Por nombre
        logger.log("[PASADA 1] Buscando coincidencias por nombre...")
        for archivo_csv in archivos_csv:
            tipo_por_nombre = self.schema_loader.obtener_tipo_archivo(archivo_csv)
            
            if tipo_por_nombre and tipo_por_nombre not in esquemas_ocupados:
                ruta_csv = os.path.join(carpeta_entrada, archivo_csv)
                logger.log(f"  {archivo_csv} → coincide con esquema {tipo_por_nombre}")
                
                exitoso, tipo_detectado = self._convertir_csv_con_tipo(
                    ruta_csv, carpeta_salida, separador_entrada, tipo_por_nombre, logger
                )
                
                if exitoso:
                    nombre_salida = f"{tipo_detectado}.csv"
                    archivos_salida.append(nombre_salida)
                    archivos_csv_exitosos.append(archivo_csv)
                    logger.registrar_conversion(archivo_csv, nombre_salida)
                    esquemas_ocupados.add(tipo_detectado)
                else:
                    logger.archivos_conversion_error.append(archivo_csv)
                    logger.log(f"       {archivo_csv} permanece en entrada_csv/ (revisar error)")
            else:
                archivos_pendientes.append(archivo_csv)

        # PASADA 2: Por estructura
        if archivos_pendientes:
            logger.log("")
            logger.log(f"[PASADA 2] Detectando {len(archivos_pendientes)} archivos por estructura...")
            
            for archivo_csv in archivos_pendientes:
                ruta_csv = os.path.join(carpeta_entrada, archivo_csv)
                
                exitoso, tipo_detectado = self._convertir_csv_detectando(
                    ruta_csv, carpeta_salida, separador_entrada, esquemas_ocupados, logger
                )
                
                if exitoso and tipo_detectado:
                    nombre_salida = f"{tipo_detectado}.csv"
                    archivos_salida.append(nombre_salida)
                    archivos_csv_exitosos.append(archivo_csv)
                    logger.registrar_conversion(archivo_csv, nombre_salida)
                    esquemas_ocupados.add(tipo_detectado)
                else:
                    logger.archivos_conversion_error.append(archivo_csv)
                    logger.log(f"       {archivo_csv} permanece en entrada_csv/ (revisar error)")

        logger.fin_conversion()
        return archivos_salida, archivos_csv_exitosos

    def _convertir_csv_con_tipo(self, ruta_csv: str, carpeta_salida: str,
                                 separador_entrada: str, tipo_archivo: str, 
                                 logger: MigrationLogger) -> Tuple[bool, Optional[str]]:
        """Procesa archivo CSV usando un tipo específico."""
        nombre = os.path.basename(ruta_csv)

        try:
            df = pd.read_csv(ruta_csv, sep=separador_entrada, header=None, 
                            encoding='utf-8', dtype=str, keep_default_na=False)

            if df.empty:
                logger.log(f"  [ERROR] Archivo vacío: {nombre}")
                return False, None

            exitoso, df, tipo = self._procesar_dataframe_con_tipo(df, nombre, tipo_archivo, logger)
            if not exitoso:
                return False, None

            nombre_csv = f"{tipo}.csv"
            ruta_salida = os.path.join(carpeta_salida, nombre_csv)
            df.to_csv(ruta_salida, sep=self.separador, index=False, encoding='utf-8')

            filas = len(df)
            columnas = len(df.columns)
            logger.log(f"  [OK] {nombre} → {nombre_csv} ({filas} filas, {columnas} columnas)")

            return True, tipo

        except Exception as e:
            logger.log(f"  [ERROR] {nombre}: {str(e)}")
            return False, None

    def _convertir_csv_detectando(self, ruta_csv: str, carpeta_salida: str,
                                   separador_entrada: str, esquemas_ocupados: set, 
                                   logger: MigrationLogger) -> Tuple[bool, Optional[str]]:
        """Procesa archivo CSV detectando tipo por estructura."""
        nombre = os.path.basename(ruta_csv)

        try:
            df = pd.read_csv(ruta_csv, sep=separador_entrada, header=None, 
                            encoding='utf-8', dtype=str, keep_default_na=False)

            if df.empty:
                logger.log(f"  [ERROR] Archivo vacío: {nombre}")
                return False, None

            tipo_archivo = self.schema_loader.detectar_tipo_archivo(df, esquemas_ocupados)
            
            if not tipo_archivo:
                logger.log(f"  [ERROR] {nombre}: No se pudo identificar el tipo de archivo")
                logger.log(f"          No coincide con ningún esquema disponible")
                return False, None

            logger.log(f"  [OK] {nombre} → Tipo detectado: {tipo_archivo}")

            exitoso, df, tipo = self._procesar_dataframe_con_tipo(df, nombre, tipo_archivo, logger)
            if not exitoso:
                return False, None

            nombre_csv = f"{tipo}.csv"
            ruta_salida = os.path.join(carpeta_salida, nombre_csv)
            df.to_csv(ruta_salida, sep=self.separador, index=False, encoding='utf-8')

            filas = len(df)
            columnas = len(df.columns)
            logger.log(f"  [OK] {nombre} → {nombre_csv} ({filas} filas, {columnas} columnas)")

            return True, tipo

        except Exception as e:
            logger.log(f"  [ERROR] {nombre}: {str(e)}")
            return False, None


# =============================================================================
# VALIDADOR Y PROCESADOR
# =============================================================================

class ColumnValidator(IValidator):
    """Validador de columnas para archivos CSV."""

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
                return False, "Archivo vacío"

            primera_linea = lineas[0]
            columnas_archivo = [col.strip() for col in primera_linea.split(self.separador)]

            diferencias = []

            if tiene_comodin:
                if len(columnas_archivo) < len(columnas_esquema):
                    logger.error_validacion(len(columnas_esquema), len(columnas_archivo), [])
                    return False, "Cantidad de columnas insuficiente"
            else:
                if len(columnas_archivo) != len(columnas_esquema):
                    logger.error_validacion(len(columnas_esquema), len(columnas_archivo), diferencias)
                    return False, "Cantidad de columnas no coincide"

            for i, col_esperada in enumerate(columnas_esquema):
                if i < len(columnas_archivo):
                    col_archivo = columnas_archivo[i]
                    if col_archivo != col_esperada:
                        diferencias.append(
                            f"Posición {i + 1}: esperada '{col_esperada}', encontrada '{col_archivo}'"
                        )

            if diferencias:
                logger.error_validacion(len(columnas_esquema), len(columnas_archivo), diferencias)
                return False, "Columnas no coinciden"

            logger.log(f"[OK] Validación exitosa ({len(columnas_archivo)} columnas)")
            return True, None

        except Exception as e:
            logger.log(f"[ERROR] Error durante validación: {e}")
            return False, str(e)


class CharacterProcessor(IProcessor):
    """Procesador de caracteres especiales."""

    def __init__(self, archivo_reemplazos: str):
        self.reemplazos = self._cargar_reemplazos(archivo_reemplazos)

    def _cargar_reemplazos(self, archivo: str) -> Dict[str, str]:
        try:
            with open(archivo, 'r', encoding='utf-8') as f:
                reemplazos = json.load(f)
                print(f"[CONFIG] Cargados {len(reemplazos)} reemplazos de caracteres")
                return reemplazos
        except FileNotFoundError:
            print(f"[WARN] Archivo {archivo} no encontrado - sin reemplazos")
            return {}
        except json.JSONDecodeError as e:
            print(f"[ERROR] Error en {archivo}: {e}")
            return {}

    def procesar(self, contenido: str, separador_entrada: str, separador_salida: str) -> Tuple[str, int]:
        resultado = contenido
        total_reemplazos = 0

        if separador_entrada != separador_salida:
            cuenta = resultado.count(separador_entrada)
            resultado = resultado.replace(separador_entrada, separador_salida)
            total_reemplazos += cuenta

        for buscar, reemplazar in self.reemplazos.items():
            if buscar in resultado:
                cuenta = resultado.count(buscar)
                resultado = resultado.replace(buscar, reemplazar)
                total_reemplazos += cuenta

        return resultado, total_reemplazos


class SmartFileHandler(IFileHandler):
    """Manejador de archivos con detección de encoding."""

    ENCODINGS = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']

    def leer(self, ruta: str, logger: MigrationLogger) -> str:
        for encoding in self.ENCODINGS:
            try:
                with open(ruta, 'r', encoding=encoding) as f:
                    contenido = f.read()
                logger.log(f"[OK] Archivo leído con encoding: {encoding}")
                return contenido
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.log(f"[ERROR] Error leyendo archivo: {e}")
                raise

        raise UnicodeDecodeError(
            'multiple', b'', 0, 1,
            f"No se pudo decodificar el archivo con ningún encoding: {self.ENCODINGS}"
        )

    def escribir(self, ruta: str, contenido: str) -> None:
        with open(ruta, 'w', encoding='utf-8', newline='') as f:
            f.write(contenido)


# =============================================================================
# FUNCIÓN PRINCIPAL DE PROCESAMIENTO
# =============================================================================

def procesar(config: ProcessConfig = None, logger: MigrationLogger = None) -> bool:
    """
    Procesa archivos y genera salida en carpeta SALIDA.
    
    Args:
        config: Configuración del proceso (si None, usa valores por defecto)
        logger: Logger a usar (si None, crea uno nuevo)
    
    Returns:
        True si el proceso fue exitoso
    """
    # Configuración por defecto
    if config is None:
        config = ProcessConfig(
            carpeta_entrada_xlsx=os.getenv('CARPETA_ENTRADA_XLSX', 'entrada_xlsx'),
            carpeta_entrada_csv=os.getenv('CARPETA_ENTRADA_CSV', 'entrada_csv'),
            carpeta_temporal=os.getenv('CARPETA_TEMPORAL', 'temporal'),
            carpeta_salida=os.getenv('CARPETA_SALIDA', 'salida'),
            carpeta_logs=os.getenv('CARPETA_LOGS', 'logs'),
            separador_salida=os.getenv('SEPARADOR_SALIDA', '|'),
            separador_decimal=os.getenv('SEPARADOR_DECIMAL', '.'),
            archivo_esquemas=os.getenv('ARCHIVO_ESQUEMAS', 'config/esquemas.json'),
            archivo_reemplazos=os.getenv('ARCHIVO_REEMPLAZOS', 'config/reemplazos.json'),
            conservar_entrada=os.getenv('CONSERVAR_ENTRADA', 'true').lower() == 'true'
        )
    
    # Logger
    crear_logger = logger is None
    if crear_logger:
        logger = MigrationLogger(config.carpeta_logs, "processor")
        logger.inicio_proceso()
    
    # Componentes
    schema_loader = SchemaLoader(config.archivo_esquemas)
    excel_converter = ExcelConverter(
        schema_loader, 
        config.separador_salida,
        config.separador_decimal
    )
    validator = ColumnValidator(schema_loader, config.separador_salida)
    processor = CharacterProcessor(config.archivo_reemplazos)
    file_handler = SmartFileHandler()
    
    # Limpiar salida
    if os.path.exists(config.carpeta_salida):
        for archivo in os.listdir(config.carpeta_salida):
            os.remove(os.path.join(config.carpeta_salida, archivo))
    os.makedirs(config.carpeta_salida, exist_ok=True)
    logger.log(f"\n[CLEAN] Directorio limpio: {config.carpeta_salida}")
    
    # Procesar según tipo de entrada
    if config.tipo_entrada == 'xlsx':
        # Convertir XLSX → CSV
        archivos_csv, archivos_xlsx_exitosos = excel_converter.convertir_carpeta(
            config.carpeta_entrada_xlsx, config.carpeta_temporal, logger
        )
        
        # Procesar CSV (reemplazos)
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
        
        # Limpiar temporales
        if os.path.exists(config.carpeta_temporal):
            shutil.rmtree(config.carpeta_temporal)
    else:
        # Procesar CSV directamente
        archivos_procesados, archivos_csv_exitosos = excel_converter.convertir_carpeta_csv(
            config.carpeta_entrada_csv, config.carpeta_salida,
            config.separador_entrada_csv, logger
        )
        
        # Procesar reemplazos
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
    
    # Resumen si creamos el logger
    if crear_logger:
        logger.resumen_final(config)
    
    return True


def _procesar_archivos_csv(carpeta_entrada: str, carpeta_salida: str,
                           separador_entrada: str, separador_salida: str,
                           validator: ColumnValidator, processor: CharacterProcessor,
                           file_handler: SmartFileHandler, logger: MigrationLogger) -> List[str]:
    """Procesa archivos CSV aplicando validación y reemplazos."""
    archivos_a_eliminar = []
    
    if not os.path.exists(carpeta_entrada):
        return archivos_a_eliminar
    
    archivos = [f for f in os.listdir(carpeta_entrada) if f.lower().endswith('.csv')]
    
    if not archivos:
        return archivos_a_eliminar
    
    logger.inicio_procesamiento()
    
    for archivo in archivos:
        ruta_entrada = os.path.join(carpeta_entrada, archivo)
        ruta_salida = os.path.join(carpeta_salida, archivo)
        
        # Buscar archivo origen en el mapeo
        origen = logger.mapeo_archivos.get(archivo)
        logger.inicio_archivo(archivo, origen)
        
        try:
            contenido = file_handler.leer(ruta_entrada, logger)
            
            valido, error = validator.validar(archivo, contenido, logger)
            if not valido:
                logger.fin_archivo(archivo, False)
                continue
            
            contenido_procesado, num_reemplazos = processor.procesar(
                contenido, separador_entrada, separador_salida
            )
            
            if num_reemplazos > 0:
                logger.log(f"[OK] Reemplazos realizados: {num_reemplazos}")
            
            file_handler.escribir(ruta_salida, contenido_procesado)
            logger.log(f"[OK] Archivo guardado en {carpeta_salida}/")
            
            logger.fin_archivo(archivo, True)
            archivos_a_eliminar.append(ruta_entrada)
            
        except Exception as e:
            logger.log(f"[ERROR] Error procesando {archivo}: {e}")
            logger.fin_archivo(archivo, False)
    
    return archivos_a_eliminar


# =============================================================================
# EJECUCIÓN INDEPENDIENTE
# =============================================================================

if __name__ == "__main__":
    procesar()