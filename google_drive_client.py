"""Cliente para descargar archivos desde Google Drive."""

import os
import io
from typing import List, Optional
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# Permisos que necesitamos (solo lectura)
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


class GoogleDriveClient:
    """Cliente para conectarse a Google Drive y descargar archivos."""

    def __init__(self, credentials_path: str = 'config/credentials.json',
                 token_path: str = 'config/token.json'):
        """
        Inicializa el cliente de Google Drive.
        
        Args:
            credentials_path: Ruta al archivo credentials.json de Google Cloud
            token_path: Ruta donde se guardará el token de autenticación
        """
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.service = None

    def autenticar(self) -> None:
        """Autentica con Google Drive. La primera vez abrirá el navegador."""
        creds = None

        # Verificar si ya tenemos un token guardado
        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)

        # Si no hay credenciales válidas, autenticar
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                # Refrescar token expirado
                print("[DRIVE] Refrescando token de autenticación...")
                creds.refresh(Request())
            else:
                # Primera autenticación (abre navegador)
                if not os.path.exists(self.credentials_path):
                    raise FileNotFoundError(
                        f"No se encontró {self.credentials_path}. "
                        "Descárgalo desde Google Cloud Console."
                    )
                
                print("[DRIVE] Abriendo navegador para autenticación...")
                print("[DRIVE] Autoriza la aplicación en tu navegador.")
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, SCOPES
                )
                creds = flow.run_local_server(port=0)

            # Guardar token para próximas ejecuciones
            with open(self.token_path, 'w') as token:
                token.write(creds.to_json())
            print(f"[DRIVE] Token guardado en {self.token_path}")

        # Crear servicio de Drive
        self.service = build('drive', 'v3', credentials=creds)
        print("[DRIVE] Conectado a Google Drive exitosamente")

    def obtener_id_carpeta(self, ruta_carpeta: str) -> Optional[str]:
        """
        Obtiene el ID de una carpeta dada su ruta.
        
        Args:
            ruta_carpeta: Ruta tipo "MiCarpeta/Subcarpeta/Archivos"
                          o directamente el ID de la carpeta
        
        Returns:
            ID de la carpeta o None si no existe
        """
        if not self.service:
            raise RuntimeError("Debes llamar autenticar() primero")

        # Si es un ID directo (cadena larga sin /), usarlo directamente
        if '/' not in ruta_carpeta and len(ruta_carpeta) > 20:
            return ruta_carpeta

        partes = ruta_carpeta.strip('/').split('/')
        parent_id = 'root'  # Empezar desde la raíz de Drive

        for parte in partes:
            query = (
                f"name = '{parte}' and "
                f"'{parent_id}' in parents and "
                f"mimeType = 'application/vnd.google-apps.folder' and "
                f"trashed = false"
            )
            
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()

            archivos = results.get('files', [])
            
            if not archivos:
                print(f"[DRIVE ERROR] No se encontró la carpeta: {parte}")
                return None
            
            parent_id = archivos[0]['id']

        return parent_id

    def listar_archivos(self, carpeta_id: str, 
                        extensiones: List[str] = None) -> List[dict]:
        """
        Lista archivos en una carpeta de Drive.
        
        Args:
            carpeta_id: ID de la carpeta en Drive
            extensiones: Lista de extensiones a filtrar (ej: ['.csv', '.txt'])
        
        Returns:
            Lista de diccionarios con info de archivos {id, name, mimeType}
        """
        if not self.service:
            raise RuntimeError("Debes llamar autenticar() primero")

        query = f"'{carpeta_id}' in parents and trashed = false"
        
        results = self.service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, mimeType, size)'
        ).execute()

        archivos = results.get('files', [])

        # Filtrar por extensión si se especificó
        if extensiones:
            archivos = [
                a for a in archivos 
                if any(a['name'].lower().endswith(ext.lower()) for ext in extensiones)
            ]

        return archivos

    def descargar_archivo(self, archivo_id: str, destino: str) -> bool:
        """
        Descarga un archivo de Drive.
        
        Args:
            archivo_id: ID del archivo en Drive
            destino: Ruta local donde guardar el archivo
        
        Returns:
            True si se descargó exitosamente
        """
        if not self.service:
            raise RuntimeError("Debes llamar autenticar() primero")

        try:
            request = self.service.files().get_media(fileId=archivo_id)
            
            # Crear directorio destino si no existe
            Path(destino).parent.mkdir(parents=True, exist_ok=True)

            with io.FileIO(destino, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()

            return True

        except Exception as e:
            print(f"[DRIVE ERROR] Error descargando archivo: {e}")
            return False

    def descargar_carpeta(self, ruta_drive: str, carpeta_local: str,
                          extensiones: List[str] = None) -> int:
        """
        Descarga todos los archivos de una carpeta de Drive.
        
        Args:
            ruta_drive: Ruta de la carpeta en Drive (ej: "Migracion/Archivos")
                        o directamente el ID de la carpeta
            carpeta_local: Carpeta local donde guardar (ej: "raw")
            extensiones: Extensiones a descargar (ej: ['.csv', '.txt'])
        
        Returns:
            Número de archivos descargados
        """
        print(f"\n[DRIVE] Buscando carpeta: {ruta_drive}")
        
        # Obtener ID de la carpeta
        carpeta_id = self.obtener_id_carpeta(ruta_drive)
        if not carpeta_id:
            print(f"[DRIVE ERROR] No se encontró la carpeta: {ruta_drive}")
            return 0

        # Mostrar ID parcial para confirmar
        if ruta_drive == carpeta_id:
            print(f"[DRIVE] Usando ID de carpeta directamente")
        else:
            print(f"[DRIVE] Carpeta encontrada (ID: {carpeta_id[:8]}...)")

        # Listar archivos
        archivos = self.listar_archivos(carpeta_id, extensiones)
        
        if not archivos:
            print("[DRIVE] No hay archivos para descargar")
            return 0

        print(f"[DRIVE] Encontrados {len(archivos)} archivos")

        # Crear carpeta local
        os.makedirs(carpeta_local, exist_ok=True)

        # Limpiar carpeta local
        for archivo_existente in os.listdir(carpeta_local):
            ruta = os.path.join(carpeta_local, archivo_existente)
            if os.path.isfile(ruta):
                os.remove(ruta)
        
        print(f"[DRIVE] Carpeta {carpeta_local}/ limpiada")

        # Descargar cada archivo
        descargados = 0
        for archivo in archivos:
            nombre = archivo['name']
            destino = os.path.join(carpeta_local, nombre)
            
            print(f"  - Descargando: {nombre}...", end=" ")
            
            if self.descargar_archivo(archivo['id'], destino):
                size_kb = os.path.getsize(destino) / 1024
                print(f"OK ({size_kb:.1f} KB)")
                descargados += 1
            else:
                print("ERROR")

        print(f"\n[DRIVE] Descargados {descargados}/{len(archivos)} archivos a {carpeta_local}/")
        return descargados


# =============================================================================
# FUNCIÓN DE PRUEBA
# =============================================================================

if __name__ == "__main__":
    """Prueba el cliente de Google Drive"""
    
    # Cargar variables de entorno
    from dotenv import load_dotenv
    load_dotenv()
    
    # Obtener configuración
    ruta_drive = os.getenv('GOOGLE_DRIVE_CARPETA', '')
    
    if not ruta_drive:
        print("ERROR: Configura GOOGLE_DRIVE_CARPETA en tu archivo .env")
        print("Ejemplo: GOOGLE_DRIVE_CARPETA=1j05emaTqisA2nUvtPwemN7Jyq5uNM57V")
        exit(1)
    
    print("=" * 60)
    print("PRUEBA DE CONEXIÓN A GOOGLE DRIVE")
    print("=" * 60)
    
    try:
        # Crear cliente
        cliente = GoogleDriveClient(
            credentials_path=os.getenv('GOOGLE_CREDENTIALS', 'config/credentials.json'),
            token_path=os.getenv('GOOGLE_TOKEN', 'config/token.json')
        )
        
        # Autenticar
        cliente.autenticar()
        
        # Descargar archivos
        descargados = cliente.descargar_carpeta(
            ruta_drive=ruta_drive,
            carpeta_local='raw',
            extensiones=['.csv', '.txt', '.CSV', '.TXT']
        )
        
        print("\n" + "=" * 60)
        if descargados > 0:
            print(f"ÉXITO: {descargados} archivos descargados en raw/")
        else:
            print("ADVERTENCIA: No se descargaron archivos")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        raise