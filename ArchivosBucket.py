import aiohttp
import os
import zipfile
import pandas as pd
import requests
from aiohttp import ClientConnectorError, TCPConnector
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime

# Configuración
package_id_historicos = "5e8bb1f8-f0a5-4719-a877-38543545505b"
package_id_diarios = "ID_DEL_PAQUETE_PARA_DATOS_DIARIOS"
url_diarios_base = "https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint={}"

async def obtener_url_descarga(request):
    try:
        package_id = request.args.get('package_id', default=package_id_historicos, type=str)
        url_api = f"https://datos.gob.cl/api/action/package_show?id={package_id}"
        response = requests.get(url_api)
        response.raise_for_status()
        data = response.json()
        if data["success"]:
            return data["result"]["resources"][0]["url"]
        else:
            raise ValueError(f"Error en la solicitud API: {data}")
    except requests.exceptions.RequestException as e:
        return str(e)

async def descargar_y_descomprimir(request):
    try:
        url_descarga = await obtener_url_descarga(request)
        destino = "/tmp/datos_historicos.zip"
        response = requests.get(url_descarga)
        response.raise_for_status()
        
        with open(destino, 'wb') as archivo:
            archivo.write(response.content)

        fecha_str = url_descarga.split('/')[-1].split('.')[0][-8:]

        with zipfile.ZipFile(destino, 'r') as zip_ref:
            csv_folder = f"/tmp/Archivos_CSV/{fecha_str}"
            diarios_folder = os.path.join(csv_folder, 'Diarios')
            os.makedirs(diarios_folder, exist_ok=True)
            originales_folder = f"/tmp/Historicos_originales/{fecha_str}"
            os.makedirs(originales_folder, exist_ok=True)

            for archivo in zip_ref.namelist():
                ruta_archivo = os.path.join(os.path.dirname(destino), archivo)

                if archivo.endswith('.txt'):
                    destino_txt = os.path.join(originales_folder, f'{os.path.splitext(archivo)[0]}_{fecha_str}.txt')
                    with open(destino_txt, 'wb') as f:
                        f.write(zip_ref.read(archivo))
                elif archivo.endswith('.csv'):
                    destino_csv = os.path.join(csv_folder, f'{os.path.splitext(archivo)[0]}_{fecha_str}.csv')
                    with open(destino_csv, 'wb') as f:
                        f.write(zip_ref.read(archivo))

            convertir_a_csv(originales_folder, csv_folder, fecha_str)
            convertir_a_csv(diarios_folder, csv_folder, fecha_str)

        return f'Descarga y descompresión exitosas para fecha: {fecha_str}'

    except requests.exceptions.RequestException as e:
        return f'Error en la descarga y descompresión: {e}'
    except zipfile.BadZipFile:
        return 'Error: El archivo descargado no es un archivo ZIP válido.'

def convertir_a_csv(directorio_originales, directorio_csv, fecha_str):
    archivos_txt = [archivo for archivo in os.listdir(directorio_originales) if archivo.endswith('.txt')]
    
    for archivo in archivos_txt:
        ruta_archivo_txt = os.path.join(directorio_originales, archivo)
        dataframe = pd.read_csv(ruta_archivo_txt, delimiter='\t')
        ruta_csv = os.path.join(directorio_csv, f'{os.path.splitext(archivo)[0]}.csv')
        dataframe.to_csv(ruta_csv, index=False)

async def obtener_codigos_servicio(request):
    try:
        url = "https://www.red.cl/restservice_v2/rest/getservicios/all"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
    except aiohttp.ClientError as e:
        return None

async def descargar_json_y_convertir_a_csv(request):
    try:
        codigos_servicio = await obtener_codigos_servicio(request)

        if codigos_servicio:
            códigos_fallidos = set(codigos_servicio)

            while códigos_fallidos:
                codigo = códigos_fallidos.pop()
                url = f"https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint={codigo}"

                connector = TCPConnector(limit_per_host=10)
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        dataframe = pd.json_normalize(await response.json())
                        diarios_folder = f"/tmp/Archivos_CSV/Diarios"
                        os.makedirs(diarios_folder, exist_ok=True)
                        ruta_csv = os.path.join(diarios_folder, f'datos_diarios_{codigo}.csv')
                        dataframe.to_csv(ruta_csv, index=False)

        return 'Proceso de descarga y conversión de datos diarios completado'

    except (aiohttp.ClientError, ClientConnectorError) as e:
        return f'Error en la descarga de datos diarios: {e}'

def subir_datos_a_bucket(local_folder, bucket):
    archivos_nuevos = 0
    archivos_actualizados = 0

    def upload_file(local_file, blob_name):
        nonlocal archivos_nuevos, archivos_actualizados
        blob = bucket.blob(blob_name)

        if blob.exists():
            archivos_actualizados += 1
            accion = "actualizado"
        else:
            archivos_nuevos += 1
            accion = "agregado"

        blob.upload_from_filename(local_file)
        print(f"Archivo {local_file} {accion} en {blob_name}.")

    for root, dirs, files in os.walk(local_folder):
        for filename in files:
            local_file_path = os.path.join(root, filename)
            upload_file(local_file_path, filename)

    print(f"Total de archivos nuevos agregados: {archivos_nuevos}")
    print(f"Total de archivos actualizados: {archivos_actualizados}")

def cloud_function_handler(request):
    try:
        # Descargar y procesar datos históricos
        response_historicos = asyncio.run(descargar_y_descomprimir(request))
        print(response_historicos)

        # Configuración de la autenticación
        credentials_path = os.path.join(os.getcwd(), 'tav2024-411600-905e9a2544f7.json')
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        storage_client = storage.Client(credentials=credentials)

        # Obtener el bucket
        bucket_name = 'tavbucket2024'
        bucket = storage_client.get_bucket(bucket_name)

        # Subir datos históricos al bucket
        subir_datos_a_bucket("/tmp", bucket)

        # Descargar y procesar datos diarios
        response_diarios = asyncio.run(descargar_json_y_convertir_a_csv(request))
        print(response_diarios)

        # Subir datos diarios al bucket
        subir_datos_a_bucket("/tmp/Archivos_CSV/Diarios", bucket)

        return 'Proceso completo'

    except Exception as e:
        return f'Error en la ejecución principal: {e}'
