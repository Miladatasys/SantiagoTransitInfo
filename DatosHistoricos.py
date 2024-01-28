import requests
import os
import zipfile
import pandas as pd
from datetime import datetime

def descargar_y_descomprimir(url, directorio_destino):

    fecha_actual = datetime.now().strftime("%Y%m%d")

    try:
        response = requests.get(url)
        response.raise_for_status()  

        nombre_archivo = f'datos_historicos_{fecha_actual}.zip'
        ruta_archivo = os.path.join(directorio_destino, nombre_archivo)
        with open(ruta_archivo, 'wb') as archivo:
            archivo.write(response.content)

        print(f'Descarga exitosa. Archivo guardado en: {ruta_archivo}')


        with zipfile.ZipFile(ruta_archivo, 'r') as zip_ref:
            zip_ref.extractall(directorio_destino)

        print(f'Descompresión exitosa.')


        convertir_a_csv(directorio_destino, fecha_actual)

    except requests.exceptions.RequestException as e:
        print(f'Error en la descarga: {e}')
    except zipfile.BadZipFile:
        print(f'Error: El archivo descargado no es un archivo ZIP válido.')

def convertir_a_csv(directorio_destino, fecha_actual):

    archivos_extraidos = [f for f in os.listdir(directorio_destino) if f.endswith('.txt')]

    # Recorrer los archivos y convertirlos a CSV
    for archivo in archivos_extraidos:
        ruta_archivo_txt = os.path.join(directorio_destino, archivo)
        dataframe = pd.read_csv(ruta_archivo_txt, delimiter='\t')
        ruta_csv = os.path.join(directorio_destino, f'{os.path.splitext(archivo)[0]}_{fecha_actual}.csv')
        dataframe.to_csv(ruta_csv, index=False)
        print(f'Archivo convertido a CSV: {ruta_csv}')

if __name__ == "__main__":
    url_descarga = "https://datos.gob.cl/dataset/5e8bb1f8-f0a5-4719-a877-38543545505b/resource/e4cd8abb-4ac3-4754-9e9c-29be83abad39/download/gtfs-v104-po20231007.zip"
    directorio_destino = "./BigData"

    if not os.path.exists(directorio_destino):
        os.makedirs(directorio_destino)

    descargar_y_descomprimir(url_descarga, directorio_destino)
