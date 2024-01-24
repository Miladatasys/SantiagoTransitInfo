import requests
import os
import zipfile
import pandas as pd

def descargar_y_descomprimir(url, destino):
    try:
        # Realizar solicitud HTTP GET
        response = requests.get(url)
        response.raise_for_status()  # Verificar si la solicitud fue exitosa


        with open(destino, 'wb') as archivo:
            archivo.write(response.content)

        print(f'Descarga exitosa. Archivo guardado en: {destino}')

        # Descomprimir el archivo ZIP
        with zipfile.ZipFile(destino, 'r') as zip_ref:
            zip_ref.extractall(os.path.dirname(destino))

        print(f'Descompresión exitosa.')


        convertir_a_csv(destino)

    except requests.exceptions.RequestException as e:
        print(f'Error en la descarga: {e}')
    except zipfile.BadZipFile:
        print(f'Error: El archivo descargado no es un archivo ZIP válido.')

def convertir_a_csv(ruta_archivo_zip):
    # Descomprimir el archivo ZIP
    with zipfile.ZipFile(ruta_archivo_zip, 'r') as zip_ref:
        zip_ref.extractall(os.path.dirname(ruta_archivo_zip))


    archivos_extraidos = zip_ref.namelist()

    # Recorrer los archivos y convertirlos a CSV
    for archivo in archivos_extraidos:
        if archivo.endswith('.txt'): 
            ruta_archivo_txt = os.path.join(os.path.dirname(ruta_archivo_zip), archivo)
            dataframe = pd.read_csv(ruta_archivo_txt, delimiter='\t') 
            ruta_csv = os.path.splitext(ruta_archivo_txt)[0] + '.csv'
            dataframe.to_csv(ruta_csv, index=False)
            print(f'Archivo convertido a CSV: {ruta_csv}')

if __name__ == "__main__":
    url_descarga = "https://datos.gob.cl/dataset/5e8bb1f8-f0a5-4719-a877-38543545505b/resource/e4cd8abb-4ac3-4754-9e9c-29be83abad39/download/gtfs-v104-po20231007.zip"

    directorio_destino = "./BigData"

    nombre_archivo = "datos_historicos.zip"

    # Ruta completa del archivo ZIP
    ruta_archivo = os.path.join(directorio_destino, nombre_archivo)


    if not os.path.exists(directorio_destino):
        os.makedirs(directorio_destino)

    descargar_y_descomprimir(url_descarga, ruta_archivo)