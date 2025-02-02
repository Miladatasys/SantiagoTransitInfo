import requests
import os
import zipfile
import pandas as pd
from datetime import datetime

def obtener_url_descarga(package_id, year):
    url_api = f"https://datos.gob.cl/api/action/package_show?id={package_id}"
    response = requests.get(url_api)
    response.raise_for_status()
    data = response.json()

    # Verificar si la solicitud fue exitosa
    if data["success"]:
        # Filtrar recursos por año
        resources = data["result"]["resources"]
        filtered_resources = [res for res in resources if f"{year}" in res.get("name", "")]
        
        if filtered_resources:
            # Ordenar recursos por fecha de creación (más reciente primero)
            sorted_resources = sorted(filtered_resources, key=lambda x: x.get("created", ""), reverse=True)
            # Obtener la URL de descarga del recurso más reciente
            return sorted_resources[0]["url"]
        else:
            print(f"No hay recursos para el año {year}.")
            return None
    else:
        raise ValueError(f"Error en la solicitud API: {data}")

def descargar_y_descomprimir(url, destino):
    try:
        # Extraer la fecha de la URL
        fecha_str = url.split('/')[-1].split('.')[0][-8:]
        # Obtener la fecha actual
        fecha_actual = datetime.now().strftime("%Y%m%d")

        # Realizar solicitud HTTP GET
        response = requests.get(url)
        response.raise_for_status()  # Verificar si la solicitud fue exitosa

        with open(destino, 'wb') as archivo:
            archivo.write(response.content)

        print(f'Descarga exitosa. Archivo guardado en: {destino}')

        # Descomprimir el archivo ZIP
        with zipfile.ZipFile(destino, 'r') as zip_ref:
            # Crear la carpeta "Archivos CSV" dentro del directorio de destino
            csv_folder = os.path.join(os.path.dirname(destino), 'Archivos CSV')
            os.makedirs(csv_folder, exist_ok=True)

            # Crear la carpeta "Históricos originales" dentro del directorio de destino
            originales_folder = os.path.join(os.path.dirname(destino), 'Históricos originales')
            os.makedirs(originales_folder, exist_ok=True)

            # Extraer archivos y organizar según su extensión
            for archivo in zip_ref.namelist():
                ruta_archivo = os.path.join(os.path.dirname(destino), archivo)

                if archivo.endswith('.txt'):
                    # Mover archivos TXT a la carpeta "Históricos originales"
                    destino_txt = os.path.join(originales_folder, f'{os.path.splitext(archivo)[0]}_{fecha_str}.txt')
                    with open(destino_txt, 'wb') as f:
                        f.write(zip_ref.read(archivo))
                elif archivo.endswith('.csv'):
                    # Mover archivos CSV a la carpeta "Archivos CSV" y agregar fecha al nombre
                    destino_csv = os.path.join(csv_folder, f'{os.path.splitext(archivo)[0]}_{fecha_str}.csv')
                    with open(destino_csv, 'wb') as f:
                        f.write(zip_ref.read(archivo))

        print(f'Descompresión exitosa.')

        # Convertir archivos TXT a CSV
        convertir_a_csv(originales_folder, csv_folder, fecha_str)

    except requests.exceptions.RequestException as e:
        print(f'Error en la descarga y descompresión: {e}')
    except zipfile.BadZipFile:
        print(f'Error: El archivo descargado no es un archivo ZIP válido.')

def convertir_a_csv(directorio_originales, directorio_csv, fecha_str):
    archivos_txt = [archivo for archivo in os.listdir(directorio_originales) if archivo.endswith('.txt')]

    # Recorrer los archivos TXT y convertirlos a CSV
    for archivo in archivos_txt:
        ruta_archivo_txt = os.path.join(directorio_originales, archivo)
        dataframe = pd.read_csv(ruta_archivo_txt, delimiter='\t')

        # Crear la ruta para guardar el archivo CSV dentro de la carpeta "Archivos CSV" con la fecha
        ruta_csv = os.path.join(directorio_csv, f'{os.path.splitext(archivo)[0]}_{fecha_str}.csv')

        dataframe.to_csv(ruta_csv, index=False)
        print(f'Archivo convertido a CSV: {ruta_csv}')

if __name__ == "__main__":
    # Obtener la URL de descarga desde la API usando el package_id
    package_id = "5e8bb1f8-f0a5-4719-a877-38543545505b"
    directorio_destino = "./BigData"

    for year in [2023, 2022, 2018]:
        url_descarga = obtener_url_descarga(package_id, year)

        if url_descarga:
            nombre_archivo = f"datos_historicos_{year}.zip"
            ruta_archivo = os.path.join(directorio_destino, nombre_archivo)

            if not os.path.exists(directorio_destino):
                os.makedirs(directorio_destino)

            descargar_y_descomprimir(url_descarga, ruta_archivo)
