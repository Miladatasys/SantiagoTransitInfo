import aiohttp
import asyncio
import os
import pandas as pd
from aiohttp import ClientConnectorError, TCPConnector

async def obtener_codigos_servicio(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
    except aiohttp.ClientError as e:
        print(f'Error en la descarga de códigos de servicio: {e}')
        return None

async def descargar_json_y_convertir_a_csv(url, codigo, directorio_destino, códigos_fallidos):
    try:
        connector = TCPConnector(limit_per_host=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url) as response:
                response.raise_for_status()

                dataframe = pd.json_normalize(await response.json())

                ruta_csv = os.path.join(directorio_destino, f'datos_diarios_{codigo}.csv')
                dataframe.to_csv(ruta_csv, index=False)
                print(f'Archivo CSV creado para código {codigo}')

                códigos_fallidos.discard(codigo)

    except (aiohttp.ClientError, ClientConnectorError) as e:
        print(f'Error al procesar código {codigo}: {e}')
        códigos_fallidos.add(codigo)

async def main():
    url_codigos_servicio = "https://www.red.cl/restservice_v2/rest/getservicios/all"
    url_base_conocerecorrido = "https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint="
    directorio_destino = "./DatosDiarios"

    codigos_servicio = await obtener_codigos_servicio(url_codigos_servicio)

    if codigos_servicio:
        if not os.path.exists(directorio_destino):
            os.makedirs(directorio_destino)

        codigos_servicio_set = set(codigos_servicio)
        códigos_fallidos = codigos_servicio_set.copy()

        while códigos_fallidos:
            tasks = [descargar_json_y_convertir_a_csv(url_base_conocerecorrido + codigo, codigo, directorio_destino, códigos_fallidos) for codigo in códigos_fallidos]
            await asyncio.gather(*tasks)

            print(f"Descarga completada. Códigos fallidos restantes: {len(códigos_fallidos)}")

if __name__ == "__main__":
    asyncio.run(main())