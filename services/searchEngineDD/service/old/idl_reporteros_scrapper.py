import asyncio
import logging
import json
from async_ip_rotator import IpRotator, ClientSession
from bs4 import BeautifulSoup
from lxml import etree
from time import perf_counter
import re
import ssl
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from utils import request_status_counter
from collections import defaultdict

# set logging to get information about API creation and deletion
logging.basicConfig(level=logging.INFO)

# async def scraping_adverse_media_batch(entities_set: list[tuple[str, str, str]]):
#     try:
#         _site = 'https://www.google.com'


#         async with _gateway_instance as ip_rotator:
#             tasks = [scraping_adverse_media(entity[0], entity[1], entity[2], ip_rotator) for entity in entities_set]
#             tasks_data = await asyncio.gather(*tasks)

#         return tasks_data
#     except Exception as e:
#         logging.error(f"An error has ocurred in Adverse Media Finding: {e}")
#         raise Exception("Internal Adverse Media Error Finding")

# async def main2():

#     _site = 'https://www.google.com'

#     start = perf_counter()


#     async with _gateway_instance as ip_rotator:
#         tasks = [scraping_adverse_media("A+A ASSURANCE SERVICES LTD", "A+A ASSURANCE SERVICES LTD", ip_rotator),
#                 scraping_adverse_media("Miguel caceres aciendas", "Miguel caceres aciendas", ip_rotator),
#                 scraping_adverse_media("Abimael Guzman", "Abimael Guzman", ip_rotator),
#                 scraping_adverse_media("Ollanta Humala", "Ollanta Humala", ip_rotator),
#                 scraping_adverse_media("Lucas Podesta", "Lucas Podesta", ip_rotator),
#                 scraping_adverse_media("Alexis Coria", "Alexis Coria", ip_rotator),
#                 scraping_adverse_media("Pedro Castillo", "Pedro Castillo", ip_rotator),
#                 scraping_adverse_media("Keiko Fujimori", "Keiko Fujimori", ip_rotator),
#                 scraping_adverse_media("CESAR ACUÑA PERALTA", "CESAR ACUÑA PERALTA", ip_rotator),
#                 scraping_adverse_media("Jalil Mukulu", "Jalil Mukulu", ip_rotator)
#                 ]
#         tasks_data = await asyncio.gather(*tasks)

#     end = perf_counter()
#     print(f"Elapsed time during the request: {end-start:.2f}s")

#     for index, data in enumerate(tasks_data):
#         with open(f'output{index}.json', 'w', encoding="UTF-8") as json_file:
#             json.dump(data, json_file, indent=4)

def parsear_fecha_IDL_REPORTEROS(fecha_str: str) -> str:
    ahora = datetime.now()
    if "days ago" in fecha_str:
        dias = int(fecha_str.split()[0])
        fecha_calculada = ahora - timedelta(days=dias)
        return fecha_calculada.strftime('%Y-%m-%d')
    elif "hours ago" in fecha_str:
        horas = int(fecha_str.split()[0])
        fecha_calculada = ahora - timedelta(hours=horas)
        return fecha_calculada.strftime('%Y-%m-%d')
    elif "minutes ago" in fecha_str:
        minutos = int(fecha_str.split()[0])
        fecha_calculada = ahora - timedelta(minutes=minutos)
        return fecha_calculada.strftime('%Y-%m-%d')
    else:
        # Asumimos que la fecha está en formato "mes día, año"
        partes = fecha_str.replace(',', '').split()
        if len(partes) == 3:
            mes, dia, anho = partes
            meses = {
                "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
                "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
            }
            mes = meses.get(mes, mes)
            return f"{anho}-{mes}-{dia.zfill(2)}"
        return fecha_str

def extrae_informacion(objeto: etree._Element, xpath: str) -> list:
    if objeto is None:
        return []
    xpath_objeto = objeto.xpath(xpath)
    xpath_lista = xpath_objeto if isinstance(xpath_objeto, list) else [xpath_objeto]
    return xpath_lista

async def consulta_pagina_web(session: ClientSession, url: str, header: dict, keyword: str, dict_tracker) -> str:
    try:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        response = await session.get(url, headers=header, timeout=30, ssl=ssl_context)
        request_status_counter.agregar_o_actualizar(dict_tracker, str(response.status))
        return await response.text(), url, keyword
    except asyncio.TimeoutError as e:
        return {
            'status': 'Error',
            'description': str(e),
            'url': url,
            'content': None
        }, url, keyword
    except Exception as e:
        logging.error(f"Error for URL {url}: {e}")
        return {
            'status': 'Error',
            'description': str(e),
            'url': url,
            'content': None
        }, url, keyword


def limpiar_titulo(titulo: str) -> str:
    patron_enlace = r'https?://[^\s›]+'
    titulo_limpio = re.sub(patron_enlace, '', titulo)
    return titulo_limpio.strip()

async def google_search_async(lista_criterios_busqueda: list[tuple[str, str]], xpath: str, sess: ClientSession, dict_tracker) -> list:
    
    _header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0"}
    tasks = [consulta_pagina_web(sess, f'https://www.google.com/search?q=allintext%3A{quote(criterio_busqueda)}+site:idl-reporteros.pe&num=50&tbs=cdr:1,cd_min:1/1/2019&source=lnt', _header, keyword, dict_tracker) for keyword, criterio_busqueda in lista_criterios_busqueda]
    return await asyncio.gather(*tasks)

def formatting_results(results: list, _xpath_base: str, _xpath_titulo: str, _xpath_enlace: str, _xpath_fecha: str, _xpath_fuente: str) -> list:
    data_total = []
    for resultado, url, keyword in results:

        if isinstance(resultado, dict):
            # Skipping bad request
            continue

        # Fomating row data to html format
        resultado_to_dict = {
            'status': 200,  # Hardcodeado
            'description': 'Ok',  # Hardcodeado
            'url': url,
            'content': etree.HTML(resultado)
        }

        #Extracting relevant information from html
        informacion = extrae_informacion(resultado_to_dict['content'], _xpath_base)
        resultado_to_dict['content'] = informacion

        #Formating data to pd.DataFrame
        Nueva_Data = [
                {
                    'Fuente': resultado_content.xpath(_xpath_fuente)[0].xpath('string(.)') if resultado_content.xpath(_xpath_fuente) else 'IDL Reporteros',
                    'Titulo': limpiar_titulo(resultado_content.xpath(_xpath_titulo)[0].xpath('string(.)')) if resultado_content.xpath(_xpath_titulo) else '',
                    'Fecha': parsear_fecha_IDL_REPORTEROS(resultado_content.xpath(_xpath_fecha)[0].xpath('string(.)') if resultado_content.xpath(_xpath_fecha) else ''),
                    'URL': resultado_content.xpath(_xpath_enlace)[0] if resultado_content.xpath(_xpath_enlace) else '',
                    'Keyword': keyword,
                    'RequestStatus': resultado_to_dict['status'],
                    'DescriptionStatus': resultado_to_dict['description']
                } for resultado_content in resultado_to_dict['content']
            ]
        
        data_total.extend(Nueva_Data)
    
    return data_total

async def scraping_IDL_reporteros(sess: ClientSession, nombre_comercial: str, razon_social: str, entityIdNumber: str):
    dict_tracker = defaultdict(request_status_counter.default_value)
    # print("Llame a adverse Media")
    # print(f"IpRotator es {gateway_instance}")
    # print(f"Nombre Commercial es {nombre_comercial}")
    # print(f"Razon social es {razon_social}")
    # _keyword_list = ['judicial', 'denuncia', 'sanción', 'fraude', 'estafa', 'corrupción', 'coima', 'soborno', 'colusión', 'lavado de activos', 'financiamineto del terrorismo', 'malversación', 'gobierno', 'escándalo', 'demanda', 'esquema', 'quiebra', 'ilegal', 'lavado de dinero', 'investigación', 'crimen', 'arresto', 'terror', 'contrabando', 'evasión', 'violar', 'sunafil']
    _keyword_list = ['denuncia', 'delito', 'corrupción', 'soborno', 'lavado de activos', 'financiamiento del terrorismo', 'financiamiento a la proliferación de armas de destrucción masiva']
    _xpath_base = '//div[@class="N54PNb BToiNc"]'
    _xpath_titulo = './/h3[@class="LC20lb MBeuO DKV0Md"]'
    _xpath_enlace = './/a[@jsname="UWckNb"]/@href'
    _xpath_fecha = './/span[@class="LEwnzc Sqrs4e"]/span'
    _xpath_fuente = './/span[@class="VuuXrf"]'

    nombre_comercial_original = nombre_comercial
    razon_social_original = razon_social

    #logging.info(f'La razon social es: {razon_social}')
    #logging.info(f'Nombre comercial es: {nombre_comercial}')

    lista_criterios_busqueda = []
    if nombre_comercial:
        nombre_comercial = nombre_comercial.replace(' ', '+')
        lista_criterios_busqueda += [
            (keyword, f'"{nombre_comercial}"+"{keyword.replace(" ", "+")}"') for keyword in _keyword_list
        ]
    lista_criterios_busqueda = list(set(lista_criterios_busqueda))
    resultados = []
    try:
        logging.info(f'Starting "IDL reporteros" search process for entity "{nombre_comercial}"')
        resultados = await google_search_async(lista_criterios_busqueda, xpath=_xpath_base, sess=sess, dict_tracker=dict_tracker)
        entityResponse = {
            "entityIdNumber": entityIdNumber,
            "name": razon_social_original,
            "commercialName": nombre_comercial_original,
            "requestStatus": 200,
            "results": [],
            'createdOn': datetime.now(timezone.utc).isoformat(),
            'updatedOn': datetime.now(timezone.utc).isoformat()
        }

        entityResponse['results'] = formatting_results(resultados, _xpath_base, _xpath_titulo, _xpath_enlace, _xpath_fecha, _xpath_fuente)
        logging.info(f'Requests status counter in "IDL reporteros" from "{nombre_comercial}": {dict_tracker}')
        logging.info(f'Successfully finishing "IDL reporteros" search process for entity "{nombre_comercial}"')
        # json_data = json.dumps(data_total)
        return entityResponse
    
    except Exception as e:
        logging.error(f"Finishing with errors: {str(e)}")
        return {
            "entityIdNumber": entityIdNumber,
            "name": razon_social_original,
            "commercialName": nombre_comercial_original,
            "requestStatus": 400,
            "results": [],
            'createdOn': datetime.now(timezone.utc).isoformat(),
            'updatedOn': datetime.now(timezone.utc).isoformat()
        }
    


if __name__ == '__main__':
    pass
    # asyncio.run(main2())
