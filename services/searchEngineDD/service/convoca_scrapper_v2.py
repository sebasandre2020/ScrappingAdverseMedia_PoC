import random
import asyncio
import logging
import json
from async_ip_rotator import IpRotator, ClientSession
from bs4 import BeautifulSoup
from lxml import etree
from time import perf_counter
import re
import pandas as pd
import ssl
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from utils import request_status_counter
from collections import defaultdict
from utils.constants import ENDPOINT_URL, ENDPOINT_TIMEOUT, MAX_SEARCH_RESULTS, MAX_RETRIES, RETRY_DELAY, EXPONENTIAL_BACKOFF, REQUEST_SLEEP_TIME, USER_AGENTS
from utils.decorators import set_random_user_agent


ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# set logging to get information about API creation and deletion
logging.basicConfig(level=logging.INFO)
# async def scraping_adverse_media_batch(entities_set: list[tuple[str, str, str]]):
#     try:
#         _site = 'https://www.bing.com'


#         async with _gateway_instance as ip_rotator:
#             tasks = [scraping_adverse_media(entity[0], entity[1], entity[2], ip_rotator) for entity in entities_set]
#             tasks_data = await asyncio.gather(*tasks)

#         return tasks_data
#     except Exception as e:
#         logging.error(f"An error has ocurred in Convoca Finding: {e}")
#         raise Exception("Internal Convoca Error Finding")

# async def main2():

#     _site = 'https://www.bing.com'

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

def parsear_fecha(fecha_str: str) -> str:
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

async def consulta_pagina_web(session: ClientSession, url: str, header: dict, keyword: str, dict_tracker, limit) -> str:
    max_retries = MAX_RETRIES
    retry_delay = RETRY_DELAY
    sleep_time = REQUEST_SLEEP_TIME

    logging_done = False
    for attempt in range(max_retries):
        try:
            async with limit:
                response = await session.get(url, headers=header, timeout=30, ssl=ssl_context)
                if response.status == 404:
                    logging.warning(f'For <Convoca>: {response.status}: {response.reason}')
                    logging.warning(f'For <Convoca>: URL: {url} - Retrying in {retry_delay} seconds...')
                    await asyncio.sleep(retry_delay)
                    retry_delay *= EXPONENTIAL_BACKOFF
                    continue  # Retry the request
                # Log the IP address being used for debugging rotation issues
                resp_text = await response.text()
                #if
                if not logging_done:
                    ip_address = header.get("X-Forwarded-For", "Unknown (Direct Request)")
                    logging.info(f'For <Convoca>: Current Response:  {response.host}')
                    logging.info(f'For <Convoca>: Current URL in Rotative IP: {response.real_url}')
                    logging.info(f"Response Text (truncated): {resp_text[:500]}...")
                    logging_done = True
                if any(substr in resp_text for substr in [
                    "No hay resultados para",
                    "There are no results for",
                    "Check your spelling or try different keywords"
                ]) and attempt <= 2:
                    header = set_random_user_agent(header)
                    continue
                # Google CAPTCHA / Blocking Detection
                if any(substring in resp_text for substring in [
                    "Our systems have detected unusual traffic",
                    'Please click <a href="/httpservice/retry/enablejs?',
                    "If you're having trouble accessing Google Search",
                    "Please verify you are a human",
                    "To continue, please enter the characters you see below"
                ]):
                    logging.warning(f'For <Convoca>: Google CAPTCHA detected for URL: {url}')
                    request_status_counter.agregar_o_actualizar(dict_tracker, "500")
                else:
                    request_status_counter.agregar_o_actualizar(dict_tracker, str(response.status))
                await asyncio.sleep(sleep_time)
                
                return await response.text(), url, keyword
        except asyncio.TimeoutError as e:
            return {
                'status': 'Error',
                'description': str(e),
                'url': url,
                'content': None
            }, url, keyword
        except Exception as e:
            logging.error(f"For <Convoca> Error for URL {url}: {e}")
            return {
                'status': 'Error',
                'description': str(e),
                'url': url,
                'content': None
            }, url, keyword
            
    logging.error(f"For <Convoca> Max retries reached for URL {url}. Failing the request.")
    request_status_counter.agregar_o_actualizar(dict_tracker, "404")
    return {
        'status': 'Error',
        'description': 'Max retries reached',
        'url': url,
        'content': None
    }, url, keyword

def limpiar_titulo(titulo: str) -> str:
    patron_enlace = r'https?://[^\s›]+'
    titulo_limpio = re.sub(patron_enlace, '', titulo)
    return titulo_limpio.strip()

async def google_search_async(lista_criterios_busqueda: list[tuple[str, str]], xpath: str, sess: ClientSession, dict_tracker, limit) -> list:
    
    _header = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.bing.com/",
        "DNT": "1",  # Do Not Track request
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
    }
    tasks = [
        consulta_pagina_web(
            sess,
            f'https://www.bing.com/search?q={quote(criterio_busqueda, safe="+%22")}+site:convoca.pe&num={MAX_SEARCH_RESULTS}&tbs=cd_min:01/01/2019&hl=es&gl=PE',
            _header,
            keyword,
            dict_tracker,
            limit
        )
        for keyword, criterio_busqueda in lista_criterios_busqueda
    ]

    return await asyncio.gather(*tasks)

def formatting_results(results: list, _xpath_empty: str, _xpath_base: str, _xpath_titulo: str, _xpath_enlace: str, _xpath_fecha: str, _xpath_fuente: str, nombre_comercial_original: str) -> list:
    
    def clean_xpath_fuente(fuente: str) -> str:
        if fuente:
            
            if '›' in fuente:
                return fuente.split('›', 1)[0].strip() 
            else:
                return fuente.strip()
        return ''
    
    data_total = []
    resultados_por_keyword = dict()
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
        
        if resultado_to_dict['content'].xpath(_xpath_empty):
            resultados_por_keyword[keyword] = []
            continue

        #Extracting relevant information from html
        informacion = extrae_informacion(resultado_to_dict['content'], _xpath_base)
        resultado_to_dict['content'] = informacion

        #Formating data to pd.DataFrame
        Nueva_Data = [
                {
                    'Fuente': 'Convoca',
                    'Sitio': clean_xpath_fuente(resultado_content.xpath(_xpath_fuente)[0].xpath('string(.)')) if resultado_content.xpath(_xpath_fuente) else '',
                    'Nombre o Razón Social': nombre_comercial_original,
                    'Titulo': limpiar_titulo(resultado_content.xpath(_xpath_titulo)[0].xpath('string(.)')) if resultado_content.xpath(_xpath_titulo) else '',
                    'Fecha': resultado_content.xpath(_xpath_fecha)[0].xpath('string(.)') if resultado_content.xpath(_xpath_fecha) else '',
                    'URL': resultado_content.xpath(_xpath_enlace)[0] if resultado_content.xpath(_xpath_enlace) else '',
                    'Keyword': keyword,
                    'RequestStatus': resultado_to_dict['status'],
                    'DescriptionStatus': resultado_to_dict['description']
                } for resultado_content in resultado_to_dict['content']
                if (
                    (url := resultado_content.xpath(_xpath_enlace)[0] if resultado_content.xpath(_xpath_enlace) else '') and
                    '/tags/' not in url and
                    '-' in url.split('/')[-1] and  # Debe contener un guion en la última parte de la URL
                    'convoca-a-tu-servicio' not in url and  # Excluir URLs específicas
                    'convoca-radio' not in url
                )
            ]
        
        resultados_por_keyword[keyword] = Nueva_Data
    
    cant_total_evaluation = 0
    for keyword in resultados_por_keyword.keys():
        resultados_validos = resultados_por_keyword[keyword]
        
        if len(resultados_validos) == 0:
            continue
        
        # # Convertir las fechas a datetime y filtrar
        # for resultado in resultados_validos:
        #     try:
        #         resultado['Fecha'] = datetime.strptime(resultado['Fecha'], '%Y-%m-%d')
        #     except ValueError:
        #         resultado['Fecha'] = None
        
        # # Filtrar por fecha y ordenar
        # resultados_validos = [r for r in resultados_validos if r['Fecha'] is None or r['Fecha'] >= datetime(2019, 1, 1)]
        # resultados_validos.sort(key=lambda x: (x['Fecha'] is not None, x['Fecha']), reverse=True)
        
        # Remove results with an empty 'Titulo'
        resultados_validos = [resultado for resultado in resultados_validos 
                            if (resultado['Titulo'] != ""
                            and str(resultado['Fecha']).strip() != "We cannot provide a description for this page right now")]
        resultados_validos = [resultado for resultado in resultados_validos if "convoca.pe" in resultado['Sitio']]
        
        # Seleccionar los primeros 10 resultados válidos por keyword
        resultados_validos = resultados_validos[:10]
        
        # Convertir las fechas de nuevo a string y agregar el contador
        # for idx, resultado in enumerate(resultados_validos, start=1):
        #     if resultado['Fecha'] is not None:
        #         resultado['Fecha'] = resultado['Fecha'].strftime('%Y-%m-%d')
        #     else:
        #         resultado['Fecha'] = 'N/A'
            #resultado['Resultado N°'] = idx
        cant_total_evaluation += len(resultados_validos)
        data_total.extend(resultados_validos)
    logging.info(f'Total amount of results in <Convoca>: {cant_total_evaluation}')
    
    return data_total

async def web_scraper_convoca(sess: ClientSession, nombre_comercial: str, razon_social: str, entityIdNumber: str):
    dict_tracker = defaultdict(request_status_counter.default_value)
    # print("Llame a Convoca")
    # print(f"IpRotator es {gateway_instance}")
    # print(f"Nombre Commercial es {nombre_comercial}")
    # print(f"Razon social es {razon_social}")
    # _keyword_list = ['judicial', 'denuncia', 'sanción', 'fraude', 'estafa', 'corrupción', 'coima', 'soborno', 'colusión', 'lavado de activos', 'financiamineto del terrorismo', 'malversación', 'gobierno', 'escándalo', 'demanda', 'esquema', 'quiebra', 'ilegal', 'lavado de dinero', 'investigación', 'crimen', 'arresto', 'terror', 'contrabando', 'evasión', 'violar', 'sunafil']
    _keyword_list = ['denuncia', 'delito', 'corrupción', 'soborno', 'lavado de activos', 'financiamiento del terrorismo', 'financiamiento a la proliferación de armas de destrucción masiva']
    
    # For Google
    # _xpath_empty = '//div[contains(text(), "No results found for:")]'
    # _xpath_base = '//div[@class="N54PNb BToiNc"]'
    # _xpath_titulo = './/h3[@class="LC20lb MBeuO DKV0Md"]'
    # _xpath_enlace = './/a[@jsname="UWckNb"]/@href'
    # _xpath_fecha = './/span[@class="LEwnzc Sqrs4e"]/span'
    # _xpath_fuente = './/span[@class="VuuXrf"]'
    
    # _xpath_base = '//div[contains(@class, "N54PNb")]'
    # _xpath_titulo = './/span[@jscontroller="msmzHf"]//h3'
    # _xpath_resumen = './/div[contains(@class,"VwiC3b")]'
    # _xpath_enlace = './/span[@jscontroller="msmzHf"]/a/@href'
    
    # For Bing
    _xpath_empty = '//div[@class="no_results"]'
    _xpath_base = '//li[@class="b_algo"]'    
    _xpath_titulo = './/h2/a'
    _xpath_enlace = './/h2/a/@href'
    _xpath_fecha = './/p[contains(@class, "b_lineclamp")]'
    _xpath_fuente = './/div[@class="b_attribution"]/cite'
    
    # For Brave
    # _xpath_empty = '//span[contains(text(), "Not many great matches came back for your search:")]'
    # _xpath_base = '//div[contains(@class,"svelte-n9nog2")]'
    # _xpath_titulo = './/div[contains(@class, "title")]'
    # _xpath_enlace = './/a/@href'
    # _xpath_fecha = './/div[contains(@class, "snippet-content")]/div[contains(@class, "snippet-description")]'
    # _xpath_fuente = './/cite/span[contains(@class, "netloc")]'
    
    
    nombre_comercial_original = nombre_comercial
    razon_social_original = razon_social

    #logging.info(f'La razon social es: {razon_social}')
    #logging.info(f'Nombre comercial es: {nombre_comercial}')

    lista_criterios_busqueda = []
    if nombre_comercial:
        nombre_comercial_criterio = nombre_comercial.replace(' ', '+').replace("-", "") if nombre_comercial else ''
        lista_criterios_busqueda += [
            (keyword, f'{nombre_comercial_criterio}+"{keyword.replace(" ", "+")}"') for keyword in _keyword_list
        ]

    lista_criterios_busqueda = list(set(lista_criterios_busqueda))
    resultados = []
    try:
        limit = asyncio.Semaphore(2)
        logging.info(f'Starting <Convoca> search process for entity "{nombre_comercial}"')
        resultados = await google_search_async(lista_criterios_busqueda, xpath=_xpath_base, sess=sess, dict_tracker=dict_tracker, limit=limit)
        entityResponse = {
            "entityIdNumber": entityIdNumber,
            "name": razon_social_original,
            "commercialName": nombre_comercial_original,
            "requestStatus": 200,
            "results": [],
            'createdOn': datetime.now(timezone.utc).isoformat(),
            'updatedOn': datetime.now(timezone.utc).isoformat()
        }

        entityResponse['results'] = formatting_results(resultados, _xpath_empty, _xpath_base, _xpath_titulo, _xpath_enlace, _xpath_fecha, _xpath_fuente, nombre_comercial_original)
        logging.info(f'Requests status counter in <Convoca> from <{nombre_comercial_original}>: {dict_tracker}')
        logging.info(f'Successfully finishing <Convoca> search process for entity <{nombre_comercial_original}>')
        # json_data = json.dumps(data_total)
        return entityResponse
    
    except Exception as e:
        logging.info(f"Requests status counter in <Convoca> from <{nombre_comercial_original}>: ", "{'500': 7}")
        logging.error(f"An error has ocurred in <Convoca>: {e}")
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