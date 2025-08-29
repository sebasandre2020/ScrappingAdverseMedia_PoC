# -*- coding: utf-8 -*-

# --- Importaciones de librerías ---
import os
import requests
import random  # Para seleccionar elementos aleatorios (ej. User-Agents).
import requests # Cliente HTTP síncrono (aunque el código principal usa asíncrono).
import aiohttp  # Cliente HTTP asíncrono, fundamental para este script.
import asyncio  # Librería para ejecutar corutinas y gestionar bucles de eventos.
import logging  # Para registrar información y errores durante la ejecución.
import json     # Para trabajar con datos en formato JSON.
from async_ip_rotator import IpRotator, ClientSession  # Para la rotación de IPs a través de AWS API Gateway.
from bs4 import BeautifulSoup  # Para parsear HTML (importado pero no usado en el código activo).
from lxml import etree  # Una librería muy eficiente para parsear XML/HTML y usar XPath.
from time import perf_counter  # Para medir el tiempo de ejecución.
import re       # Para trabajar con expresiones regulares (ej. limpiar texto).
import ssl      # Para gestionar la configuración de certificados SSL.
from datetime import datetime, timezone  # Para manejar fechas y horas con zona horaria.
from services.scraperApiService import ScraperApiService  # Un servicio personalizado para hacer scraping.
from utils import request_status_counter  # Utilidad propia para contar estados de las peticiones.
from collections import defaultdict  # Diccionario que inicializa claves inexistentes con un valor por defecto.

# Importación de constantes desde un archivo de utilidades.
from utils.constants import ENDPOINT_URL, ENDPOINT_TIMEOUT, GOOGLE_SEARCH, KEYWORDS_LIST, MAX_SEARCH_RESULTS, MAX_RETRIES, NEWS_SEARCH, RETRY_DELAY, EXPONENTIAL_BACKOFF, REQUEST_SLEEP_TIME


# --- Configuración Inicial ---

# Configura el sistema de logging para mostrar mensajes de nivel INFO y superior.
logging.basicConfig(level=logging.INFO)
logging_done = False # Una bandera global para controlar cuándo se registran ciertos mensajes.

# ¡ADVERTENCIA! Se crea un contexto SSL que deshabilita la verificación de certificados.
# Esto puede ser útil para sitios con certificados auto-firmados, pero es inseguro
# para producción ya que expone a ataques "man-in-the-middle".
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


async def scraping_adverse_media_batch(entities_set: list[tuple[str, str, str]]):
    """
    Orquesta el proceso de scraping para un lote de entidades de forma concurrente.
    Configura el rotador de IPs y lanza una tarea de scraping para cada entidad.

    Args:
        entities_set (list[tuple[str, str, str]]): Una lista de tuplas, donde cada tupla
        contiene (nombre_comercial, razon_social, entityIdNumber) de una entidad.

    Returns:
        list[dict]: Una lista de diccionarios, donde cada diccionario es el resultado
                    completo del scraping para una entidad exitosa. En caso de un
                    error mayor, devuelve una lista vacía.
    """
    try:
        _site = 'https://www.bing.com'  # Sitio web objetivo para el rotador de IPs.

        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        _gateway_instance = IpRotator(
            target=_site,
            aws_key_id=aws_access_key_id,
            aws_key_secret=aws_secret_access_key,
            regions=['us-east-1', 'us-east-2'],
        )

        # Inicia el rotador de IP. Esto crea los recursos necesarios en AWS.
        async with _gateway_instance as ip_rotator:
            # Crea una lista de tareas asíncronas, una por cada entidad en el lote.
            tasks = [scraping_adverse_media(entity[0], entity[1], entity[2], ip_rotator) for entity in entities_set]
            # Ejecuta todas las tareas concurrentemente y espera sus resultados.
            # `return_exceptions=True` permite que el programa continúe aunque algunas tareas fallen.
            tasks_data = await asyncio.gather(*tasks, return_exceptions=True)

        results = []
        # Filtra los resultados para separar las tareas exitosas de las que lanzaron excepciones.
        for task in tasks_data:
            if isinstance(task, Exception):
                logging.error(f"Task resulted in an exception: {task}")
            else:
                results.append(task)

        return results

    except Exception as e:
        logging.info("Requests status counter in <Adverse Media> from <All Entities>: {'500': 7}")
        logging.error(f"An error has ocurred in Adverse Media Finding: {e}")
        return []


def extrae_informacion(objeto: etree._Element, xpath: str) -> list:
    """
    Función auxiliar para extraer información de un objeto lxml usando una consulta XPath.
    Está diseñada para ser segura y siempre devolver una lista.

    Args:
        objeto (etree._Element): El objeto lxml (HTML o XML) del cual extraer datos.
        xpath (str): La cadena de la consulta XPath.

    Returns:
        list: Una lista con los elementos encontrados. Devuelve una lista vacía si
              no se encuentra nada o si el objeto de entrada es None.
    """
    if objeto is None:
        return []
    xpath_objeto = objeto.xpath(xpath)
    # Asegura que el resultado siempre sea una lista, incluso si xpath devuelve un solo elemento.
    xpath_lista = xpath_objeto if isinstance(xpath_objeto, list) else [xpath_objeto]
    return xpath_lista


async def consulta_pagina_web(session: ClientSession, url: str, header: dict, keyword: str, dict_tracker, limit) -> tuple:
    """
    Realiza una única petición GET a una URL de forma asíncrona, con reintentos y
    manejo de errores.

    Args:
        session (ClientSession): La sesión de aiohttp que se usará para la petición.
        url (str): La URL a consultar.
        header (dict): Las cabeceras HTTP para la petición.
        keyword (str): La palabra clave de búsqueda asociada a esta URL.
        dict_tracker: Un objeto para contar los códigos de estado de las respuestas.
        limit: Un semáforo de asyncio para limitar la concurrencia.

    Returns:
        tuple: Una tupla con (contenido, url, keyword).
               - Si la petición es exitosa: (str_html, str_url, str_keyword)
               - Si falla: (dict_error, str_url, str_keyword)
    """
    max_retries = MAX_RETRIES
    retry_delay = RETRY_DELAY
    sleep_time = REQUEST_SLEEP_TIME

    for attempt in range(max_retries):
        try:
            # Espera a que el semáforo permita la ejecución para no sobrecargar el servidor.
            async with limit:
                # Realiza la petición GET.
                response = await session.get(url, headers=header, timeout=30, ssl=ssl_context)
                
                # Si la página no se encuentra, reintenta con un retardo exponencial.
                if response.status == 404:
                    logging.warning(f'For <Adverse Media>: {response.status}: {response.reason}')
                    await asyncio.sleep(retry_delay)
                    retry_delay *= EXPONENTIAL_BACKOFF # Aumenta el tiempo de espera para el siguiente reintento.
                    continue

                resp_text = await response.text()

                # Detección de CAPTCHA o bloqueo por parte del buscador.
                if any(substring in resp_text for substring in [
                    "Our systems have detected unusual traffic", "Please click", "trouble accessing Google Search"
                ]):
                    logging.warning(f'For <Adverse Media>: Google CAPTCHA detected for URL: {url}')
                    request_status_counter.agregar_o_actualizar(dict_tracker, "500")
                else:
                    request_status_counter.agregar_o_actualizar(dict_tracker, str(response.status))
                
                await asyncio.sleep(sleep_time) # Pequeña pausa para no ser demasiado agresivo.
                
                return resp_text, url, keyword

        except asyncio.TimeoutError as e:
            logging.error(f"For <Adverse Media> Timeout Error for URL {url}: {e}")
            return {'status': 'Error', 'description': str(e), 'url': url, 'content': None}, url, keyword
        except Exception as e:
            logging.error(f"For <Adverse Media> Error for URL {url}: {e}")
            return {'status': 'Error', 'description': str(e), 'url': url, 'content': None}, url, keyword
    
    # Si se superan todos los reintentos.
    logging.error(f"For <Adverse Media> Max retries reached for URL {url}.")
    request_status_counter.agregar_o_actualizar(dict_tracker, "404")
    return {'status': 'Error', 'description': 'Max retries reached', 'url': url, 'content': None}, url, keyword


def limpiar_titulo(titulo: str) -> str:
    """
    Limpia una cadena de título eliminando las URLs que pueda contener.

    Args:
        titulo (str): El título a limpiar.

    Returns:
        str: El título limpio sin URLs.
    """
    # Expresión regular para encontrar URLs.
    patron_enlace = r'https?://[^\s›]+'
    titulo_limpio = re.sub(patron_enlace, '', titulo)
    return titulo_limpio.strip()


async def google_search_async(lista_criterios_busqueda: list[tuple[str, str]], xpath: str, gateway: IpRotator, dict_tracker, limit) -> list:
    """
    Realiza búsquedas en Bing (a pesar del nombre) para una lista de criterios de
    búsqueda de forma asíncrona.

    Args:
        lista_criterios_busqueda (list[tuple[str, str]]): Lista de tuplas (keyword, criterio).
        xpath (str): XPath base para la extracción (no usado directamente aquí).
        gateway (IpRotator): La instancia del rotador de IP.
        dict_tracker: El contador de estados de petición.
        limit: El semáforo de concurrencia.

    Returns:
        list[tuple]: Una lista de tuplas, donde cada tupla es el resultado de
                     `consulta_pagina_web`. Formato:
                     `[(contenido_html_o_error, url, keyword), ...]`
    """
    # Lista de User-Agents para simular diferentes navegadores y reducir la probabilidad de bloqueo.
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0"
    ]

    _header = {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.bing.com/",
    }
    
    # Crea una sesión de cliente que usará el rotador de IP.
    async with ClientSession(gateway) as sess:
        # Prepara las tareas de búsqueda para cada criterio.
        tasks = [consulta_pagina_web(sess, f'https://www.bing.com/search?q={criterio_busqueda}&num={MAX_SEARCH_RESULTS}', _header, keyword, dict_tracker, limit) for keyword, criterio_busqueda in lista_criterios_busqueda]
        return await asyncio.gather(*tasks)


def formatting_results(results: list, _xpath_empty: str, _xpath_base: str, _xpath_titulo: str, _xpath_resumen: str, _xpath_enlace: str, nombre_comercial_original: str) -> list:
    """
    Procesa y formatea los resultados en HTML crudo obtenidos del scraping.

    Args:
        results (list): La lista de resultados crudos de `Google Search_async`.
        _xpath_... (str): Las cadenas XPath para extraer cada parte de la información.
        nombre_comercial_original (str): El nombre de la entidad buscada.

    Returns:
        list[dict]: Una lista de diccionarios, cada uno representando un resultado de
                    búsqueda formateado y limpio.
    """
    data_total = []
    resultados_por_keyword = dict()
    
    for resultado, url, keyword in results:
        # Si el resultado es un diccionario, fue un error, así que lo saltamos.
        if isinstance(resultado, dict):
            continue

        # Parsea el HTML crudo a un objeto lxml.
        content_tree = etree.HTML(resultado)

        # Si la página no tiene resultados, continúa con el siguiente.
        if content_tree.xpath(_xpath_empty):
            resultados_por_keyword[keyword] = []
            continue
        
        # Extrae los bloques principales de cada resultado de búsqueda.
        informacion = extrae_informacion(content_tree, _xpath_base)

        # Itera sobre cada bloque y extrae los detalles (título, resumen, enlace).
        Nueva_Data = [
            {
                'KeyWord': keyword,
                'Nombre o Razón Social': nombre_comercial_original,
                'Titulo': limpiar_titulo(item.xpath(_xpath_titulo)[0].xpath('string(.)')) if item.xpath(_xpath_titulo) else '',
                'Resumen': item.xpath(_xpath_resumen)[0].xpath('string(.)') if item.xpath(_xpath_resumen) else '',
                'URL': item.xpath(_xpath_enlace)[0] if item.xpath(_xpath_enlace) else '',
                'RequestStatus': 200,
                'DescriptionStatus': 'Ok'
            } for item in informacion
        ]
        resultados_por_keyword[keyword] = Nueva_Data
    
    # Filtra y limita los resultados.
    for keyword in resultados_por_keyword.keys():
        resultados_validos = resultados_por_keyword[keyword]
        if not resultados_validos:
            continue
        
        # Filtra resultados que no tengan título o tengan un resumen genérico.
        resultados_validos = [r for r in resultados_validos if r['Titulo'] and "cannot provide a description" not in r['Resumen']]
        
        # Limita a los primeros 10 resultados válidos por palabra clave.
        data_total.extend(resultados_validos[:10])
    
    logging.info(f'Total amount of results in <Adverse Media>: {len(data_total)}')
    return data_total


async def scraping_adverse_media(nombre_comercial: str, razon_social: str, entityIdNumber: str, gateway_instance: IpRotator):
    """
    Función principal para el scraping de una única entidad. Construye las consultas,
    ejecuta el scraping y empaqueta la respuesta final.

    Args:
        nombre_comercial (str): Nombre comercial de la entidad.
        razon_social (str): Razón social de la entidad.
        entityIdNumber (str): ID de la entidad.
        gateway_instance (IpRotator): La instancia del rotador de IP ya inicializada.

    Returns:
        dict: Un diccionario con toda la información de la entidad y los resultados
              de la búsqueda. Formato:
              {
                  "entityIdNumber": str, "name": str, "commercialName": str,
                  "requestStatus": int, "results": list[dict],
                  "createdOn": str_iso_date, "updatedOn": str_iso_date
              }
    """
    dict_tracker = defaultdict(request_status_counter.default_value)
    
    # Definiciones de XPath específicas para el motor de búsqueda (Bing está activo).
    # Las de Google y Brave están comentadas como referencia.
    _xpath_empty = '//div[@class="no_results"]'
    _xpath_base = '//li[@class="b_algo"]'
    _xpath_titulo = './/h2/a'
    _xpath_resumen = './/p[contains(@class,"b_lineclamp")]'
    _xpath_enlace = './/h2/a/@href'
    
    # Construye la lista de criterios de búsqueda combinando los nombres con las palabras clave.
    lista_criterios_busqueda = []
    if nombre_comercial:
        nombre_comercial_criterio = nombre_comercial.replace("-", "")
        lista_criterios_busqueda += [(keyword, nombre_comercial_criterio) for keyword in KEYWORDS_LIST]

    if razon_social:
        razon_social_criterio = razon_social.replace("-", "")
        lista_criterios_busqueda += [(keyword, razon_social_criterio) for keyword in KEYWORDS_LIST]

    # Elimina duplicados.
    lista_criterios_busqueda = list(set(lista_criterios_busqueda))
    
    resultados = []
    try:
        # Utiliza un servicio externo para ejecutar las peticiones.
        _scraperService = ScraperApiService()
        resultados = await _scraperService.execute_requests(lista_criterios_busqueda)
        # Nota: El código original tenía una llamada a `Google Search_async` que ahora
        # está comentada y reemplazada por el ScraperApiService.
    except Exception as e:
        logging.error(f"An error has ocurred in <Adverse Media>: {e}")
    
    # Construye el objeto de respuesta final para esta entidad.
    entityResponse = {
        "entityIdNumber": entityIdNumber,
        "name": razon_social,
        "commercialName": nombre_comercial,
        "requestStatus": 200,
        "results": [],
        'createdOn': datetime.now(timezone.utc).isoformat(),
        'updatedOn': datetime.now(timezone.utc).isoformat()
    }

    # Asigna los resultados obtenidos.
    # Nota: La llamada a `formatting_results` está comentada, por lo que los resultados
    # se guardan en crudo tal como los devuelve el `ScraperApiService`.
    entityResponse['results'] = resultados
    
    logging.info(f'Requests status counter in <Adverse Media> from <{nombre_comercial}>: {dict_tracker}')
    logging.info(f'Successfully finishing <Adverse Media> search process for entity <{nombre_comercial}>')

    return entityResponse