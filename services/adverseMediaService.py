# -*- coding: utf-8 -*-

# --- Importaciones de librerías y módulos ---
import logging
import asyncio
from datetime import datetime, timezone

# --- Modelos de Base de Datos ---
# Se utilizan para estructurar los datos que vienen o van a la base de datos.
from db_models.entityModel import Entity
from db_models.sourceModel import Source

# --- Servicios de Scraping ---
# Módulos específicos para hacer scraping en diferentes sitios de noticias.
from services.searchEngineDD.service import peru21_scrapper_v2, elcomercio_scrapper_v2, idl_reporteros_scrapper_v2, convoca_scrapper_v2, larepublica_scrapper_v2, gestion_scrapper_v2
# Orquestador para manejar múltiples procesos de scraping de forma organizada.
from services.searchEngineDD.adverseMediaNewsOrchestrator import AdverseMediaNewsOrchestrator
# Funciones principales que ejecutan el scraping de medios adversos.
from services.searchEngineDD.adverseMedia import scraping_adverse_media_batch, scraping_adverse_media

# --- Utilidades y Constantes ---
from utils.constants import KEYWORDS_LIST, NEWS_WEBSITES, SOURCECODE_ADVERSE_MEDIA
from utils.decorators import log_execution_time
from utils.normalize_string import normalize_string_special_chars

# --- Repositorios (Capa de Acceso a Datos) ---
# Clases que manejan la lógica para comunicarse con la base de datos.
from repositories.entityRepository import EntityRepository
from repositories.resultRepository import ResultRepository
from repositories.sourceRepository import SourceRepository

# --- Servicios Adicionales ---
# Servicio centralizado para realizar peticiones de scraping.
from services.scraperApiService import ScraperApiService


async def countdown_timer(timeout):
    """
    Función auxiliar asíncrona que simplemente espera un número determinado de segundos.
    Es un "sleep" no bloqueante.

    Args:
        timeout (int | float): El número de segundos a esperar.
    
    Returns:
        None
    """
    await asyncio.sleep(timeout)


async def process_adverse_media(sourceApiService, entity, entityDocs):
    """
    Procesa una única entidad para buscar "medios adversos" asociados a ella.
    Busca la entidad, ejecuta el scraping y formatea el resultado.

    Args:
        sourceApiService: Una instancia del servicio para realizar operaciones.
        entity (dict): Un diccionario con la información de la entidad a procesar.
        entityDocs: Una colección de documentos donde buscar la entidad localmente.

    Returns:
        dict: Un diccionario con los resultados formateados para la entidad.
        None: Si la entidad no es válida (ej. no tiene nombre).
        list: Una lista vacía `[]` si ocurre un `ValueError` al buscar la entidad.
              Formato de retorno exitoso:
              {
                  "entityIdNumber": str, "name": str, "commercialName": str,
                  'requestStatus': int, 'results': list,
                  'createdOn': str_iso_date, 'updatedOn': str_iso_date
              }
    """
    try:
        # Busca la entidad en una colección de documentos locales para obtener datos completos.
        item = sourceApiService.findEntityLocally(entityDocs, entity)
    except ValueError as e:
        logging.error(f"Error during adverse media search process: {e}")
        return []
    
    if item:
        # Convierte el diccionario encontrado en un objeto `Entity` para un manejo más fácil.
        found_entity = Entity.from_dict(item)

        # Validación: si la entidad no tiene nombre ni nombre comercial, no se puede buscar.
        if (found_entity.commercialName is None and found_entity.name is None):
            return None
        
        logging.info(f"Initialized process for Entity with keywords: {found_entity.name} {found_entity.commercialName}")
        # Define las palabras clave principal y secundaria para la búsqueda.
        keyword = entity.get('name', '')
        secondary_keyword = entity.get('commercialName', '') or entity.get('name', '')
        
        try:
            # Llama a la función de scraping principal.
            result_media = await scraping_adverse_media({
                "site": SOURCECODE_ADVERSE_MEDIA,
                "keyword": keyword,
                "secondary_keyword": secondary_keyword
            })
            
            # Estructura el resultado para ser insertado o devuelto.
            result_to_insert = {
                "entityIdNumber": found_entity.entityIdNumber,
                "name": entity.get('name', ''),
                "commercialName": entity.get('commercialName', ''),
                'requestStatus': result_media[0], # El primer elemento es el código de estado.
                'results': result_media[1],       # El segundo elemento es la lista de resultados.
                'createdOn': datetime.now(timezone.utc).isoformat(),
                'updatedOn': datetime.now(timezone.utc).isoformat(),
            }
            return result_to_insert
        # Manejo de errores específicos para timeout y excepciones generales.
        except asyncio.TimeoutError:
            return {
                "entityIdNumber": found_entity.entityIdNumber, "name": entity.get('name', ''),
                "commercialName": entity.get('commercialName', ''), 'requestStatus': 504,
                'results': [], 'createdOn': datetime.now(timezone.utc).isoformat(),
                'updatedOn': datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            logging.error(f"An error occurred during the media processing: {str(e)}")
            return {
                "entityIdNumber": found_entity.entityIdNumber, "name": found_entity.name,
                "commercialName": found_entity.commercialName, 'requestStatus': 500,
                'results': [], 'createdOn': datetime.now(timezone.utc).isoformat(),
                'updatedOn': datetime.now(timezone.utc).isoformat(),
            }
    else:
        return None


async def fill_adverse_media_http(sourceApiService, entity_list, entityDocs, hasAdverseMedia):
    """
    Orquesta la búsqueda de medios adversos para una lista de entidades, creando
    una tarea asíncrona para cada una.

    Args:
        sourceApiService: Instancia del servicio.
        entity_list (list[dict]): Lista de entidades a procesar.
        entityDocs: Colección de documentos para búsqueda local.
        hasAdverseMedia (bool): Una bandera para activar o desactivar esta función.

    Returns:
        list[dict]: Una lista con los resultados de todas las entidades procesadas exitosamente.
    """
    # Si la bandera está desactivada, retorna una lista vacía inmediatamente.
    if not hasAdverseMedia:
        return []
    logging.info("Started Adverse Media Finding")
    
    # Crea una lista de tareas, una por cada entidad.
    tasks = [
        asyncio.create_task(process_adverse_media(sourceApiService, entity, entityDocs))
        for entity in entity_list
    ]
    
    adverse_result = []
    try:
        # `asyncio.as_completed` procesa las tareas a medida que se completan,
        # lo cual es eficiente en memoria para grandes volúmenes de tareas.
        for task in asyncio.as_completed(tasks):
            result_media = await task
            # Solo agrega el resultado si no es None.
            if result_media is not None:
                adverse_result.append(result_media)
    except asyncio.CancelledError:
        logging.warning("Adverse media processing was cancelled")

    return adverse_result


async def fill_adverse_media_news(sourceApiService, entity_list, entityDocs, hasAdMedia):
    """
    Prepara y ejecuta una búsqueda de noticias adversas en sitios web específicos para
    una lista de entidades.

    Args:
        sourceApiService: Instancia del servicio.
        entity_list (list[dict]): Lista de entidades a procesar.
        entityDocs: Colección de documentos para búsqueda local.
        hasAdMedia (bool): Bandera para activar o desactivar la función.

    Returns:
        list: Una lista con los resultados crudos devueltos por el `ScraperApiService`.
              El formato exacto de cada elemento depende de la implementación del servicio.
    """
    if not hasAdMedia:
        return []
    logging.info("Started Adverse Media News Finding")

    entities_to_search = []
    for complete_entity in entity_list:
        # Solución temporal hardcodeada para ignorar un tipo de entidad.
        if complete_entity["relatedEntityType"] == "notario":
            continue
        
        try:
            item = sourceApiService.findEntityLocally(entityDocs, complete_entity)
        except ValueError as e:
            logging.error(f"Error during adverse media news search process: {e}")
            return []
        
        if item:
            found_entity = Entity.from_dict(item)
            if (found_entity.commercialName is None and found_entity.name is None):
                continue
            
            # Normaliza los nombres para crear consultas de búsqueda limpias.
            keyword = normalize_string_special_chars(complete_entity.get('name')) or found_entity.name
            secondary_keyword = normalize_string_special_chars(complete_entity.get('commercialName')) or keyword

            # Construye una lista exhaustiva de consultas combinando nombres, palabras clave
            # y sitios web de noticias.
            if secondary_keyword:
                entities_to_search.extend([(key, secondary_keyword, website) for key in KEYWORDS_LIST for website in NEWS_WEBSITES])
            if keyword and keyword.lower() != secondary_keyword.lower():
                entities_to_search.extend([(key, keyword, website) for key in KEYWORDS_LIST for website in NEWS_WEBSITES])
    
    try:
        _scraperService = ScraperApiService()
        # Ejecuta todas las solicitudes de noticias a través del servicio.
        tasks_data = await _scraperService.news_execute_requests(entities_to_search)
    except Exception as e:
        logging.error(f"Error during adverse media news search process: {e}")
        tasks_data = []
            
    return tasks_data


async def fill_adverse_media(sourceApiService, entity_list, entityDocs, hasAdverseMedia):
    """
    Prepara y ejecuta una búsqueda de medios adversos en un motor de búsqueda general
    (como Bing) para una lista de entidades.

    Args:
        sourceApiService: Instancia del servicio.
        entity_list (list[dict]): Lista de entidades a procesar.
        entityDocs: Colección de documentos para búsqueda local.
        hasAdverseMedia (bool): Bandera para activar o desactivar la función.

    Returns:
        list: Una lista de resultados devuelta por `scraping_adverse_media_batch`.
              Cada elemento es un diccionario con los hallazgos para una entidad.
    """
    if not hasAdverseMedia:
        return []
    logging.info("Started Adverse Media Finding")

    entities_to_search = []
    for complete_entity in entity_list:
        if complete_entity["relatedEntityType"] == "notario":
            continue
        
        try:
            item = sourceApiService.findEntityLocally(entityDocs, complete_entity)
        except ValueError as e:
            logging.error(f"Error during adverse media search process: {e}")
            return []
        
        # Genera las palabras clave para la búsqueda.
        keywords = generate_entity_keywords(complete_entity, Entity.from_dict(item) if item else None)
        
        entity_id = item.get('entityIdNumber') if item else complete_entity.get('entityIdNumber', '')
        # Agrega una tupla por cada palabra clave a la lista de búsqueda.
        for kw in keywords:
            entities_to_search.append((kw, '', entity_id))
            
    try:
        # Llama a la función que procesa el lote de entidades en un motor de búsqueda.
        tasks_data = await scraping_adverse_media_batch(entities_to_search)
    except Exception as e:
        logging.error(f"Error during <Adverse Media> for <All Entities> search process: {e}")
        tasks_data = []
            
    return tasks_data


def generate_entity_keywords(entity_dict, found_entity=None):
    """
    Función auxiliar para generar una lista de palabras clave únicas para una entidad,
    normalizando su nombre y nombre comercial.

    Args:
        entity_dict (dict): El diccionario de la entidad con 'name' y 'commercialName'.
        found_entity (Entity, optional): Un objeto Entity para usar como respaldo si
                                          los datos del diccionario no están completos.

    Returns:
        list[str]: Una lista de palabras clave únicas y normalizadas.
    """
    # Normaliza los nombres del diccionario de entrada.
    name_norm = normalize_string_special_chars(entity_dict.get('name'))
    commercialName_norm = normalize_string_special_chars(entity_dict.get('commercialName'))

    # Define valores de respaldo por si la entidad fue encontrada localmente.
    fallback_name = found_entity.name if found_entity else ''
    fallback_commercial = found_entity.commercialName if found_entity else ''

    # Determina la palabra clave principal y secundaria usando los valores normalizados
    # y los de respaldo.
    keyword = name_norm or fallback_name
    secondary_keyword = commercialName_norm or name_norm or fallback_commercial or fallback_name

    # Se usa un conjunto (`set`) para almacenar las palabras clave y así eliminar
    # duplicados automáticamente.
    keywords = set()
    if keyword:
        keywords.add(keyword)
    # Solo se añade la secundaria si es diferente de la principal.
    if secondary_keyword and secondary_keyword.lower() != keyword.lower():
        keywords.add(secondary_keyword)

    return list(keywords)