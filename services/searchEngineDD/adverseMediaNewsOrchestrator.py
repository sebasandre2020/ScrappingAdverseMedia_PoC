# -*- coding: utf-8 -*-

# Importaciones necesarias para el funcionamiento del script.
import asyncio  # Para ejecutar código asíncrono (operaciones de red concurrentes).
import logging  # Para registrar información, advertencias y errores.
from async_ip_rotator import IpRotator, ClientSession  # Librería para rotar direcciones IP usando AWS API Gateway, útil para evitar bloqueos por rate-limiting.
from collections.abc import Coroutine  # Para anotaciones de tipo (type hints) de corutinas.
from types import FunctionType as function  # Para anotaciones de tipo de funciones.
from collections import defaultdict  # Un tipo de diccionario que crea un item por defecto si una clave no existe.
import os
import requests

class AdverseMediaNewsOrchestrator:
    '''
    Esta clase orquesta la búsqueda de noticias adversas sobre entidades en múltiples sitios web de forma asíncrona.
    Utiliza un rotador de IP para realizar las peticiones HTTP y evitar bloqueos.

    Consideraciones:
    - Los métodos de búsqueda asíncronos que se añadan deben tener la misma firma (cantidad y tipo de parámetros).
    - Se asume que se realiza una petición HTTP por cada entidad a buscar.
    '''

    def __init__(self):
        """
        Constructor de la clase. Inicializa la lista que almacenará los métodos de búsqueda.
        """
        # Esta lista contendrá tuplas, cada una con una instancia del rotador de IP,
        # la función de búsqueda específica para un sitio, y el nombre de ese sitio.
        self.search_methods: list[tuple[IpRotator, function, str]] = []

    def add_search_method(self, site_target: str, async_search_method: function, site_name: str) -> bool:
        """
        Registra un nuevo sitio web y su método de búsqueda en el orquestador.

        Args:
            site_target (str): La URL base del sitio web al que se harán las peticiones.
            async_search_method (function): La función asíncrona que sabe cómo buscar en ese sitio.
            site_name (str): Un nombre descriptivo para el sitio web (usado en logs).

        Returns:
            bool: True si el método se añadió correctamente, False si hubo un error.
        """
        try:
            # Crea una instancia de IpRotator. Este objeto se encargará de crear y gestionar
            # los endpoints en AWS API Gateway para enmascarar nuestra IP.
            aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

            _gateway_instance = IpRotator(
                target=site_target,
                aws_key_id=aws_access_key_id,
                aws_key_secret=aws_secret_access_key,
                regions=['us-east-1', 'us-east-2'],
            )
            # Añade la tupla (instancia_rotador, funcion_busqueda, nombre_sitio) a la lista.
            self.search_methods.append((_gateway_instance, async_search_method, site_name))
        except Exception as e:
            # Si algo falla al crear la instancia de IpRotator (ej. credenciales inválidas),
            # se registra el error.
            logging.info(f"Requests status counter in <{site_name}> from <All Entities>: {{'500': 7}}")
            logging.error(f"An error has ocurred in <{site_name}>: {e}")
            return False
        return True

    def __gather_results(self, total_results: list) -> list:
        """
        Método privado para consolidar los resultados de búsqueda de todas las fuentes.
        Agrupa los resultados por entidad.

        Args:
            total_results (list): Una lista plana con todos los resultados de todas las búsquedas.

        Returns:
            list: Una lista de resultados consolidados, donde cada elemento corresponde a una única entidad.
        """
        # Se usa un defaultdict para agrupar los resultados. Si una entidad no existe en el
        # diccionario, se crea automáticamente una entrada con la estructura definida.
        resultados_combinados = defaultdict(lambda: {
            'results': [],
            'createdOn': None,
            'updatedOn': None
        })

        # Itera sobre cada resultado individual obtenido.
        for item in total_results:
            # Crea una clave única para cada entidad para poder agruparlas.
            key = (item['entityIdNumber'], item['name'], item['commercialName'])
            # Agrega los hallazgos (noticias) a la lista de resultados de la entidad.
            resultados_combinados[key]['results'].extend(item['results'])
            
            # Actualiza la fecha de creación para conservar la más antigua de todas las fuentes.
            if resultados_combinados[key]['createdOn'] is None or item['createdOn'] < resultados_combinados[key]['createdOn']:
                resultados_combinados[key]['createdOn'] = item['createdOn']
            # Actualiza la fecha de actualización para conservar la más reciente.
            if resultados_combinados[key]['updatedOn'] is None or item['updatedOn'] > resultados_combinados[key]['updatedOn']:
                resultados_combinados[key]['updatedOn'] = item['updatedOn']

        # Convierte el diccionario de resultados agrupados de nuevo a una lista con el formato final.
        lista_fusionada = [
            {
                "entityIdNumber": key[0],
                "name": key[1],
                "commercialName": key[2],
                "requestStatus": total_results[0]['requestStatus'],  # Asume que el estado es el mismo para todos.
                "results": list(val['results']),  # Convierte a lista para asegurar el formato.
                "createdOn": val['createdOn'],
                "updatedOn": val['updatedOn']
            }
            for key, val in resultados_combinados.items()
        ]

        return lista_fusionada

    async def __search_process(self, rotator: IpRotator, async_search_method: function, entities: list,) -> list:
        """
        Método asíncrono privado que realiza el proceso de búsqueda para un único sitio web
        pero para una lista completa de entidades.

        Args:
            rotator (IpRotator): La instancia del rotador de IP para este sitio.
            async_search_method (function): La función de búsqueda específica para este sitio.
            entities (list): La lista de entidades a buscar.

        Returns:
            list: Una lista con los resultados de la búsqueda para este sitio.
        """
        try:
            # El contexto `async with rotator` inicia el proceso de creación de la API en AWS.
            async with rotator as api_gateway:
                # `ClientSession` es un objeto de aiohttp que usará la URL del `api_gateway`
                # para enrutar todas sus peticiones a través de la IP rotada.
                async with ClientSession(api_gateway) as sess:
                    # Crea una lista de "tareas" asíncronas. Cada tarea es una llamada a la
                    # función de búsqueda para una entidad específica.
                    tasks = [async_search_method(sess, entity[0], entity[1], entity[2]) for entity in entities]
                    # `asyncio.gather` ejecuta todas las tareas de la lista de forma concurrente.
                    # `return_exceptions=True` evita que el proceso se detenga si una de las tareas falla.
                    results_by_entity = await asyncio.gather(*tasks, return_exceptions=True)
                    return results_by_entity
        except Exception as e:
            logging.error(f"Unexpected error in <Adverse Media News>: {e}")
            return []
        # El bloque `finally` (comentado) sería el lugar ideal para limpiar los recursos,
        # como las APIs creadas en AWS, para no incurrir en costos innecesarios.
        # finally:
        #     if rotator:
        #         try:
        #             await rotator.clear_existing_apis()  # Limpia las IPs/APIs.
        #         except Exception as cleanup_error:
        #             logging.error(f"Error clearing IpRotator APIs: {cleanup_error}")

    async def execute_search_processes(self, entities: list) -> list:
        """
        El método principal para ejecutar todas las búsquedas configuradas de forma concurrente.

        Args:
            entities (list): La lista de entidades a buscar en todas las fuentes.

        Returns:
            list: La lista final de resultados, consolidados y formateados.
        """
        total_results = []  # Lista para acumular los resultados de todos los sitios.
        try:
            tasks = []
            site_names = []
            # Itera sobre cada método de búsqueda que fue registrado con `add_search_method`.
            for rotator, async_search_method, site_name in self.search_methods:
                # Para cada sitio, crea una tarea que consiste en buscar TODAS las entidades en ESE sitio.
                tasks.append(self.__search_process(rotator, async_search_method, entities))
                site_names.append(site_name)

            # Ejecuta concurrentemente las tareas de búsqueda para todos los sitios.
            # Cada elemento en `results` será la lista de resultados de un sitio completo.
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Procesa los resultados obtenidos de cada sitio.
            for site_name, result in zip(site_names, results):
                # Si el resultado es una excepción, significa que la búsqueda en ese sitio falló por completo.
                if isinstance(result, Exception):
                    logging.info(f"Requests status counter in <{site_name}> from <All Entities>: {{'500': 7}}")
                    logging.error(f"An error has ocurred in <{site_name}>: {result}")
                else:
                    # Si tuvo éxito, extiende la lista de resultados totales.
                    total_results.extend(result)
            
            # Una vez que se han recopilado todos los resultados de todos los sitios,
            # se llama al método para agruparlos y darles formato.
            total_results_formatted = self.__gather_results(total_results)
        except Exception as e:
            logging.error(f"Unexpected error in <Adverse Media News>: {e}")
            return []

        return total_results_formatted