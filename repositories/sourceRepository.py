import os
import logging
from db_utils.db_connection import get_cosmos_client, cosmos_container_connection
from datetime import datetime, timedelta, timezone
from typing import List
from azure.cosmos import exceptions
from db_models.locals.sourceScheduleModel import SourceSchedule
from db_models.sourceModel import Source

class SourceRepository:

    __DATABASE_NAME = os.environ.get('DB_DATABASE')
    if os.environ.get('DB_DATABASE_MASTER'):
        __DATABASE_NAME = f"{os.environ.get('DB_DATABASE')}-{os.environ.get('DB_DATABASE_MASTER')}"

    __CONTAINER_NAME = os.environ.get("DB_CONTAINER_SOURCES")

    def __init__(self):
        self.client =  get_cosmos_client()
        self.container = cosmos_container_connection(self.client, SourceRepository.__DATABASE_NAME, SourceRepository.__CONTAINER_NAME)
    
    def validate_source_codes_by_request (self, entity_request):
        has_non_sources = False
        source_codes_to_query = [item.sourceCode for item in entity_request.consultSources]
            
        source_codes_placeholders = ', '.join([f"@sourceCode{index}" for index, _ in enumerate(source_codes_to_query)])
        query = f"SELECT * FROM c WHERE c.sourceCode IN ({source_codes_placeholders})"
        parameters = [{f"name": f"@sourceCode{index}", "value": source_code} for index, source_code in enumerate(source_codes_to_query)]
        
        list_sources = self.container.query_items(query=query, parameters=parameters, enable_cross_partition_query=True)    
        
        returnListSources = list(list_sources)
        if len(list(returnListSources)) < len(source_codes_to_query):
            has_non_sources = True
            logging.info(f"Some of the sources provided were not found in the database.")
            
        return returnListSources, has_non_sources

    def get_sources_to_update(self, source_schedules: List[SourceSchedule]):
        # Obtener la fecha y hora actual en UTC
        current_utc_datetime = datetime.now(timezone.utc)

        # Crear una lista de condiciones para la consulta
        conditions = []
        for source_schedule in source_schedules:
            # Calcular la fecha de corte para cada sourceCode
            cutoff_date = current_utc_datetime - timedelta(days=source_schedule.days)
            cutoff_date_str = cutoff_date.isoformat()
            conditions.append(f"(c.sourceCode = '{source_schedule.sourceCode}' AND c.lastScheduleRequest <= '{cutoff_date_str}')")

        # Unir las condiciones con OR
        query_conditions = ' OR '.join(conditions)
        # La consulta final
        query = f"SELECT * FROM c WHERE {query_conditions}"

        raw_sources_to_update = list(self.container.query_items(query=query, enable_cross_partition_query=True))

        source_model_to_update = [Source.from_dict(item) for item in raw_sources_to_update]

        return source_model_to_update

    def get_source_by_srcCode(self, srcCode: str):
        if not srcCode:
            return None
        query = "SELECT * FROM c WHERE c.sourceCode = @srcCode"
        parameters = [{"name": "@srcCode", "value": srcCode}]

        try:
            items_iterable = self.container.query_items(query=query, parameters=parameters, enable_cross_partition_query=True)
            
            items_list = list(items_iterable)
            if items_list:
                source = Source.from_dict(items_list[0])
                return source
            else:
                return None
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"An error occurred during Data Retrieval: {e}")
            return None
    
    def update_source(self, body: dict):
        try:
            response = self.container.upsert_item(body)
            return response
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"An error occurred during Data Update: {e}")
            return None