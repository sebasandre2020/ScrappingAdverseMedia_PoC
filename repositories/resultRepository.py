import os
import logging
from datetime import datetime, timezone
from typing import List
from azure.cosmos import exceptions
from db_models.resultModel import Result
from db_utils.db_connection import get_cosmos_client, cosmos_container_connection
from utils.constants import SOURCECODE_ADVERSE_MEDIA,SOURCECODE_AD_MEDIA,SOURCE_CODE_GAFI

class ResultRepository:

    __DATABASE_NAME = os.environ.get('DB_DATABASE')
    if os.environ.get('DB_DATABASE_MASTER'):
        __DATABASE_NAME = f"{os.environ.get('DB_DATABASE')}-{os.environ.get('DB_DATABASE_MASTER')}"
 
    __CONTAINER_NAME = os.environ.get("DB_CONTAINER_RESULTS")

    def __init__(self):
        self.client =  get_cosmos_client()
        self.container = cosmos_container_connection(self.client, ResultRepository.__DATABASE_NAME, ResultRepository.__CONTAINER_NAME)
    
    def get_entity_results_by_source_codes_array(self, source_codes: List[str]):
        query_conditions = ', '.join([f"@sourceCode{index}" for index, _ in enumerate(source_codes)])
        query = f"SELECT c.sourceCode, r.resultId FROM c JOIN r IN c.entityResults WHERE c.sourceCode IN ({query_conditions})"
        parameters = [{f"name": f"@sourceCode{index}", "value": source_code} for index, source_code in enumerate(source_codes)]
        try:
            results = self.container.query_items(query=query, parameters=parameters, enable_cross_partition_query=True)
            return results
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"An error occurred during Data Retrieval: {e}")
            return None
    
    def get_results_by_entity_list(self, source_codes_to_query: list, entity_list: list):
        entity_ids = [entity['id'] for entity in entity_list if entity['id']]

        if len(source_codes_to_query) <= 0:
            return []
        
        source_codes_placeholders = ', '.join([f"@sourceCode{index}" for index, _ in enumerate(source_codes_to_query)])
        parameters = [{f"name": f"@sourceCode{index}", "value": source_code} for index, source_code in enumerate(source_codes_to_query)]

        # Construct query conditions and parameters for entity IDs
        if entity_ids:
            entity_id_params = [f"@entityId{index}" for index, _ in enumerate(entity_ids)]
            entity_id_conditions = f"c.entityId IN ({', '.join(entity_id_params)})"
            parameters.extend([{"name": param, "value": entity_id} for param, entity_id in zip(entity_id_params, entity_ids)])
        else:
            return None

        # Combine conditions for source codes and entity IDs
        combined_conditions = " AND ".join(filter(None, [f"c.sourceCode IN ({source_codes_placeholders})", entity_id_conditions]))
        
        query = f"SELECT * FROM c WHERE {combined_conditions}"
        try:
            list_results = self.container.query_items(query=query, parameters=parameters, enable_cross_partition_query=True)    
            resultDocs = list(list_results)
            if not resultDocs:
                logging.info(f"No documents found for the provided sources codes. {resultDocs}")
            
            return resultDocs
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"An error occurred during Data Retrieval: {e}")
            return None
    
    def get_results_by_entity_list_by_request (self, source_codes, entity_list):
        hasAdverseMedia = False
        hasAdMedia = False
        hasGAFI = False
        source_codes_to_query = [item['sourceCode'] for item in source_codes]
        logging.info(f"source_codes_to_query: {source_codes_to_query}")
        if SOURCECODE_ADVERSE_MEDIA in source_codes_to_query:
            hasAdverseMedia = True
            source_codes_to_query.remove(SOURCECODE_ADVERSE_MEDIA)

        if SOURCECODE_AD_MEDIA in source_codes_to_query:
            hasAdMedia = True
            source_codes_to_query.remove(SOURCECODE_AD_MEDIA)

        if SOURCE_CODE_GAFI in source_codes_to_query:
            hasGAFI = True
            source_codes_to_query.remove(SOURCE_CODE_GAFI)
        logging.info(f"HASADMEDIA VALUE: {hasAdMedia}")
        if not source_codes_to_query:
            return [], hasAdverseMedia, hasGAFI, hasAdMedia
        
        entity_ids = [entity['id'] for entity in entity_list if entity['id']]

        source_codes_placeholders = ', '.join([f"@sourceCode{index}" for index, _ in enumerate(source_codes_to_query)])
        parameters = [{f"name": f"@sourceCode{index}", "value": source_code} for index, source_code in enumerate(source_codes_to_query)]

        # Construct query conditions and parameters for entity IDs
        if entity_ids:
            entity_id_params = [f"@entityId{index}" for index, _ in enumerate(entity_ids)]
            entity_id_conditions = f"c.entityId IN ({', '.join(entity_id_params)})"
            parameters.extend([{"name": param, "value": entity_id} for param, entity_id in zip(entity_id_params, entity_ids)])
        else:
            return None, hasAdverseMedia, hasGAFI, hasAdMedia

        # Combine conditions for source codes and entity IDs
        combined_conditions = " AND ".join(filter(None, [f"c.sourceCode IN ({source_codes_placeholders})", entity_id_conditions]))
        
        query = f"SELECT * FROM c WHERE {combined_conditions}"

        list_results = self.container.query_items(query=query, parameters=parameters, enable_cross_partition_query=True)    
        resultDocs = list(list_results)
        if not resultDocs:
            logging.info(f"No documents found for the provided sources codes. {resultDocs}")
        
        return resultDocs, hasAdverseMedia, hasGAFI, hasAdMedia
    
    def generate_new_result (self, new_result, id_item):
        new_result = Result(id = new_result.id,
                            sourceCode = new_result.sourceCode,
                            resultId = new_result.resultId,
                            entityId = id_item,
                            requestResponse = new_result.requestResponse or "",
                            requestStatus = new_result.requestStatus or "200",
                            results = new_result.results or [])
        id_result = new_result.id
        new_res_dict = new_result.to_dict()
        
        response = self.insert_result(new_res_dict)
        
        if response is None:
            logging.error("New Result was not inserted in the Database.")
        return response
    
    def update_result(self, body: dict):
        try:
            response = self.container.upsert_item(body)
            return response
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"An error occurred during Data Update: {e}")
            return None
    
    def insert_result(self, body: dict):
        try:
            response = self.container.create_item(body)
            return response
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"An error occurred during Data Insertion: {e}")
            return None
