import os
import logging
from datetime import datetime, timezone
from azure.cosmos import exceptions
from db_utils.db_connection import get_cosmos_client, cosmos_container_connection
from datetime import datetime, timedelta, timezone
from db_models.entityModel import Entity
from utils.match_closest_string import match_closest_string
from utils.normalize_string import normalize_string
from utils.constants import JURIDICAL_PERSON, NATURAL_PERSON, ENTITY_TYPE_ORG
class EntityRepository:

    __DATABASE_NAME = os.environ.get('DB_DATABASE')
    if os.environ.get('DB_DATABASE_MASTER'):
        __DATABASE_NAME = f"{os.environ.get('DB_DATABASE')}-{os.environ.get('DB_DATABASE_MASTER')}"
 
    __CONTAINER_NAME = os.environ.get("DB_CONTAINER_ENTITIES")

    def __init__(self):
        self.client =  get_cosmos_client()
        self.container = cosmos_container_connection(self.client, EntityRepository.__DATABASE_NAME, EntityRepository.__CONTAINER_NAME)
    
    def get_latest_results_from_sources_to_update(sources_to_update):
        pass
    
    def get_entity_by_id(self, entityId):
        if not entityId:
            return None
        query = "SELECT * FROM c WHERE c.id = @entityId"
        parameters = [{"name": "@entityId", "value": entityId}]

        try:
            items_iterable = self.container.query_items(query=query, parameters=parameters, enable_cross_partition_query=True)
            
            items_list = list(items_iterable)
            if items_list:
                entity = Entity.from_dict(items_list[0])
                return entity
            else:
                return None
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"An error occurred during Data Retrieval: {e}")
            return None
     
    def get_entities_by_request (self, entities):
        results = []
        query_conditions = []
        parameters = []
        
        entity_id_numbers = [
            entity['entityIdNumber'] for entity in entities
            if entity.get('entityIdNumber') not in (None, '')
        ]
        entity_names = [
            normalize_string(entity['name']).lower() for entity in entities
            if entity.get('name') not in (None, '')
        ]
        entity_commercial_names = [
            normalize_string(entity['commercialName']).lower() for entity in entities
            if entity.get('commercialName') not in (None, '')
        ]

        if entity_id_numbers:
            entity_id_number_params = [f"@entityIdNumber{i}" for i in range(len(entity_id_numbers))]
            query_conditions.append(f"c.entityIdNumber IN ({', '.join(entity_id_number_params)})")
            for i, id_number in enumerate(entity_id_numbers):
                parameters.append({"name": entity_id_number_params[i], "value": id_number})

        if entity_names:
            entity_name_params = [f"@entityName{i}" for i in range(len(entity_names))]
            query_conditions.append(f"LOWER(c.name) IN ({', '.join(entity_name_params)})")
            query_conditions.extend([f"ARRAY_CONTAINS(c.knownNames, {param})" for param in entity_name_params])
            for i, name in enumerate(entity_names):
                parameters.append({"name": entity_name_params[i], "value": name})

        if entity_commercial_names:
            commercial_name_params = [f"@commercialName{i}" for i in range(len(entity_commercial_names))]
            query_conditions.append(f"LOWER(c.commercialName) IN ({', '.join(commercial_name_params)})")
            query_conditions.extend([f"ARRAY_CONTAINS(c.knownNames, {param})" for param in commercial_name_params])
            for i, commercial_name in enumerate(entity_commercial_names):
                parameters.append({"name": commercial_name_params[i], "value": commercial_name})

        if not query_conditions:
            return []

        query = f"SELECT * FROM c WHERE {' OR '.join(query_conditions)}"
        
        items_iterable = self.container.query_items(query=query,parameters=parameters,enable_cross_partition_query=True)
        results.extend(list(items_iterable))
        
        return results
    
    def generate_new_entity(self, entity: dict):
        most_similar_type = ""
        if(entity.get('entityType')):
            most_similar_type = match_closest_string(entity['entityType'], JURIDICAL_PERSON, NATURAL_PERSON)
        
        try:
            entity_name = normalize_string(entity.get('name', ""))
            entity_commercial_name = normalize_string(entity.get('commercialName', ""))
            
            if entity['relatedEntityType'] == ENTITY_TYPE_ORG:
                new_entity = Entity(entityType=most_similar_type if most_similar_type != "" else JURIDICAL_PERSON,
                                    relatedEntityType= entity['relatedEntityType'],
                                    entityIdNumber=entity['entityIdNumber'] or "",
                                    name=entity_name or "",
                                    commercialName=entity_commercial_name or "",
                                    latestResults=[])
            else:
                new_entity = Entity(entityType=most_similar_type if most_similar_type != "" else NATURAL_PERSON,
                                    relatedEntityType= entity['relatedEntityType'],
                                    entityIdNumber=entity['entityIdNumber'] or "",
                                    name=entity_name or "",
                                    commercialName=entity_commercial_name if most_similar_type == JURIDICAL_PERSON else None,
                                    knownNames=[],
                                    latestResults=[])
            response = self.container.upsert_item(new_entity.to_dict())
            return response
        
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"An error occurred during Data Insertion: {e}")
            return None
    
    def update_entity(self, body: dict):
        try:
            response = self.container.upsert_item(body)
            return response
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"An error occurred during Data Update: {e}")
            return None
    
    def insert_entity(self, body: dict):
        try:
            response = self.container.create_item(body)
            return response
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"An error occurred during Data Insertion: {e}")
            return None
