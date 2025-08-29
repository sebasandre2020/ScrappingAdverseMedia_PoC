import logging
import asyncio
from datetime import datetime, timezone
import os
from azure.cosmos import CosmosClient
import json
from db_models.entityModel import Entity
from db_models.sourceModel import Source
from db_models.locals.entityRequestModel import EntityRequest
from db_utils.db_connection import get_cosmos_client, cosmos_container_connection
from db_utils.bus_connection import get_service_bus_client, enqueue_service_bus_messages
from services.adverseMediaService import generate_entity_keywords
from utils.match_closest_string import match_closest_string
from utils.normalize_string import normalize_string, normalize_string_special_chars
from utils.constants import JURIDICAL_PERSON, NATURAL_PERSON, ENTITY_TYPE_ORG, SOURCE_CODE_GAFI, SOURCE_CODE_GAFI, GAFI_COUNTRIES, SOURCECODE_ADVERSE_MEDIA
from utils.decorators import log_execution_time
from repositories.entityRepository import EntityRepository
from repositories.resultRepository import ResultRepository
from repositories.sourceRepository import SourceRepository

class SourceApiService:
    __source_repository = SourceRepository()
    __entity_repository = EntityRepository()
    __result_repository = ResultRepository()

    @log_execution_time
    def retrieveBodyMessageReq (self, req) :
        logging.info(f"JSON Deserealization for HTTPRequest, DB Connection and SourceCodes retrieve process\n")

        headers = req.headers
        logging.info(f"Request Headers: {json.dumps(dict(headers))}")

        try:
            req_body = req.get_json()
            logging.info(f"Request Body {json.dumps(req_body)}")
            entity_request = EntityRequest.from_dict(req_body)
        except Exception as e:
            return "Invalid body provided.", headers, None
        
        if entity_request.empty_values():
            return "No entities or sources provided.", headers, None
            
        return "", headers, entity_request
    
    @log_execution_time
    def obtainCurrentResults (self, entity_req: EntityRequest) :
        try:
            sourceDocs, has_non_sources = SourceApiService.__source_repository.validate_source_codes_by_request(entity_req)
            entity_list = entity_req.aggregate_roles()
            entityDocs = SourceApiService.__entity_repository.get_entities_by_request(entity_list)
            resultDocs, hasAdverseMedia, hasGAFI, hasAdMedia = SourceApiService.__result_repository.get_results_by_entity_list_by_request(sourceDocs, entityDocs)
        
            return entity_list, sourceDocs, has_non_sources, entityDocs, resultDocs, hasAdverseMedia, hasGAFI, hasAdMedia
        
        except Exception as e:
            raise "Error during database consult."
        
    @log_execution_time
    def findCurrentEntity (self, entityDocs, entity):
        try:
            item = self.findEntityLocally(entityDocs, entity)
            return item
        except Exception as e:
            logging.error(f"Error during result finding: {e}")
            return None
    
    @log_execution_time
    def calculateScore (self, entity, search_entity):
        score = 0
        
        entity_id_number = entity.get('entityIdNumber', "")
        entity_name = normalize_string(entity.get('name', ""))
        entity_commercial_name = normalize_string(entity.get('commercialName', ""))
        entity_known_names = [normalize_string(name) for name in entity.get('knownNames', [])]

        search_id_number = search_entity.get('entityIdNumber', "")
        search_name = normalize_string(search_entity.get('name', ""))
        search_commercial_name = normalize_string(search_entity.get('commercialName', ""))

        if search_id_number and search_id_number == entity_id_number:
            score += 250

        if search_name and search_name == entity_name:
            score += 75
        if search_commercial_name and search_commercial_name == entity_commercial_name:
            score += 75

        if (search_name and search_name == entity_commercial_name
            or search_commercial_name and search_commercial_name == entity_name):
            score += 50

        if (search_name and search_name in entity_known_names
            or search_commercial_name and search_commercial_name in entity_known_names):
            score += 25

        return score

    @log_execution_time
    def findEntityLocally (self, entity_list, search_entity):
        print(f"Trying to find a match for the entity in the database.\n")

        if not any(search_entity.get(key) for key in ['entityIdNumber', 'name', 'commercialName']):
            raise ValueError("Provide at least one search criteria.")

        search_id_number = search_entity.get('entityIdNumber')
        if search_id_number and not any(e.get('entityIdNumber') == search_id_number for e in entity_list):
            logging.info(f"No matches were found for the current entityIdNumber")
            return None

        scored_entities = [
            (entity, self.calculateScore(entity, search_entity))
            for entity in entity_list
        ]
        
        scored_entities = [(entity, score) for entity, score in scored_entities if score > 0]
        if not scored_entities:
            return None
        
        scored_entities.sort(key=lambda x: x[1], reverse=True)
        
        return scored_entities[0][0]

    @log_execution_time
    def evaluateCurrentEntityValues (self, entity: EntityRequest, found_entity: Entity):
        logging.info(f"Evaluating the current Entity to update it's values if needed. {found_entity.id}\n")
        hasChange = False
        
        if entity.get('entityIdNumber') and entity['entityIdNumber'] == found_entity.entityIdNumber:
            if (entity.get('name') and entity['name'] != found_entity.name):
                name = normalize_string(entity['name'])
                name_not_in_known_names = name not in found_entity.knownNames
                if name_not_in_known_names:
                    found_entity.knownNames.append(name)
                    hasChange = True
                
            if(entity.get('commercialName') and entity['commercialName'] != found_entity.commercialName):    
                commercial_name = normalize_string(entity['commercialName'])
                commercial_name_not_in_known_names = commercial_name not in found_entity.knownNames
                if commercial_name_not_in_known_names:
                    found_entity.knownNames.append(commercial_name)
                    hasChange = True
        
        if (entity.get('entityType') and entity['entityType'] != found_entity.entityType):
            found_entity.entityType = entity['entityType']
            hasChange = True
        
        if (entity.get('relatedEntityType') and entity['relatedEntityType'] != found_entity.relatedEntityType):
            found_entity.relatedEntityType = entity['relatedEntityType']
            hasChange = True
            
        if(hasChange):
            try:
                found_entity.updatedOn = datetime.now(timezone.utc)
                entity_dict = found_entity.to_dict()
                SourceApiService.__entity_repository.update_entity(entity_dict)
                logging.info(f"Entity with entity Id Number - {found_entity.entityIdNumber} has been updated in the database.")
            except Exception as e:
                logging.error(f'Error saving entity to Cosmos DB: {str(e)}')
                raise Exception("Could not save the entity to Cosmos DB.")

    @log_execution_time
    def retrieveMatchesFromResults (self, entity: EntityRequest, found_entity: Entity, source_codes: list):
        result_ids_to_query = [] 
        source_codes_not_found = []
        logging.info(f"Initialized the proccess for matching the sources to Consult and retrieve the corresponding result Id's in the Entity from the DB.\n")
        
        if entity.get('consultSources'):
            for consult_source in entity['consultSources']:
                source_code = consult_source['sourceCode']
                if any(item["sourceCode"] == source_code for item in source_codes):
                    match = next((lr for lr in found_entity.latestResults if str(lr.sourceCode) == str(source_code)), None)
                    if match:
                        result_ids_to_query.append({"resultId": match.resultId, "sourceCode": source_code})
                    else:
                        source_codes_not_found.append(source_code)
        return result_ids_to_query, source_codes_not_found

    def findFieldKeyword (self, found_entity: Entity, source: Source, entity = None):
        identifiers = source.identifiers
        field_keyword = ""
        
        if identifiers is None or len(identifiers) <= 0:
            field_keyword = getattr(found_entity, 'commercialName', '')
        
        # Prioritize fields based on the order: entityIdNumber, name, commercialName
        prioritized_fields = ['entityIdNumber', 'name', 'commercialName']
        
        for priority_field in prioritized_fields:
            for identifier in identifiers:
                field = identifier.field
                if field != priority_field:
                    continue
                
                field_value = getattr(found_entity, field, None)
                
                if field_value not in (None, ""):
                    field_keyword = field_value
                    break
                if field_value in (None, "") and entity is not None and entity.get(field) not in (None, ""):
                    field_keyword = entity[field]
                    break
                if field_value in (None, "") and field in ['commercialName', 'name']:
                    if field == 'commercialName':
                        field_keyword = entity.get('name') if entity is not None and entity.get('name') not in (None, "") else getattr(found_entity, 'name', None)
                    else:
                        field_keyword = entity.get('commercialName') if entity is not None and entity.get('commercialName') not in (None, "") else getattr(found_entity, 'commercialName', None)
                    break
            if field_keyword:
                break

        return field_keyword
       
    @log_execution_time         
    def sendNoMatchesToServiceBus(self, check_sources_list, found_entity: Entity, source_codes_not_found, service_bus, source_list, original_entityType, original_entityRelation, entity, headers):
        logging.info("Some sources were not found. Initialized the process of webscrapping for these process.\n")
        
        if source_list is None and len(source_list) <= 0:
            return (500, "No sources were found for Service Bus messages.")
        
        try:
            listSources = ""
            messages = []
            for source_code in source_codes_not_found:
                if source_code == SOURCE_CODE_GAFI:
                    continue
                if not any(source['sourceCode'] == source_code for source in check_sources_list):
                    continue
                try:
                    source = next((src for src in source_list if src['sourceCode'] == source_code), None)
                    if source is None:
                        continue
                    source_model = Source.from_dict(source)
                    field_keyword = self.findFieldKeyword(found_entity, source_model, entity)
                    
                    if field_keyword in (None, ""):
                        continue
                    
                    simplified_entity = {
                        "id": found_entity.id,
                        "entityType": original_entityType,
                        "relatedEntityType": original_entityRelation,
                        "entityIdNumber": found_entity.entityIdNumber,
                        "name": found_entity.name,
                        "commercialName": found_entity.commercialName
                    }
                    message_body = {
                        "sourceCode": source_code,
                        "keyword": field_keyword,
                        "entity": simplified_entity,
                        "artifactId": headers.get('artifactId', ''),
                        "objectId": headers.get('objectId', ''),
                        "tenantId": headers.get('tenantId', ''),
                        "ihubKey": headers.get('ihubKey', ''),
                        "endPointUrl": headers.get('endPointUrl', ''),
                        "apiRequestManagerId": headers.get('apiRequestManagerId', ''),
                        "userId": headers.get('userId', '')
                    }
                    messages.append(json.dumps(message_body))
                    listSources += f"- {source_code}"
                except Exception as e:
                    logging.error(f"Failed to send message for source code {source_code}: {e}")
            
            if messages:
                enqueue_service_bus_messages(service_bus, os.environ.get("WEBSCRAP_QUEUE"), messages, found_entity.id)
            else:
                logging.warning("No valid messages were prepared for the Service Bus.")
            
            logging.info(f'Send to Service Bus all of the sources not found in the resultList from the provided Entity ({found_entity.entityIdNumber}) {listSources}\n')
            return (400, "One or more of the provided external databases are being looked up for the due diligence process. Please, wait a moment for the function to finish")
        except Exception as e:
            logging.error(f'Error sending message to Service Bus: {str(e)}')
            raise Exception("Could not send message.")

    @log_execution_time
    def queryForEachMatch(self, original_entity, original_entityRelation, result_ids_to_query, result_list, result, source_codes_not_found):
        logging.info("Initialized the process of finding the results with the corresponding resultId.\n")
        
        try:
            for query_item in result_ids_to_query:
                # Find the document with the matching sourceCode
                result_match = next((res for res in result_list if res['sourceCode'] == query_item['sourceCode'] and res['resultId'] == query_item['resultId']), None)
                if result_match:
                    
                    arrayResult = result_match.get("results", None)
                    
                    if arrayResult is not None and isinstance(arrayResult, dict):
                        arrayResult = [arrayResult]
                    
                    result_to_insert = {
                        'resultId': result_match.get("resultId"),
                        'entityId': result_match.get("entityId"),
                        "relatedEntityType": original_entityRelation,
                        "entityType": original_entity.entityType,
                        "entityIdNumber": original_entity.entityIdNumber,
                        "name": original_entity.name,
                        "commercialName": original_entity.commercialName,
                        'requestStatus': result_match.get("requestStatus", None),
                        'results': arrayResult,
                        'createdOn': result_match.get("created_on", None),
                        'updatedOn': result_match.get("updated_on", None),
                    }
                    source_index = None
                    for index, res in enumerate(result):
                        if res['sourceCode'] == query_item['sourceCode']:
                            source_index = index
                            break

                    if source_index is not None:
                        result[source_index]['sourceResults'].append(result_to_insert)
                    else:
                        result_array = []
                        result_array.append(result_to_insert)
                        result.append({"sourceCode": result_match['sourceCode'], "sourceResults": result_array})

                else:
                    source_codes_not_found.append(query_item['sourceCode'])
                    logging.warning(f"There was an error stablishing a match for the provided resultId in the source: {query_item['sourceCode']}")

        except Exception as e:
            logging.error(f'Error querying Cosmos DB: {str(e)}')
            raise Exception("An internal server error ocurred during query conditions match.")
        
        return (200,"The results from the external databases were returned for the current process.")
    
    def createNewEntity(self, entity):
        logging.info(f'Entity has not been found and was sent to the serviceBus and the database.\n')
        try:
            response = SourceApiService.__entity_repository.generate_new_entity(entity)
        except Exception as e:
            logging.error(f"An error has ocurred in entity evaluation: {e}")
            raise Exception("An internal server error ocurred during entity evaluation")
        return Entity.from_dict(response)
    
    def sendNewEntityToServiceBus(self, new_entity, sourceDocs, check_sources_list, headers, service_bus):
        new_entity_id = new_entity.id
        listSources = ""
        messages = []
        for source in sourceDocs:
            if source['sourceCode'] == SOURCE_CODE_GAFI:
                continue 
            if not any(src['sourceCode'] == source['sourceCode'] for src in check_sources_list):
                continue
            try:
                source_model = Source.from_dict(source)
                field_keyword = self.findFieldKeyword(new_entity, source_model)
                
                if field_keyword in (None, ""):
                    continue
                
                simplified_entity = {
                    "id": new_entity_id,
                    "entityIdNumber": new_entity.entityIdNumber,
                    "entityType": new_entity.entityType,
                    "relatedEntityType": new_entity.relatedEntityType,
                    "name": new_entity.name,
                    "commercialName": new_entity.commercialName
                }
                message_body = {
                    "sourceCode": source_model.sourceCode,
                    "keyword": field_keyword,
                    "entity": simplified_entity,
                    "artifactId": headers.get('artifactId', ''),
                    "objectId": headers.get('objectId', ''),
                    "tenantId": headers.get('tenantId', ''),
                    "ihubKey": headers.get('ihubKey', ''),
                    "endPointUrl": headers.get('endPointUrl', ''),
                    "apiRequestManagerId": headers.get('apiRequestManagerId', ''),
                    "userId": headers.get('userId', '')
                }
                messages.append(json.dumps(message_body))
                listSources += f"- {source_model.sourceCode}"
            except Exception as e:
                logging.error(f"Failed to send message for source code {source_model.sourceCode}: {e}")
        
        
        if messages:
            enqueue_service_bus_messages(service_bus, os.environ.get("WEBSCRAP_QUEUE"), messages, new_entity_id)
        else:
            logging.warning("No valid messages were prepared for the Service Bus.")        
        
        logging.info(f'Send to Service Bus all sources for the new provided Entity ({new_entity_id} - {new_entity.entityIdNumber})\n {listSources}')

    def sendAdverseMediaToServiceBus(self, original, found, new, service_bus, headers):
        logging.info("Starting adverse media message enqueue process for a single entity")
        messages = []

        if found:  # Entidad encontrada
            
            keywords = generate_entity_keywords(original, found)

            for keyword in keywords:
                simplified_entity = {
                    "id": found.id,
                    "entityType": original.get("entityType"),
                    "relatedEntityType": original.get("relatedEntityType"),
                    "entityIdNumber": found.entityIdNumber,
                    "name": found.name,
                    "commercialName": original.get("commercialName")
                }

                message_body = {
                    "sourceCode": SOURCECODE_ADVERSE_MEDIA,
                    "keyword": keyword,
                    "entity": simplified_entity,
                    "artifactId": headers.get('artifactId', ''),
                    "objectId": headers.get('objectId', ''),
                    "tenantId": headers.get('tenantId', ''),
                    "ihubKey": headers.get('ihubKey', ''),
                    "endPointUrl": headers.get('endPointUrl', ''),
                    "apiRequestManagerId": headers.get('apiRequestManagerId', ''),
                    "userId": headers.get('userId', '')
                }

                messages.append(message_body)

            session_id = found.id

        else:  # Entidad nueva
            keywords = generate_entity_keywords({
                "name": new.name,
                "commercialName": original.get("commercialName")
            })

            for keyword in keywords:
                simplified_entity = {
                    "id": new.id,
                    "entityIdNumber": new.entityIdNumber,
                    "entityType": new.entityType,
                    "relatedEntityType": new.relatedEntityType,
                    "name": new.name,
                    "commercialName": original.get("commercialName")
                }

                message_body = {
                    "sourceCode": SOURCECODE_ADVERSE_MEDIA,
                    "keyword": keyword,
                    "entity": simplified_entity,
                    "artifactId": headers.get('artifactId', ''),
                    "objectId": headers.get('objectId', ''),
                    "tenantId": headers.get('tenantId', ''),
                    "ihubKey": headers.get('ihubKey', ''),
                    "endPointUrl": headers.get('endPointUrl', ''),
                    "apiRequestManagerId": headers.get('apiRequestManagerId', ''),
                    "userId": headers.get('userId', '')
                }

                messages.append(message_body)

            session_id = new.id  

        if messages:
            enqueue_service_bus_messages(
                service_bus,
                os.environ.get("WEBSCRAP_QUEUE"),
                messages,
                session_id
            )
            logging.info(f'<Adverse Media> Send to Service Bus all sources for the new provided Entity ({session_id} - {simplified_entity.entityIdNumber})\n BG1')
        else:
            logging.warning("No valid <Adverse Media> messages were prepared for the Service Bus.")