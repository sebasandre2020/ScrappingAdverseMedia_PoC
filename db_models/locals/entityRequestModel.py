import unicodedata
import re
from typing import List, Optional
from utils.constants import ENTITY_TYPE_ORG
from utils.normalize_string import normalize_string, normalize_string_special_chars
from utils.return_empty_string import return_empty_string

class Person:
    def __init__(self, 
                 country: Optional[str] = None,
                 relatedType: Optional[str] = None,
                 entityType: Optional[str] = None, 
                 entityIdNumber: Optional[str] = None, 
                 name: Optional[str] = None, 
                 commercialName: Optional[str] = None):
        self.country = country
        self.relatedType = relatedType
        self.entityType = entityType
        self.entityIdNumber = entityIdNumber
        self.name = name
        self.commercialName = commercialName

    def to_dict(self):
        return {
            'country': self.country,
            'relatedType': self.relatedType,
            'entityType': self.entityType,
            'entityIdNumber': self.entityIdNumber,
            'name': self.name,
            'commercialName': self.commercialName
        }

    @staticmethod
    def from_dict(data: dict):
        return Person(
            country=data.get('country'),
            relatedType=data.get('relatedType'),
            entityType=data.get('entityType'),
            entityIdNumber=data.get('entityIdNumber'),
            name=data.get('name'),
            commercialName=data.get('commercialName')
        )

class ConsultSource:
    def __init__(self, sourceCode: str):
        self.sourceCode = sourceCode

    def to_dict(self):
        return {
            'sourceCode': self.sourceCode
        }

    @staticmethod
    def from_dict(data: dict):
        return ConsultSource(
            sourceCode=data.get('sourceCode')
        )

class EntityRequest:
    def __init__(self, 
                 country: Optional[str] = None, 
                 entityType: Optional[str] = None, 
                 entityIdNumber: Optional[str] = None, 
                 name: Optional[str] = None, 
                 commercialName: Optional[str] = None, 
                 related_entities: Optional[List[Person]] = None,
                 consultSources: Optional[List[ConsultSource]] = None,
                 duplicated_values_am: Optional[List[str]] = None,
                 duplicated_values_amn: Optional[List[str]] = None):
        self.country = country
        self.entityType = entityType
        self.entityIdNumber = entityIdNumber
        self.name = name
        self.commercialName = commercialName
        self.related_entities = related_entities if related_entities is not None else []
        self.consultSources = consultSources if consultSources is not None else []
        self.duplicated_values_am = duplicated_values_am if duplicated_values_am is not None else []
        self.duplicated_values_amn = duplicated_values_amn if duplicated_values_amn is not None else []

    def to_dict(self):
        return {
            'country': self.country,
            'entityType': self.entityType,
            'entityIdNumber': self.entityIdNumber,
            'name': self.name,
            'commercialName': self.commercialName,
            'related_entities': [person.to_dict() for person in self.related_entities],
            'consultSources': [source.to_dict() for source in self.consultSources],
            'duplicated_values_am': self.duplicated_values_am,
            'duplicated_values_amn': self.duplicated_values_amn
        }
        
    @staticmethod
    def from_dict(data: dict):
        return EntityRequest(
            country=data.get('country'),
            entityType=data.get('entityType'),
            entityIdNumber=data.get('entityIdNumber'),
            name=data.get('name'),
            commercialName=data.get('commercialName'),
            related_entities=[Person.from_dict(person_dict) for person_dict in data.get('related_entities', [])],
            consultSources=[ConsultSource.from_dict(cs) for cs in data.get('consultSources', [])],
            duplicated_values_am = data.get('duplicated_values_am'),
            duplicated_values_amn = data.get('duplicated_values_amn')
        )
        
    def aggregate_roles(self):
        
        def find_and_update_person_related_type(person, aggregated_list):
            for existing_person in aggregated_list:
                if (return_empty_string(existing_person['entityIdNumber']) == return_empty_string(person.entityIdNumber) and
                    normalize_string(return_empty_string(existing_person['name'])) == normalize_string(return_empty_string(person.name)) and
                    normalize_string(return_empty_string(existing_person['commercialName'])) == normalize_string(return_empty_string(person.commercialName))):
   
                    new_related_type = person.relatedType
                    if new_related_type not in existing_person['relatedEntityType']:
                        existing_person['relatedEntityType'] += f", {new_related_type}"
                    return True
            return False

            
        aggregated_list = []
        
        source_codes_dicts = [source.to_dict() for source in self.consultSources]
        
        normalized_entityIdNumber = normalize_string_special_chars(self.entityIdNumber)
        normalized_name = normalize_string_special_chars(self.name)
        normalized_commercialName = normalize_string_special_chars(self.commercialName)
        # if (
        #     not normalized_entityIdNumber and
        #     not normalized_name and
        #     not normalized_commercialName
        # ): 
        #     return aggregated_list
        
        aggregated_list.append({
            'country': self.country,
            'relatedEntityType': ENTITY_TYPE_ORG,
            'entityIdNumber': normalized_entityIdNumber,
            'entityType': self.entityType,
            'name': normalized_name,
            'commercialName': normalized_commercialName,
            'consultSources': source_codes_dicts
        })
        
        for person in self.related_entities:
            normalized_entityIdNumber = normalize_string_special_chars(person.entityIdNumber)
            normalized_name = normalize_string_special_chars(person.name)
            normalized_commercialName = normalize_string_special_chars(person.commercialName)
            if (
                not normalized_entityIdNumber and
                not normalized_name and
                not normalized_commercialName
            ):            
                continue
            if not find_and_update_person_related_type(person, aggregated_list):
                aggregated_list.append({
                    'country': person.country,
                    'relatedEntityType': person.relatedType,
                    'entityIdNumber': normalized_entityIdNumber,
                    'entityType': person.entityType,
                    'name': normalized_name,
                    'commercialName': normalized_commercialName,
                    'consultSources': source_codes_dicts
                })

        return aggregated_list
    
    def empty_values (self):
        if not self.entityType and not self.entityIdNumber and not self.name and not self.commercialName:
            return True
        if not self.consultSources:
            return True
        return False