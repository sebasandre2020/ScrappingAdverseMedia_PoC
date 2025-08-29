import uuid
from datetime import datetime, timezone
from typing import List, Optional

class LatestResult:
    def __init__(self, sourceCode: str, resultId: str):
        self.sourceCode = sourceCode
        self.resultId = resultId

    def to_dict(self):
        return {
            'sourceCode': self.sourceCode,
            'resultId': self.resultId
        }

    @staticmethod
    def from_dict(data: dict):
        return LatestResult(
            sourceCode=data.get('sourceCode'),
            resultId=data.get('resultId')
        )
    
    def __str__(self):
        return f"[LatestResult(sourceCode={self.sourceCode}, resultId={self.resultId})]"

class Entity:
    def __init__(self, 
                 id: str = None, 
                 entityType: str = None, 
                 relatedEntityType: str = None, 
                 entityIdNumber: str = None, 
                 country: Optional[str] = None, 
                 parentIds: Optional[List[str]] = None, 
                 name: Optional[str] = None, 
                 commercialName: Optional[str] = None, 
                 knownNames: Optional[List[str]] = None, 
                 latestResults: Optional[List[LatestResult]] = None, 
                 createdOn: datetime = None, 
                 updatedOn: datetime = None):
        self.id = id
        self.entityType = entityType
        self.relatedEntityType = relatedEntityType
        self.entityIdNumber = entityIdNumber
        self.country = country
        self.parentIds = parentIds or []
        self.name = name
        self.commercialName = commercialName
        self.knownNames = knownNames or []
        self.latestResults = latestResults or []
        self.createdOn = createdOn
        self.updatedOn = updatedOn

    def to_dict(self):
        entity_id = self.id if self.id is not None else str(uuid.uuid4())
        
        current_datetime_utc = datetime.now(timezone.utc).isoformat()
        created_on = self.createdOn.isoformat() if self.createdOn else current_datetime_utc
        updated_on = self.updatedOn.isoformat() if self.updatedOn else current_datetime_utc

        return {
            'id': entity_id,
            'entityType': self.entityType,
            'relatedEntityType': self.relatedEntityType,
            'entityIdNumber': self.entityIdNumber,
            'country': self.country,
            'parentIds': self.parentIds,
            'name': self.name,
            'commercialName': self.commercialName,
            'knownNames': self.knownNames,
            'latestResults': [consult_source.to_dict() for consult_source in self.latestResults],
            'createdOn': created_on,
            'updatedOn': updated_on,
        }

    @staticmethod
    def from_dict(data: dict):
        created_on_str = data.get('createdOn', '')
        updated_on_str = data.get('updatedOn', '')

        # Only attempt to replace 'Z' if the strings are not empty
        created_on_str = created_on_str.replace('Z', '+00:00') if created_on_str else created_on_str
        updated_on_str = updated_on_str.replace('Z', '+00:00') if updated_on_str else updated_on_str


        return Entity(
            id=data.get('id'),
            entityType=data.get('entityType'),
            relatedEntityType=data.get('relatedEntityType'),
            entityIdNumber=data.get('entityIdNumber'),
            country=data.get('country'),
            parentIds=data.get('parentIds', []),
            name=data.get('name'),
            commercialName=data.get('commercialName'),
            knownNames=data.get('knownNames', []),
            latestResults=[LatestResult.from_dict(cs) for cs in data.get('latestResults', [])],
            createdOn=datetime.fromisoformat(created_on_str) if created_on_str else None,
            updatedOn=datetime.fromisoformat(updated_on_str) if updated_on_str else None,
        )
        
    def __eq__(self, other):
        if isinstance(other, Entity):
            return (self.id == other.id
                    and self.entityType == other.entityType
                    and self.relatedEntityType == other.relatedEntityType
                    and self.entityIdNumber == other.entityIdNumber
                    and self.country == other.country
                    and self.parentIds == other.parentIds
                    and self.name == other.name
                    and self.commercialName == other.commercialName
                    and self.knownNames == other.knownNames
                    and self.latestResults == other.latestResults
                    and self.createdOn == other.createdOn
                    and self.updatedOn == other.updatedOn)
        return False
    
    def __str__(self):
        return f"Entity(id={self.id}, entityType={self.entityType}, entityType={self.relatedEntityType}, entityIdNumber={self.entityIdNumber}, country={self.country}, parentIds={self.parentIds}, name={self.name}, commercialName={self.commercialName}, knownNames={self.knownNames}, latestResults={self.latestResults}, createdOn={self.createdOn}, updatedOn={self.updatedOn})"
