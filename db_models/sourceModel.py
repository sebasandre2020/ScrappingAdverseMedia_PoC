import uuid
from datetime import datetime, timezone
from typing import List, Optional

class Field:
    def __init__(self, field: str):
        self.field = field

    def to_dict(self):
        return {
            'field': self.field
        }

    @staticmethod
    def from_dict(data: dict):
        return Field(
            field=data.get('field')
        )

class Source:
    def __init__(self, 
                 id: str, 
                 sourceCode: str = None, 
                 sourceName: Optional[str] = None, 
                 lastScheduleRequest: Optional[datetime] = None, 
                 status: Optional[str] = None,
                 identifiers: Optional[List[Field]] = None, 
                 createdOn: datetime = datetime.now, 
                 updatedOn: datetime = None):
        self.id = id
        self.sourceCode = sourceCode
        self.sourceName = sourceName
        self.lastScheduleRequest = lastScheduleRequest
        self.status = status
        self.identifiers = identifiers if identifiers is not None else []
        self.createdOn = createdOn
        self.updatedOn = updatedOn

    def to_dict(self):
        entity_id = self.id if self.id is not None else str(uuid.uuid4())
        
        current_datetime_utc = datetime.now(timezone.utc).isoformat()
        created_on = self.createdOn.isoformat() if self.createdOn else current_datetime_utc
        updated_on = self.updatedOn.isoformat() if self.updatedOn else current_datetime_utc
        return {
            'id': entity_id,
            'sourceCode': self.sourceCode,
            'sourceName': self.sourceName,
            'lastScheduleRequest': self.lastScheduleRequest.isoformat() if self.lastScheduleRequest else None,
            'status': self.status,
            'identifiers': [field.to_dict() for field in self.identifiers],
            'createdOn': created_on,
            'updatedOn': updated_on,
        }

    @staticmethod
    def from_dict(data: dict):
        scheduler_on_str = data.get('lastScheduleRequest', '')
        created_on_str = data.get('createdOn', '')
        updated_on_str = data.get('updatedOn', '')

        # Only attempt to replace 'Z' if the strings are not empty
        scheduler_on_str = scheduler_on_str.replace('Z', '+00:00') if scheduler_on_str else scheduler_on_str
        created_on_str = created_on_str.replace('Z', '+00:00') if created_on_str else created_on_str
        updated_on_str = updated_on_str.replace('Z', '+00:00') if updated_on_str else updated_on_str

            
        return Source(
            id=data.get('id'),
            sourceCode=data.get('sourceCode'),
            sourceName=data.get('sourceName'),
            lastScheduleRequest=datetime.fromisoformat(scheduler_on_str) if scheduler_on_str else None,
            status=data.get('status'),
            identifiers=[Field.from_dict(field) for field in data.get('identifiers', [])],
            createdOn=datetime.fromisoformat(created_on_str) if created_on_str else None,
            updatedOn=datetime.fromisoformat(updated_on_str) if updated_on_str else None,
        )
        
    def __eq__(self, other):
        if isinstance(other, Source):
            return (self.id == other.id
                    and self.sourceCode == other.sourceCode
                    and self.sourceName == other.sourceName
                    and self.lastScheduleRequest == other.lastScheduleRequest
                    and self.status == other.status
                    and self.createdOn == other.createdOn
                    and self.updatedOn == other.updatedOn)
        return False
    
    def __str__(self):
        return f"Source(id={self.id}, sourceCode={self.sourceCode}, sourceName={self.sourceName}, lastScheduleRequest={self.lastScheduleRequest}, status={self.status}, createdOn={self.createdOn}, updatedOn={self.updatedOn})"
