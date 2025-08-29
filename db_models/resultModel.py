import uuid
from datetime import datetime, timezone
from typing import List, Optional

class Result:
    def __init__(self, 
                 id: str = None, 
                 sourceCode: str = None,
                 resultId: str = None, 
                 entityId: str = None, 
                 requestResponse: Optional[str] = None, 
                 requestStatus: Optional[str] = None, 
                 results: Optional[List[dict]] = None, 
                 createdOn: datetime = None, 
                 updatedOn: datetime = None):
        self.id = id or str(uuid.uuid4())
        self.sourceCode = sourceCode
        self.resultId = resultId or str(uuid.uuid4())
        self.entityId = entityId
        self.requestResponse = requestResponse
        self.requestStatus = requestStatus
        self.results = results or []
        self.createdOn = createdOn or datetime.now()
        self.updatedOn = updatedOn or datetime.now()

    def to_dict(self):
        sourceresult_id = self.id if self.id is not None else str(uuid.uuid4())
        result_id = self.resultId if self.resultId is not None else str(uuid.uuid4())
        
        current_datetime_utc = datetime.now(timezone.utc).isoformat()
        created_on = self.createdOn.isoformat() if self.createdOn else current_datetime_utc
        updated_on = self.updatedOn.isoformat() if self.updatedOn else current_datetime_utc
        return {
            'id': sourceresult_id,
            'sourceCode': self.sourceCode,
            'resultId': result_id,
            'entityId': self.entityId,
            'requestResponse': self.requestResponse,
            'requestStatus': self.requestStatus,
            'results': self.results,
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


        return Result(
            id=data.get('id'),
            sourceCode=data.get('sourceCode'),
            resultId=data.get('resultId'),
            entityId=data.get('entityId'),
            requestStatus=data.get('requestStatus'),
            results=data.get('results', []),
            createdOn=datetime.fromisoformat(created_on_str) if created_on_str else None,
            updatedOn=datetime.fromisoformat(updated_on_str) if updated_on_str else None,
        )
    
    def __str__(self):
        return (
            f"Result("
            f"id={self.id}, "
            f"sourceCode={self.sourceCode}, "
            f"resultId={self.resultId}, "
            f"entityId={self.entityId}, "
            f"requestResponse={self.requestResponse}, "
            f"requestStatus={self.requestStatus}, "
            f"results={self.results}, "
            f"createdOn={self.createdOn}, "
            f"updatedOn={self.updatedOn}"
            f")"
        )