from typing import Optional


class SimplifiedEntity:
    def __init__(self,
                 id: str = None,
                 entityType: str = None, 
                 relatedEntityType: str = None, 
                 entityIdNumber: str = None,
                 name: str = None,
                 commercialName: Optional[str] = None):
        self.id = id
        self.entityType = entityType
        self.relatedEntityType = relatedEntityType
        self.entityIdNumber = entityIdNumber
        self.name = name
        self.commercialName = commercialName

    def to_dict(self):
        return {
            'id': self.id,
            'entityType': self.entityType,
            'relatedEntityType': self.relatedEntityType,
            'entityIdNumber': self.entityIdNumber,
            'name': self.name,
            'commercialName': self.commercialName
        }

    @staticmethod
    def from_dict(data: dict):
        return SimplifiedEntity(
            id=data.get('id'),
            entityType=data.get('entityType'),
            relatedEntityType=data.get('relatedEntityType'),
            entityIdNumber=data.get('entityIdNumber'),
            name=data.get('name'),
            commercialName=data.get('commercialName')
        )

    def __str__(self):
        return (f"SimplifiedEntity(id={self.id}, "
                f"entityType={self.entityType}, "
                f"relatedEntityType={self.relatedEntityType}, "
                f"entityIdNumber={self.entityIdNumber}, "
                f"name={self.name}, "
                f"commercialName={self.commercialName})")


class ServiceBusMessage:
    def __init__(self,
                 sourceCode: str = None,
                 keyword: str = None,
                 entity: SimplifiedEntity = None):
        self.sourceCode = sourceCode
        self.keyword = keyword
        self.entity = entity

    def to_dict(self):
        return {
            'sourceCode': self.sourceCode,
            'keyword': self.keyword,
            'entity': self.entity.to_dict() if self.entity else None
        }

    @staticmethod
    def from_dict(data: dict):
        return ServiceBusMessage(
            sourceCode=data.get('sourceCode'),
            keyword=data.get('keyword'),
            entity=SimplifiedEntity.from_dict(data.get('entity')) if data.get('entity') else None
        )

    def __str__(self):
        return (f"ServiceBusMessage(sourceCode={self.sourceCode}, "
                f"keyword={self.keyword}, "
                f"entity={self.entity})")
