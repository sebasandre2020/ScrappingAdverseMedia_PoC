import uuid
from datetime import datetime, timezone
from typing import Optional

class SourceSchedule:
    def __init__(self,
                 sourceCode: str = None, 
                 days: int = None):
        self.sourceCode = sourceCode
        self.days = days

    def to_dict(self):
        return {
            'sourceCode': self.sourceCode,
            'days': self.days   
        }

    @staticmethod
    def from_dict(data: dict):
        return SourceSchedule(
            sourceCode=data.get('sourceCode'),
            days=data.get('days'),
        )
        
    def __eq__(self, other):
        if isinstance(other, SourceSchedule):
            return (self.sourceCode == other.sourceCode
                    and self.days == other.days)
        return False
    
    def __str__(self):
        return f"Source(sourceName={self.sourceName}, days={self.days})"