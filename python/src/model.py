from pydantic import BaseModel


class MessageModel(BaseModel):
    id: str
    content: str
    timestamp: str
