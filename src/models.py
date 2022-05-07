from typing import Any

from pydantic import BaseModel


class ConsumerGroup(BaseModel):
    name: str
    consumers: int
    pending: int
    last_delivered_id: str


class Consumer(BaseModel):
    name: str
    pending: int
    idle: int


class StreamsMessage(BaseModel):
    id: str
    data: Any
