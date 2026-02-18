from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, AwareDatetime


class PriceMsg(BaseModel):
    symbol: str
    price: float
    time: AwareDatetime

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()


class NewsMsg(BaseModel):
    time: AwareDatetime
    title: str
    url: Optional[str] = None
    source: str
    sentiment: float

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()


class EnrichedNewsMsg(BaseModel):
    event_id: Optional[str] = None
    time: AwareDatetime
    title: str
    url: Optional[str] = None
    source: str
    sentiment: float
    label: Optional[str] = None
    confidence: Optional[float] = None

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()


class SummaryRequestMsg(BaseModel):
    event_id: Optional[str] = None
    time: str
    symbol: str
    window: str
    direction: str
    ret: float
    threshold: float
    headline: Optional[str] = None
    sentiment: Optional[float] = None

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()


class AlertMsg(BaseModel):
    event_id: str
    time: str
    symbol: str
    window: str
    direction: str
    ret: float
    threshold: float
    headline: Optional[str] = None
    sentiment: Optional[float] = None
    summary: str

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()
