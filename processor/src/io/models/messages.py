from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, field_validator


class PriceMsg(BaseModel):
    symbol: str
    price: float
    time: str

    @field_validator("time", mode="before")
    @classmethod
    def _time_to_iso(cls, v):
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.isoformat()
        if isinstance(v, str):
            return v
        raise TypeError("time must be a datetime or ISO 8601 string")

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()


class NewsMsg(BaseModel):
    time: str
    title: str
    url: Optional[str] = None
    source: str
    sentiment: float

    @field_validator("time", mode="before")
    @classmethod
    def _time_to_iso(cls, v):
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.isoformat()
        if isinstance(v, str):
            return v
        raise TypeError("time must be a datetime or ISO 8601 string")

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()


class EnrichedNewsMsg(BaseModel):
    event_id: Optional[str] = None
    time: str
    title: str
    url: Optional[str] = None
    source: str
    sentiment: float
    label: Optional[str] = None
    confidence: Optional[float] = None

    @field_validator("time", mode="before")
    @classmethod
    def _time_to_iso(cls, v):
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.isoformat()
        if isinstance(v, str):
            return v
        raise TypeError("time must be a datetime or ISO 8601 string")

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
