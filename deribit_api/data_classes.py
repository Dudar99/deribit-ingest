"""
Module for models definition
"""
from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class DeribitInstrument:
    name: str
    kind: str
    id: int
    expiration_timestamp: int


@dataclass
class InstrumentData:
    ticks: List[int]
    open: List[float]
    close: List[float]
    high: List[float]
    low: List[float]

    df: List[Tuple[int, float, float, float, float]] = None

    def __post_init__(self):
        data = list(zip(self.ticks, self.open, self.close, self.high, self.low))
        self.df = data