from dataclasses import dataclass
from typing import Optional

from hydra.core.config_store import ConfigStore
from omegaconf import MISSING


@dataclass
class DB:
    url: str = "mongodb://localhost:27017"


@dataclass
class Input:
    file: str = MISSING


@dataclass
class Range:
    # starting date >=. If missing all
    start: Optional[str] = None
    end: Optional[str] = None


@dataclass
class Filter:
    # symbol of options or stock, case-insensitive, regex
    symbol: Optional[str] = None

    # Options only: Underlying stock symbol, case-insensitive, exact match
    underlying: Optional[str] = None

    range: Range = Range()


@dataclass
class Config:
    # list, import
    action: str = "list"
    db: DB = DB()
    input: Input = Input()
    filter: Filter = Filter()


ConfigStore.instance().store(group="optrack", name="schema", node=Config)
