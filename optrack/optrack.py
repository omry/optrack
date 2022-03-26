from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional

from pymongo import MongoClient


@dataclass(frozen=True)
class CSVLine:
    # Transaction date
    date: date

    # Transaction type
    action: str

    # Symbol
    symbol: str

    # Transaction description
    desc: str

    # #stocks or #contracts
    quantity: int

    # Transaction price/proceeds (per stock/option contract)
    price: Decimal

    # Transaction fees
    fees: Decimal

    # Total cost/proceeds
    amount: Decimal

    @staticmethod
    def init_from_str_array(line : List[str]) -> "CSVLine":
        date = datetime.strptime(line[0], "%m/%d/%Y").date()
        action = line[1] # TODO: convert to enum
        symbol = line[2]
        desc = line[3]
        quantity = int(line[4])
        assert line[5][0] == "$"
        price = Decimal(line[5][1:])
        assert line[6][0] == "$"
        fees = Decimal(line[6][1:])
        assert line[7][0] == "$"
        amount = Decimal(line[7][1:])

        return CSVLine(date=date, action=action, symbol=symbol, desc=desc, quantity=quantity, price=price, fees=fees, amount=amount,)

    def is_option(self) -> bool:
        return self.action in (
            "Buy to Open",
            "Buy to Close",
            "Sell to Open",
            "Sell to Close",
        )

    def key(self) -> str:
        return f"{self.date}:{self.action}_#{self.quantity}_{self.symbol}@{self.price}"


class Strategy(Enum):
    # Unclassified strategy
    CUSTOM = 0
    SHORT_PUT = 1
    SHORT_CALL = 2
    LONG_PUT = 3
    LONG_CALL = 4


@dataclass
class Leg:
    symbol: str
    expiration: date
    # number of contracts in this leg. This is the sum of opening contract lines
    # negative value means short positions
    quantity: int
    # average price for open
    open_price: Decimal
    # average price for close
    close_price: Optional[Decimal] = None

    # A leg can have multiple transactions of the same type.
    # e.g. multiple transaction opening (selling 2 contracts in two transactions).
    lines: List[CSVLine] = field(default_factory=lambda: list())


@dataclass
class Position:
    strategy: Strategy
    legs: List[Leg] = field(default_factory=lambda: list())


def load_csv(file: str) -> List[CSVLine]:
    import csv

    lines = []
    with open(file, "r") as file:
        reader = csv.reader(file)
        next(reader)  # skip header
        for row in reader:
            row = row[0:8]  # Schwab has a trailing comma, strip the last empty item

            cl = CSVLine.init_from_str_array(row)
            lines.append(cl)

    return lines


def import_csv(client: MongoClient, lines: List[CSVLine]) -> None:
    trans = client["optrack"]["transactions"]
    for line in lines:
        now = datetime.utcnow()
        insert = asdict(line)
        insert["insertion_date"] = now
        if line.is_option():
            tokens = line.symbol.split(" ")
            insert["underlying"] = tokens[0]
            insert["expiration"] = tokens[1]
            insert["strike"] = tokens[2]
            insert["option_type"] = "PUT" if tokens[3] == "P" else "CALL"

        record = {
            "$setOnInsert": insert,
            "$set": {"last_update_date": now},
        }
        trans.update_one(
            filter={
                "_id": line.key(),
            },
            update=record,
            upsert=True,
        )


def get_positions(client: MongoClient) -> List[Position]:
    trans = client["optrack"]["transactions"]
    ret = trans.find(
        {
            "underlying": {"$exists": 1},
            "action": {"$in": ["Buy to Open", "Sell to Open"]},
        }
    )
    positions = []
    for x in ret:
        positions.append(Position(strategy=Strategy.CUSTOM, legs=[]))

    return positions
