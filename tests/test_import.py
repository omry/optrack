from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import mongomock

from optrack.optrack import (Leg, Position, Strategy, get_positions,
                             import_csv, load_csv, CSVLine)

def test_load_csv():
    file = Path(__file__).parent.absolute() / "data" / "open1.csv"
    csv = load_csv(file)
    assert csv == [CSVLine(
        date=date(2022,3,17),
        action="Sell to Open",
        symbol="SHOP 04/22/2022 550.00 P",
        desc= "PUT SHOPIFY INC $550 EXP 04/22/22", quantity=1,
        price=Decimal("21.07"),
        fees=Decimal("0.66"),
        amount=Decimal("2106.34")
    )]


def test_empty():
    trans = mongomock.MongoClient()["optrack"]["transactions"]
    assert list(trans.find({})) == []


def test_open1():
    client = mongomock.MongoClient()
    file = Path(__file__).parent.absolute() / "data" / "open1.csv"
    csv = load_csv(file)
    import_csv(client, csv)

    pos = get_positions(client)

    assert len(pos) == 1
    assert pos[0] == Position(
        Strategy.CUSTOM,
        legs=[
            Leg(
                symbol="SHOP",
                expiration=date.strptime("04/22/2022", "%m/%d/%Y"),
                quantity=1,
                open_price=Decimal("21.07"),
                close_price=None,
            )
        ],
    )
