from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import mongomock

from optrack.options import (
    Leg,
    Position,
    Strategy,
    get_positions,
    import_csv,
    load_csv,
    CSVLine,
    Action,
)


def test_load_csv():
    file = Path(__file__).parent.absolute() / "data" / "open1.csv"
    csv = load_csv(file)
    assert csv == [
        CSVLine(
            str_date="03/17/2022",
            date=datetime(2022, 3, 17, 0, 0, 0),
            action=Action.SELL_TO_OPEN,
            symbol="SHOP 04/22/2022 550.00 P",
            desc="PUT SHOPIFY INC $550 EXP 04/22/22",
            quantity=1,
            price=Decimal("21.07"),
            fees=Decimal("0.66"),
            amount=Decimal("2106.34"),
        )
    ]


def test_import_csv():
    file = Path(__file__).parent.absolute() / "data" / "open1.csv"
    csv = load_csv(file)

    client = mongomock.MongoClient()
    import_csv(client, csv)
    trans = client["optrack"]["transactions"]
    ret = list(trans.find({}))
    assert len(ret) == 1
    del ret[0]["insertion_date"]
    del ret[0]["last_update_date"]
    assert list(ret) == [
        {
            "_id": "2022-03-17 00:00:00:3_#1_SHOP 04/22/2022 550.00 P@21.07",
            "action": "SELL_TO_OPEN",
            "amount": "$2106.34",
            "date": datetime(2022, 3, 17, 0, 0),
            "desc": "PUT SHOPIFY INC $550 EXP 04/22/22",
            "fees": "$0.66",
            "price": "$21.07",
            "quantity": 1,
            "str_date": "03/17/2022",
            "symbol": "SHOP 04/22/2022 550.00 P",
            "option_type": "PUT",
            "underlying": "SHOP",
            "strike": "550.00",
            "expiration": "04/22/2022",
        }
    ]


def test_csv_line_roundtrip():
    file = Path(__file__).parent.absolute() / "data" / "open1.csv"
    csv = load_csv(file)
    client = mongomock.MongoClient()
    import_csv(client, csv)
    trans = client["optrack"]["transactions"]
    ret = list(trans.find({}))
    assert len(ret) == 1
    assert CSVLine.init_from_transaction(ret[0]) == csv[0]


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
                symbol="SHOP 04/22/2022 550.00 P",
                quantity=-1,
                open_price=Decimal("21.07"),
                close_price=None,
                lines=[csv[0]],
            )
        ],
    )


def test_open1_two_transactions():
    client = mongomock.MongoClient()
    file = Path(__file__).parent.absolute() / "data" / "open1_two_transactions.csv"
    csv = load_csv(file)
    import_csv(client, csv)

    pos = get_positions(client)

    assert len(pos) == 1
    assert pos[0] == Position(
        Strategy.CUSTOM,
        legs=[
            Leg(
                symbol="SHOP 04/22/2022 550.00 P",
                quantity=-5,
                open_price=Decimal("21.60"),
                close_price=None,
                lines=[csv[0], csv[1]],
            ),
        ],
    )
