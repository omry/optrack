from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from pytest import mark
import mongomock

from optrack.config import Filter, Range
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
            "_id": "2022-03-17 00:00:00:3##1_SHOP 04/22/2022 550.00 P@21.07",
            "action": "SELL_TO_OPEN",
            "amount": "$2106.34",
            "date": datetime(2022, 3, 17, 0, 0),
            "desc": "PUT SHOPIFY INC $550 EXP 04/22/22",
            "fees": "$0.66",
            "price": "$21.07",
            "quantity": "1",
            "str_date": "03/17/2022",
            "symbol": "SHOP 04/22/2022 550.00 P",
            "option_type": "PUT",
            "underlying": "SHOP",
            "strike": "550.00",
            "expiration": "04/22/2022",
        }
    ]


def test_import_csv2():
    file = Path(__file__).parent.absolute() / "data" / "open2.csv"
    csv = load_csv(file)

    client = mongomock.MongoClient()
    import_csv(client, csv)
    trans = client["optrack"]["transactions"]
    ret = list(trans.find({}))
    assert len(ret) == 2
    for r in ret:
        del r["insertion_date"]
        del r["last_update_date"]
    assert list(ret) == [
        {
            "_id": "2022-02-14 00:00:01:1##1_PRU 03/18/2022 110.00 P@2.54",
            "action": "BUY_TO_CLOSE",
            "amount": "-$20",
            "date": datetime(2022, 2, 14, 0, 0, 1),
            "desc": "PUT PRUDENTIAL FINL $110 EXP 03/18/22",
            "fees": "$1",
            "price": "$2.54",
            "quantity": "1",
            "str_date": "02/14/2022",
            "symbol": "PRU 03/18/2022 110.00 P",
            "option_type": "PUT",
            "underlying": "PRU",
            "strike": "110.00",
            "expiration": "03/18/2022",
        },
        {
            "_id": "2022-02-14 00:00:00:1##1_PRU 03/18/2022 110.00 P@2.54",
            "action": "BUY_TO_CLOSE",
            "amount": "-$20",
            "date": datetime(2022, 2, 14, 0, 0, 0),
            "desc": "PUT PRUDENTIAL FINL $110 EXP 03/18/22",
            "fees": "$1",
            "price": "$2.54",
            "quantity": "1",
            "str_date": "02/14/2022",
            "symbol": "PRU 03/18/2022 110.00 P",
            "option_type": "PUT",
            "underlying": "PRU",
            "strike": "110.00",
            "expiration": "03/18/2022",
        },
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
        legs=[Leg(symbol="SHOP 04/22/2022 550.00 P", lines=[csv[0]])],
    )
    leg = pos[0].legs[0]
    assert leg.quantity_sum() == -1
    assert leg.open_price_avg() == Decimal("21.07")
    assert leg.close_price_avg() is None


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
                lines=[csv[1], csv[0]],
            ),
        ],
    )
    leg = pos[0].legs[0]
    assert leg.quantity_sum() == -5
    assert leg.open_price_avg() == Decimal("21.60")
    assert leg.close_price_avg() is None


@mark.parametrize("fname", ["open_close1.csv"])
def test_open_close1(fname: str):
    client = mongomock.MongoClient()
    file = Path(__file__).parent.absolute() / "data" / fname
    csv = load_csv(file)
    import_csv(client, csv)

    pos = get_positions(client)

    assert len(pos) == 1
    assert pos[0] == Position(
        Strategy.CUSTOM,
        legs=[
            Leg(
                symbol="NVDA 04/01/2022 300.00 C",
                lines=[csv[0], csv[1]],
            ),
        ],
    )
    leg = pos[0].legs[0]
    assert leg.quantity_sum() == 0
    assert leg.open_price_avg() == Decimal("8.22")
    assert leg.close_price_avg() == Decimal("4.00")


def test_open_close2():
    client = mongomock.MongoClient()
    file = Path(__file__).parent.absolute() / "data" / "open_close2.csv"
    csv = load_csv(file)
    import_csv(client, csv)

    pos = get_positions(client)

    assert len(pos) == 1
    assert len(pos[0].legs) == 1
    assert len(pos[0].legs[0].lines) == 3
    assert pos[0] == Position(
        Strategy.CUSTOM,
        legs=[
            Leg(
                symbol="PRU 03/18/2022 100.00 P",
                lines=[csv[2], csv[1], csv[0]],
            ),
        ],
    )
    leg = pos[0].legs[0]
    assert leg.quantity_sum() == 0
    assert leg.open_price_avg() == Decimal("2")
    assert leg.close_price_avg() == Decimal("1")


def test_open_close3():
    client = mongomock.MongoClient()
    file = Path(__file__).parent.absolute() / "data" / "open_close3.csv"
    csv = load_csv(file)
    import_csv(client, csv)

    pos = get_positions(client)

    assert len(pos) == 2
    assert pos == [
        Position(
            Strategy.CUSTOM,
            legs=[
                Leg(symbol="PRU 03/18/2022 100.00 P", lines=[csv[2], csv[0]]),
            ],
        ),
        Position(
            Strategy.CUSTOM,
            legs=[
                Leg(symbol="PRU 03/18/2022 110.00 P", lines=[csv[1]]),
            ],
        ),
    ]
    assert pos[0].is_closed()
    assert not pos[1].is_closed()
    assert pos[0].legs[0].open_price_avg() == Decimal("1.66")
    assert pos[0].legs[0].close_price_avg() == Decimal("0.77")
    assert pos[1].legs[0].open_price_avg() == Decimal("1.61")
    assert pos[1].legs[0].close_price_avg() is None


def test_open_close4():
    client = mongomock.MongoClient()
    file = Path(__file__).parent.absolute() / "data" / "open_close4.csv"
    csv = load_csv(file)
    import_csv(client, csv)

    pos = get_positions(client)

    assert len(pos) == 2
    assert pos == [
        Position(
            Strategy.CUSTOM,
            legs=[
                Leg(
                    symbol="PRU 03/18/2022 100.00 P",
                    lines=[csv[5], csv[3], csv[2], csv[1], csv[0]],
                ),
            ],
        ),
        Position(
            Strategy.CUSTOM,
            legs=[
                Leg(symbol="PRU 03/18/2022 110.00 P", lines=[csv[4]]),
            ],
        ),
    ]
    assert pos[0].is_closed()
    assert not pos[1].is_closed()
    assert pos[0].legs[0].open_price_avg() == Decimal("1.66")
    assert pos[0].legs[0].close_price_avg() == Decimal("0.762")
    assert pos[1].legs[0].open_price_avg() == Decimal("1.61")
    assert pos[1].legs[0].close_price_avg() is None


def test_filter_symbol_regex() -> None:
    client = mongomock.MongoClient()
    file = Path(__file__).parent.absolute() / "data" / "open_close4.csv"
    csv = load_csv(file)
    import_csv(client, csv)

    pos = get_positions(client, filter=Filter(symbol="aaa"))
    assert len(pos) == 0
    pos = get_positions(client, filter=Filter(symbol="PRU 03/18/2022 110.00 P"))
    assert len(pos) == 1
    pos = get_positions(client, filter=Filter(symbol="PRU 03/18/2022.*"))
    assert len(pos) == 2


def test_filter_underlying() -> None:
    client = mongomock.MongoClient()
    file = Path(__file__).parent.absolute() / "data" / "open_close4.csv"
    csv = load_csv(file)
    import_csv(client, csv)

    pos = get_positions(client, filter=Filter(underlying="aaa"))
    assert len(pos) == 0
    pos = get_positions(client, filter=Filter(underlying="P"))
    assert len(pos) == 0
    pos = get_positions(client, filter=Filter(underlying="PRU"))
    assert len(pos) == 2



def test_range_query() -> None:
    client = mongomock.MongoClient()
    file = Path(__file__).parent.absolute() / "data" / "open_close4.csv"
    csv = load_csv(file)
    import_csv(client, csv)

    fltr = Filter()
    pos = get_positions(client, filter=fltr)
    assert len(pos) == 2

    fltr.range = Range(start='02/08/2022')
    pos = get_positions(client, filter=fltr)
    assert len(pos) == 1

    fltr.range = Range(end='02/08/2022')
    pos = get_positions(client, filter=fltr)
    assert len(pos) == 2