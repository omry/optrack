from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from numbers import Number
from pathlib import Path
from typing import List, Optional, Any, Tuple, Union, Dict

from pymongo import MongoClient

from logging import getLogger

logger = getLogger(__name__)


def parse_price(s: str) -> Decimal:
    if len(s) == 0:
        return Decimal(0)
    sign = 1
    if s[0] == "-":
        sign = -1
        s = s[1:]

    assert s[0] == "$", f"Price does not start with $: {s}"
    return Decimal(s[1:]) * sign


def format_price(p: Number) -> str:
    neg = False
    if p < 0:
        neg = True
        p = p * -1
    return f"{'-' if neg else ''}${p}"


def wavg(lst: Tuple[Number, Number]) -> Decimal:
    s = 0
    t = 0
    for a in lst:
        s += a[0] * a[1]
        t += a[1]
    return Decimal(s / t)


class Action(Enum):
    BUY_TO_OPEN = 0
    BUY_TO_CLOSE = 1
    SELL_TO_CLOSE = 2
    SELL_TO_OPEN = 3
    BUY = 4
    SELL = 5
    JOURNALED_SHARES = 6
    JOURNAL = 7
    NRA_TAX_ADJ = 8
    CASH_DIVIDEND = 9
    QUALIFIED_DIVIDEND = 10
    NON_QUALIFIED_DIV = 11
    CREDIT_INTEREST = 12
    WIRE_FUNDS = 13
    MISC_CASH_ENTRY = 14
    SERVICE_FEE = 15
    MONEYLINK_TRANSFER = 16
    MONEYLINK_DEPOSIT = 17
    MONEYLINK_ADJ = 18
    STOCK_PLAN_ACTIVITY = 19
    REINVEST_SHARES = 20
    QUAL_DIV_REINVEST = 21
    REINVEST_DIVIDEND = 22
    FOREIGN_TAX_PAID = 23
    EXPIRED = 24
    MANDATORY_REORG_EXC = 25
    SPIN_OFF = 26
    FUNDS_RECEIVED = 27
    CASH_IN_LIEU = 28
    STOCK_SPLIT = (29,)
    CASH_STOCK_MERGER = (30,)

    @staticmethod
    def from_string(s: str) -> "Action":
        actions = {
            "Buy to Open": Action.BUY_TO_OPEN,
            "Buy to Close": Action.BUY_TO_CLOSE,
            "Sell to Open": Action.SELL_TO_OPEN,
            "Sell to Close": Action.SELL_TO_CLOSE,
            "Buy": Action.BUY,
            "Sell": Action.SELL,
            "Journaled Shares": Action.JOURNALED_SHARES,
            "NRA Tax Adj": Action.NRA_TAX_ADJ,
            "Qualified Dividend": Action.QUALIFIED_DIVIDEND,
            "Non-Qualified Div": Action.NON_QUALIFIED_DIV,
            "Reinvest Shares": Action.REINVEST_SHARES,
            "Qual Div Reinvest": Action.QUAL_DIV_REINVEST,
            "Journal": Action.JOURNAL,
            "Credit Interest": Action.CREDIT_INTEREST,
            "Wire Funds": Action.WIRE_FUNDS,
            "Misc Cash Entry": Action.MISC_CASH_ENTRY,
            "Service Fee": Action.SERVICE_FEE,
            "MoneyLink Deposit": Action.MONEYLINK_DEPOSIT,
            "MoneyLink Transfer": Action.MONEYLINK_TRANSFER,
            "MoneyLink Adj": Action.MONEYLINK_ADJ,
            "Stock Plan Activity": Action.STOCK_PLAN_ACTIVITY,
            "Foreign Tax Paid": Action.FOREIGN_TAX_PAID,
            "Expired": Action.EXPIRED,
            "Mandatory Reorg Exc": Action.MANDATORY_REORG_EXC,
            "Spin-off": Action.SPIN_OFF,
            "Funds Received": Action.FUNDS_RECEIVED,
            "Cash Dividend": Action.CASH_DIVIDEND,
            "Cash In Lieu": Action.CASH_IN_LIEU,
            "Reinvest Dividend": Action.REINVEST_DIVIDEND,
            "Stock Split": Action.STOCK_SPLIT,
            "Cash/Stock Merger": Action.CASH_STOCK_MERGER,
        }
        try:
            return actions[s]
        except KeyError:
            raise ValueError(
                f"Unsupported action '{s}', supported one of {list(actions.keys())}"
            )


@dataclass
class CSVLine:
    # Transaction date
    str_date: str  # as string # TODO: remove?
    date: datetime  # as date

    # Transaction type
    action: Action

    # Symbol
    symbol: str

    # Transaction description
    desc: str

    # stocks or #contracts if applicable. Can be fractional for stocks
    quantity: Optional[Decimal]

    # Transaction price/proceeds (per stock/option contract)
    price: Decimal

    # Transaction fees
    fees: Decimal

    # Total cost/proceeds
    amount: Decimal

    @classmethod
    def init_from_transaction(cls, param):
        return CSVLine(
            str_date=param["date"].strftime("%m/%d/%Y"),
            date=param["date"],
            action=Action[param["action"]],
            symbol=param["symbol"],
            desc=param["desc"],
            quantity=Decimal(param["quantity"]),
            price=parse_price(param["price"]),
            fees=parse_price(param["fees"]),
            amount=parse_price(param["amount"]),
        )

    @classmethod
    def init_from_str_array(cls, line: List[str]) -> "CSVLine":
        if "as of" in line[0]:
            line[0] = line[0].split("as of ")[1]

        date = datetime.strptime(line[0], "%m/%d/%Y")
        action = Action.from_string(line[1])
        symbol = line[2]
        desc = line[3]
        quantity = Decimal(line[4]) if len(line[4]) > 0 else None

        price = parse_price(line[5])
        fees = parse_price(line[6])
        amount = parse_price(line[7])

        return CSVLine(
            str_date=line[0],
            date=date,
            action=action,
            symbol=symbol,
            desc=desc,
            quantity=quantity,
            price=price,
            fees=fees,
            amount=amount,
        )

    def is_option(self) -> bool:
        return self.action in (
            Action.BUY_TO_OPEN,
            Action.BUY_TO_CLOSE,
            Action.SELL_TO_OPEN,
            Action.SELL_TO_CLOSE,
        )

    def key(self) -> str:
        return f"{self.date}:{self.action.value}##{self.quantity}_{self.symbol}@{self.price}"


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

    # A leg can have multiple transactions of the same type.
    # e.g. multiple transaction opening (selling 2 contracts in two transactions).
    lines: List[CSVLine] = field(default_factory=lambda: list())

    def quantity_sum(self) -> Decimal:
        quantity = Decimal(0)
        for cl in self.lines:
            if cl.action in [Action.BUY_TO_OPEN, Action.BUY_TO_CLOSE]:
                quantity += cl.quantity
            if cl.action in [Action.SELL_TO_OPEN, Action.SELL_TO_CLOSE]:
                quantity -= cl.quantity

        return quantity

    def open_price_avg(self) -> Optional[Decimal]:
        opens = []
        for cl in self.lines:
            if cl.action in [Action.SELL_TO_OPEN, Action.BUY_TO_OPEN]:
                opens.append((cl.price, cl.quantity))
        return wavg(opens) if len(opens) > 0 else None

    def close_price_avg(self) -> Optional[Decimal]:
        close = []
        for cl in self.lines:
            if cl.action in [Action.SELL_TO_CLOSE, Action.BUY_TO_CLOSE]:
                close.append((cl.price, cl.quantity))
        return wavg(close) if len(close) > 0 else None

    def is_closed(self) -> bool:
        return self.quantity_sum() == 0


@dataclass
class Position:
    strategy: Strategy
    legs: List[Leg] = field(default_factory=lambda: list())

    def is_closed(self) -> bool:
        return all(map(lambda l: l.is_closed(), self.legs))

    def add_leg(self, leg) -> None:
        found_leg = None
        for sl in self.legs:
            if sl.symbol == leg.symbol:
                found_leg = sl
                break
        if found_leg is None:
            self.legs.append(leg)
        else:
            for cl in leg.lines:
                found_leg.lines.append(cl)

    def symbols(self) -> str:
        return [x.symbol for x in self.legs]

    def __repr__(self) -> str:
        symbols = self.symbols()
        symb = symbols if len(symbols) > 1 else symbols[0]
        legs = ""
        def leg_str(x: Leg) -> str:
            if x.is_closed():
                return f"{x.symbol}: open:{x.open_price_avg()}, close:{x.close_price_avg()}"
            else:
                return f"{x.symbol}: open:{x.open_price_avg()}"

        if len(self.legs) == 0:
            legs = 'No legs'
        elif len(self.legs) == 1:
            legs = leg_str(self.legs[0])
        elif len(self.legs) > 1:
            lst = [leg_str(x) for x in self.legs]
            legs = ",".join(lst)


        return f"{symb}, {'Closed' if self.is_closed() else 'Open'}: {legs}"


def load_csv(filename: Union[Path, str]) -> List[CSVLine]:
    import csv

    lines = []
    with open(filename, "r") as file:
        reader = csv.reader(file)
        lnum = 1
        for row in reader:
            try:
                if len(row) == 0:  # skip empty lines
                    continue
                if len(row) == 1:
                    continue  # skip title header
                row = row[0:8]  # Schwab has a trailing comma, strip the last empty item
                if all(map(lambda l: not str.isnumeric(l), row)) and lnum < 3:
                    continue  # skip header
                if row[0] == "Transactions Total":
                    continue  # skip footer
                cl = CSVLine.init_from_str_array(row)
                lines.append(cl)
            except ValueError as e:
                logger.error(f"{filename} ## {lnum}: {type(e).__name__} : {e}")
            except AssertionError as e:
                logger.error(f"AssertionError importing line {lnum} from {filename}")
                raise
            finally:
                lnum = lnum + 1

    day_to_transactions: Dict[date, List[CSVLine]] = {}
    for cl in lines:
        k = cl.date.date()
        if k not in day_to_transactions:
            day_to_transactions[k] = []
        day_to_transactions[k].append(cl)

    for day, txs in day_to_transactions.items():
        second = 0
        for tx in reversed(txs):
            tx.date = tx.date + timedelta(seconds=second)
            second = second + 1

    return lines


def import_csv(client: MongoClient, lines: List[CSVLine]) -> None:
    trans = client["optrack"]["transactions"]
    for line in lines:
        if line.action not in (
            Action.BUY_TO_OPEN,
            Action.BUY_TO_CLOSE,
            Action.SELL_TO_OPEN,
            Action.SELL_TO_CLOSE,
            Action.EXPIRED,
            Action.BUY,
            Action.SELL,
        ):
            continue

        now = datetime.utcnow()
        insert = asdict(line)
        insert["insertion_date"] = now
        insert["action"] = insert["action"].name
        insert["fees"] = format_price(insert["fees"])
        insert["price"] = format_price(insert["price"])
        insert["amount"] = format_price(insert["amount"])
        insert["quantity"] = f"{insert['quantity']}"
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
            "action": {
                "$in": [
                    "BUY_TO_OPEN",
                    "SELL_TO_OPEN",
                    "BUY_TO_CLOSE",
                    "SELL_TO_CLOSE",
                ]
            },
            # TODO: order by date
        }
    )

    positions_map = {}
    positions = []
    for x in ret:
        pos = None
        if x["symbol"] in positions_map:
            pos = positions_map[x["symbol"]]
            if pos.is_closed():
                pos = None

        if pos is None:
            # new position
            # assert x["action"] in ["BUY_TO_OPEN", "SELL_TO_OPEN"]
            pos = Position(strategy=Strategy.CUSTOM, legs=[])
            positions_map[x["symbol"]] = pos
            positions.append(pos)

        leg = Leg(symbol=x["symbol"], lines=[CSVLine.init_from_transaction(x)])

        pos.add_leg(leg)

    return positions
