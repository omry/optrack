from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Any

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


@dataclass(frozen=True)
class CSVLine:
    # Transaction date
    str_date: str  # as string
    date: datetime  # as date

    # Transaction type
    action: Action

    # Symbol
    symbol: str

    # Transaction description
    desc: str

    # #stocks or #contracts if applicable
    quantity: Optional[float]

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
            quantity=param["quantity"],
            price=Decimal(param["price"][1:]),
            fees=Decimal(param["fees"][1:]),
            amount=Decimal(param["amount"][1:]),
        )

    @classmethod
    def init_from_str_array(cls, line: List[str]) -> "CSVLine":
        if "as of" in line[0]:
            line[0] = line[0].split("as of ")[1]

        date = datetime.strptime(line[0], "%m/%d/%Y")
        action = Action.from_string(line[1])
        symbol = line[2]
        desc = line[3]
        quantity = float(line[4]) if len(line[4]) > 0 else None

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
        return f"{self.date}:{self.action.value}_#{self.quantity}_{self.symbol}@{self.price}"


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
    # number of contracts in this leg. This is the sum of opening contract lines
    # negative value means short positions
    quantity: Optional[int] = None
    # average price for open
    open_price: Optional[Decimal] = None
    # average price for close
    close_price: Optional[Decimal] = None

    # A leg can have multiple transactions of the same type.
    # e.g. multiple transaction opening (selling 2 contracts in two transactions).
    lines: List[CSVLine] = field(default_factory=lambda: list())

    # update all computed fields
    def finalize(self) -> None:
        opens = []
        closes = []
        for cl in self.lines:
            if cl.action in [Action.SELL_TO_OPEN, Action.BUY_TO_OPEN]:
                opens.append((cl.price, cl.quantity))
            if cl.action in [Action.SELL_TO_CLOSE, Action.BUY_TO_CLOSE]:
                closes.append((cl.price, cl.quantity))

        def wavg(lst: Any) -> Decimal:
            s = 0
            t = 0
            for a in lst:
                s += a[0] * a[1]
                t += a[1]
            return Decimal(s / t)

        if len(opens) > 0:
            self.open_price = wavg(opens)
        if len(closes) > 0:
            self.close_price = wavg(closes)

        quantity = 0
        for cl in self.lines:
            if cl.action in [Action.BUY_TO_OPEN, Action.BUY_TO_CLOSE]:
                quantity += cl.quantity
            if cl.action in [Action.SELL_TO_OPEN, Action.SELL_TO_CLOSE]:
                quantity -= cl.quantity

        self.quantity = quantity

    def is_closed(self) -> bool:
        return self.quantity == 0


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
            found_leg = leg
        else:
            for cl in leg.lines:
                found_leg.lines.append(cl)

        found_leg.finalize()


def load_csv(filename: str) -> List[CSVLine]:
    import csv

    lines = []
    with open(filename, "r") as file:
        reader = csv.reader(file)
        lnum = 1
        for row in reader:
            try:
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
        insert["fees"] = f"${insert['fees']}"
        insert["price"] = f"${insert['price']}"
        insert["amount"] = f"${insert['amount']}"
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
            "action": {"$in": ["BUY_TO_OPEN", "SELL_TO_OPEN"]},
            # TODO: order by date
        }
    )

    positions_map = {}
    positions = []
    for x in ret:
        assert x["price"][0] == "$"
        pos = None
        if x["symbol"] in positions_map:
            pos = positions_map[x["symbol"]]
            if pos.is_closed():
                pos = None

        if pos is None:
            pos = Position(strategy=Strategy.CUSTOM, legs=[])
            positions_map[x["symbol"]] = pos
            positions.append(pos)

        leg = Leg(symbol=x["symbol"], lines=[CSVLine.init_from_transaction(x)])

        pos.add_leg(leg)

    return positions
