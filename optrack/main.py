from logging import getLogger

import hydra
import texttable
from pymongo import MongoClient

from optrack import options, config

log = getLogger(__name__)


@hydra.main(config_path="../conf", config_name="config")
def main(cfg: config.Config) -> None:
    client = MongoClient(cfg.db.url)
    if cfg.action == "import":
        log.info(f"Importing {cfg.input.file}")
        lines = options.load_csv(cfg.input.file)
        options.import_csv(client, lines)
    elif cfg.action == "list":
        log.info("Listing positions")
        pos = options.get_positions(client, cfg.filter)
        dateformat = cfg.output.data_format
        table = texttable.Texttable()
        table.set_max_width(cfg.output.max_table_width)
        table.header(["Date range", "Option", "#Cnt", "$open", "$close"])
        for pos in reversed(pos):
            for leg in pos.legs:
                assert len(leg.lines) > 0
                ldate = leg.dates()
                if len(ldate) == 1:
                    datestr = f"{ldate[0].strftime(dateformat)}"
                else:
                    datestr = f"{ldate[0].strftime(dateformat)} -> {ldate[-1].strftime(dateformat)}"
            close = f"{leg.close_price_avg()}" if leg.close_price_avg() is not None else ""
            table.add_row([datestr, leg.symbol, leg.quantity_open(), leg.open_price_avg(), close])

        print(table.draw())


if __name__ == "__main__":
    main()
