from logging import getLogger

import hydra
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
        output_date_format = "%m/%d/%Y"
        for pos in reversed(pos):
            us = pos.underlying_symbols()
            if len(us) == 1:
                print(f"Position: {us[0]}")
            else:
                print(f"Position: {us}")
            for leg in pos.legs:
                assert len(leg.lines) > 0
                dates = leg.dates()
                if len(dates) == 1:
                    datess = f"{dates[0].strftime(output_date_format)}"
                else:
                    datess = f"{dates[0].strftime(output_date_format)} -> {dates[-1].strftime(output_date_format)}"
                print(
                    f"\t{datess}: {leg.symbol}, contracts={leg.quantity_open()} open={leg.open_price_avg()}, close={leg.close_price_avg()}"
                )


if __name__ == "__main__":
    main()
