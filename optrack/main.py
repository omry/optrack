from logging import getLogger

import hydra
from omegaconf import DictConfig
from pymongo import MongoClient

from optrack import options

log = getLogger(__name__)


@hydra.main(config_path="../conf", config_name="config")
def main(cfg: DictConfig) -> None:
    client = MongoClient(cfg.db.url)
    if cfg.action == "import":
        log.info(f"Importing {cfg.input.file}")
        lines = options.load_csv(cfg.input.file)
        options.import_csv(client, lines)
    elif cfg.action == "list":
        log.info("Listing positions")
        positions = options.get_positions(client)
        for pos in positions:
            print(f"Position: {pos.strategy}")
            for leg in pos.legs:
                print(f"\t{leg.symbol}, contracts={leg.quantity_sum()} open={leg.open_price_avg()}, close={leg.close_price_avg()}")


if __name__ == "__main__":
    main()
