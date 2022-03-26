from logging import getLogger

import hydra
from omegaconf import DictConfig
from pymongo import MongoClient

from optrack import optrack

log = getLogger(__name__)


@hydra.main(config_path="../conf", config_name="config")
def main(cfg: DictConfig) -> None:
    client = MongoClient(cfg.db.url)
    if cfg.action == "import":
        log.info("Importing")
        lines = optrack.load_csv(cfg.input.file)
        optrack.import_csv(client, lines)
    elif cfg.action == "list":
        log.info("Listing")
        positions = optrack.get_positions(client)
        for pos in positions:
            print(pos)


if __name__ == "__main__":
    main()
