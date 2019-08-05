import csv
import logging
import math
import re
from multiprocessing import Process
from pathlib import Path
from typing import Set, List, Dict
import requests


def main():
    logging.basicConfig(format="%(asctime)s : [%(process)d] %(levelname)s : %(message)s", level=logging.INFO)

    cache_file: Path = Path("relation_fetcher_cache.csv")
    if not cache_file.exists():
        logging.error(f"{cache_file.absolute()} does not exist!")
        exit(1)

    entity_regex = re.compile(r".*/([Q|P]\d+)$")
    entities_to_resolve: Set[str] = set()
    with cache_file.open("r") as input_stream:
        reader: csv.reader = csv.reader(input_stream)
        next(reader)  # skip header row

        for row in reader:
            if not row:
                continue

            for i in range(len(row)):
                regex_match: re.match = entity_regex.match(row[i])
                if not regex_match:
                    continue

                entity: str = regex_match.group(1)
                entities_to_resolve.add(entity)

            # if len(entities_to_resolve) >= 100:
            #     break

    logging.info(f"{len(entities_to_resolve)} unique entities to resolve")
    entity_ids: List[str] = list(entities_to_resolve)
    number_of_workers: int = 8
    resolve_worker_batch_size: int = math.ceil(len(entity_ids) / number_of_workers)
    workers: List[ResolveWorker] = []
    for i in range(number_of_workers):
        batch: List[str] = entity_ids[i * resolve_worker_batch_size: (i + 1) * resolve_worker_batch_size]
        worker: ResolveWorker = ResolveWorker(batch)
        workers.append(worker)
        worker.start()

    entity_names: Dict[str, str] = {}
    for worker in workers:
        worker.join()
        entity_names = {**entity_names, **worker.entity_names}

    with Path("entity_names.csv").open("w+") as output_stream:
        print("entity_id,entity_label", file=output_stream)
        for entity_id, entity_name in entity_names.items():
            print(f"{entity_id},{entity_name}", file=output_stream)


class ResolveWorker(Process):

    QUERY_TEMPLATE: str = "https://www.wikidata.org/w/api.php?action=wbgetentities&props=labels&ids=%1%&format=json"

    def __init__(self, entities_to_resolve: List[str], batch_size: int = 50):
        super(ResolveWorker, self).__init__()
        self._entity_ids: List[str] = entities_to_resolve
        self._batch_size: int = batch_size
        self._entity_names: Dict[str, str] = {}

    @property
    def entity_names(self) -> Dict[str, str]:
        return self._entity_names

    def run(self) -> None:
        for i in range(0, len(self._entity_ids), self._batch_size):
            logging.info(f"resolving entity names... {i / len(self._entity_ids) * 100} %")
            batch: List[str] = self._entity_ids[i: i + self._batch_size]
            query: str = ResolveWorker.QUERY_TEMPLATE.replace("%1%", "|".join(batch))
            response = requests.get(query).json()

            self._try_unpack(response)

    def _try_unpack(self, query_result) -> None:
        try:
            if query_result["success"] != 1:
                return

            entities = query_result["entities"]
            for entity_id in entities:
                languages: List[str] = list(entities[entity_id]["labels"].keys())

                if len(languages) < 1:
                    logging.error(f"No label defined for {entity_id}")
                    continue

                language: str = "en"
                if language not in languages:
                    logging.debug(f"{entity_id} has no english label... falling back to {languages[0]}")
                    language = languages[0]

                self._entity_names[entity_id] = entities[entity_id]["labels"][language]["value"]

        except Exception as exception:
            logging.error(f"Exception {exception} during unpacking")
            pass


if __name__ == "__main__":
    main()
