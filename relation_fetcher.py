from collections import defaultdict
from pathlib import Path

import asyncio
import csv
import os

from redis import Redis

from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration
from wikidata_endpoint.return_types import UriReturnType


class NullDatabase:
    def __init__(self, *args, **kwargs):
        pass

    def sadd(self, *args, **kwargs):
        pass

    def save(self, *args, **kwargs):
        pass


class RelationFetcherCache:
    def __init__(self, *args, **kwargs):
        self.filename = "relation_fetcher_cache.csv"
        self.cache = {}
        open_modifier = "a"

        if os.path.exists(self.filename) and os.stat(self.filename).st_size > 0:
            with open(self.filename, "r+") as csv_file:
                csv_reader = csv.reader(csv_file)
                next(csv_reader)
                for row in csv_reader:
                    self._add(row[0], row[1], row[2])
        else:
            open_modifier = "w+"

        self.persisted_cache = open(self.filename, open_modifier)
        self.csv_writer = csv.writer(self.persisted_cache)
        if open_modifier == "w+":
            self.csv_writer.writerow(["subject", "predicate", "object"])

    def sadd(self, *args, **kwargs):
        subject = args[0]
        predicate = args[1]
        object_ = args[2]

        self.csv_writer.writerow([subject, predicate, object_])
        self._add(subject, predicate, object_)

    def _add(self, subject, predicate, object_):
        relations = self.cache.get(subject, None)
        if not relations:
            relations = []
            self.cache[subject] = relations
        relations.append((predicate, object_))

    def get(self, subject):
        return self.cache.get(subject, None)

    def save(self, *args, **kwargs):
        self.persisted_cache.flush()

    def __del__(self):
        self.save()
        self.persisted_cache.close()


class RelationFetcher:

    def __init__(self, wikidata_ids, entities_per_query, endpoint=None, redis_config=None):
        self.entities_per_query = entities_per_query
        self.entities_fetched = 0
        self.wikidata_ids = wikidata_ids
        self.redis = RelationFetcherCache() if not redis_config else Redis(**redis_config)
        self.endpoint = endpoint or WikidataEndpoint(
            WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini")))

    async def get_relations(self, wikidata_ids, relations_map):

        cached_ids = set()
        for id in wikidata_ids:
            subject = f"http://www.wikidata.org/entity/Q{id}"
            relations = self.redis.get(subject)
            if relations:
                for predicate, object_ in relations:
                    relations_map[(UriReturnType(predicate), UriReturnType(object_))].add(
                        UriReturnType(subject))
                cached_ids.add(id)

        joined_ids = ""
        for id in wikidata_ids:
            if id not in cached_ids:
                joined_ids += f"wd:Q{id} "

        if joined_ids != "":
            query = open('resources/get_relations.rq').read() % joined_ids
            with self.endpoint.request() as request:
                for results in request.post(query):
                    subject, predicate, object_ = results.values()
                    relations_map[(predicate, object_)].add(subject)
                    self.redis.sadd(subject.value, predicate.value, object_.value)

        self.entities_fetched += len(wikidata_ids)
        print(f"{self.entities_fetched} entities fetched.")

    async def fetch(self):
        self.entities_fetched = 0
        relations_entity_map = defaultdict(set)
        # wikidata ids sollen als Liste übergeben werden
        print(f"Fetching {len(self.wikidata_ids)} entities.")
        await asyncio.gather(*map(
            lambda x: self.get_relations(self.wikidata_ids[x: min(x + self.entities_per_query, len(self.wikidata_ids))],
                                         relations_entity_map),
            range(0, len(self.wikidata_ids), self.entities_per_query)))
        return relations_entity_map

    def __del__(self):
        self.redis.save()
