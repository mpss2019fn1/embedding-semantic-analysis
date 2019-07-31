from collections import defaultdict
from pathlib import Path
import asyncio

from redis import Redis

from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration


class NullDatabase:
    def __init__(self, *args, **kwargs):
        pass

    def sadd(self, *args, **kwargs):
        pass

    def save(self, *args, **kwargs):
        pass


class RelationFetcher:

    def __init__(self, wikidata_ids, endpoint=None, redis_config=None):
        self.wikidata_ids = wikidata_ids
        self.redis = NullDatabase() if not redis_config else Redis(**redis_config)
        self.endpoint = endpoint or WikidataEndpoint(
            WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini")))

    async def get_relations(self, wikidata_ids, relations_map):

        joined_ids = ""
        for id in wikidata_ids:
            joined_ids += f"wd:Q{id} "

        query = open('resources/get_relations.rq').read() % joined_ids
        with self.endpoint.request() as request:
            for results in request.post(query):
                subject, predicate, object_ = results.values()
                relations_map[(predicate, object_)].add(subject)
                self.redis.sadd(f'{predicate} {object_}', subject)

    async def fetch(self):
        penis_size = 1
        relations_entity_map = defaultdict(set)
        # wikidata ids sollen als Liste übergeben werden
        await asyncio.gather(*map(
            lambda x: self.get_relations(self.wikidata_ids[x: min(x + penis_size, len(self.wikidata_ids))],
                                         relations_entity_map), range(0, len(self.wikidata_ids), penis_size)))
        return relations_entity_map

    def __del__(self):
        self.redis.save()
