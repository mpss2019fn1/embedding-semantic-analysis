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

    async def get_relations(self, wikidata_id, relations_map):
        query = open('resources/get_relations.rq').read() % f'wd:Q{wikidata_id}'
        with self.endpoint.request() as request:
            for results in request.post(query):
                subject, predicate, object_ = results.values()
                relations_map[(predicate, object_)].add(subject)
                self.redis.sadd(f'{predicate} {object_}', subject)

    async def fetch(self):
        relations_entity_map = defaultdict(set)
        await asyncio.gather(*map(lambda x: self.get_relations(x, relations_entity_map), self.wikidata_ids))
        return relations_entity_map

    def __del__(self):
        self.redis.save()
