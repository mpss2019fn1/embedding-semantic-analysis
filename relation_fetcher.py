from collections import defaultdict
from pathlib import Path
import asyncio

from redis import Redis
from aiostream import stream, pipe, await_

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

    async def get_relations(self, wikidata_id: int):
        query = open('resources/get_relations.rq').read() % f'wd:Q{wikidata_id}'
        with self.endpoint.request() as request:
            for results in request.post(query):
                subject, predicate, object_ = results.values()
                yield subject, predicate, object_

    def handle_relation(self, relation):
        subject, predicate, object_ = relation
        self.redis.sadd(f'{predicate} {object_}', subject)
        return {(predicate, object_): {subject}}

    async def fetch(self):
        xs = (stream.iterate(range(10))
              | pipe.map(lambda x: self.get_relations(x), await_)
              | pipe.flatten()
              | pipe.map(lambda x: self.handle_relation(x))
              | pipe.reduce(lambda x, y: {key: x.get(key, set()).union(y.get(key, set())) for key in set(x).union(y)},
                            {}))
        ys = await xs
        return ys

    def __del__(self):
        self.redis.save()
