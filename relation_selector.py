from collections import Counter
from itertools import combinations
from functools import lru_cache
from itertools import chain, islice
from pathlib import Path
import logging

from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration


class TimeoutException(Exception):
    pass


def overlap_coefficient(set1, set2):
    return len(set1 & set2) / min(len(set1), len(set2))


def chunk_dictionary(dictionary, chunk_size):
    it = iter(dictionary)
    for _ in range(0, len(dictionary), chunk_size):
        yield {k: dictionary[k] for k in islice(it, chunk_size)}


class RelationSelector:

    def __init__(self, relations_mapping, endpoint=None):
        self.property_mapping = relations_mapping
        self.number_entities = len(set().union(*relations_mapping.values()))
        self.endpoint = endpoint or WikidataEndpoint(
            WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini")))

    # @lru_cache(maxsize=None)
    def relation_counter(self, threshold=1):
        return Counter({key: len(value) for key, value in self.property_mapping.items() if len(value) > threshold})

    # @lru_cache(maxsize=None)
    def group_counter(self):
        return Counter(c[0] for c in self.relation_counter())

    # @lru_cache(maxsize=None)
    def global_relation_counter(self, chunk_size=800):
        global_count = Counter()
        chunks = chunk_dictionary(self.property_mapping, chunk_size)
        while chunks:
            timeout_chunks = []
            for chunk in chunks:
                global_query = open('resources/global_relation_count.rq').read() % ' '.join(
                    f'({x[0].sparql_escape()} {x[1].sparql_escape()})' for x in chunk)
                with self.endpoint.request() as request:
                    try:
                        request_result = request.post(global_query,
                                                      on_timeout=lambda x: exec('raise(TimeoutException)'))
                    except TimeoutException:
                        logging.error("Request timeout: Will try again with smaller chunks")
                        timeout_chunks.append(chunk_dictionary(chunk, 3))
                        continue
                    chunk_global_counts = {(x['predicate'], x['object']): int(x['global_count'].value) for x in
                                           request_result}
                global_count.update(chunk_global_counts)
            chunks = timeout_chunks
        return global_count

    def remove_small_relation_groups(self, threshold):
        self.property_mapping = {key: value for key, value in self.property_mapping.items() if len(value) > threshold}

    def remove_rare_relations(self, threshold):
        for relation in self.group_counter():
            properties = {key: value for key, value in self.property_mapping.items() if key[0] == relation}
            count = sum(len(relation_targets) for property, relation_targets in properties.items())
            if count < threshold * self.number_entities:
                for key in properties:
                    self.property_mapping.pop(key, None)

    def remove_unique_relations(self):
        self.remove_small_relation_groups(1)

    def remove_overlapping_relation_groups(self):
        selected_relation_group = self.relation_groups()
        for relation, relation_targets in selected_relation_group.items():
            groups = (self.property_mapping[(relation, relation_target)] for relation_target in relation_targets)
            for (group1, label1), (group2, label2) in combinations(zip(groups, relation_targets), 2):
                overlap_coefficient_value = overlap_coefficient(group1, group2)
                if overlap_coefficient_value > 0.3:
                    removed_label, kept_label = (label1, label2) if len(group1) < len(group2) else (label2, label1)
                    self.property_mapping.pop((relation, removed_label), None)
                    print(f'remove smaller group after intersection: {removed_label}, kept{kept_label}')

    def relation_groups(self):
        selected_relations = self.group_counter().keys()
        selected_relation_group = {
            selected_relation: {x[1] for x in self.property_mapping if x[0] == selected_relation} for
            selected_relation in selected_relations}
        return selected_relation_group

    def score(self, relation):
        pass
