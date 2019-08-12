from collections import Counter, defaultdict
from itertools import combinations
from itertools import islice
from pathlib import Path
import logging

from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration

import pandas as pd


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
        self.data_frame = pd.DataFrame()

    def top_property(self, not_include):
        # self.remove_rare_relations(0.1)
        # self.remove_overlapping_relation_groups()
        self.data_frame = pd.DataFrame(list(self.predicate_popularity().items()), columns=['predicate', 'popularity'])
        self.data_frame['popularity'] = self.data_frame['popularity'].astype('float64')
        # self.data_frame['overlapping'] = self.data_frame['predicate'].map(self.non_overlapping_entities_counter())
        self.data_frame['big_groups'] = self.data_frame['big_groups'].astype('float64')
        self.data_frame['score'] = self.data_frame['popularity']
        self.data_frame = self.data_frame[~self.data_frame['predicate'].isin(not_include)]
        if self.data_frame['score'].size == 0:
            return
        return self.data_frame.loc[self.data_frame['score'].idxmax()]['predicate']

    # @lru_cache(maxsize=None)
    def property_counter(self, threshold=1):
        """
        :param threshold: values smaller then this value are discarded
        :return: mapping from property to number of entities with this property
        """
        return Counter({key: len(value) for key, value in self.property_mapping.items() if len(value) > threshold})

    # @lru_cache(maxsize=None)
    def group_counter(self):
        return Counter(c[0] for c in self.property_counter())

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
            count = sum(len(relation_targets) for _, relation_targets in properties.items())
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
        """
        :return: mapping from predicate to connected objects
        """
        selected_relations = self.group_counter().keys()
        selected_relation_group = {
            selected_relation: {x[1] for x in self.property_mapping if x[0] == selected_relation} for
            selected_relation in selected_relations}
        return selected_relation_group

    def group_sizes(self):
        """
        :return: mapping from predicate to list of connected groups sizes
        """
        result = defaultdict(list)
        for predicate, relation_group in self.relation_groups().items():
            for object_ in relation_group:
                result[predicate].append(len(self.property_mapping[(predicate, object_)]))
        return result

    def predicate_popularity(self):
        """
        :return: mapping from predicate to proportion of entities, which have a outgoing edge with this predicate
        """
        result = dict()
        for predicate, objects in self.relation_groups().items():
            predicate_objects = set()
            for object_ in objects:
                predicate_objects = predicate_objects.union(self.property_mapping[(predicate, object_)])
            result[predicate] = len(predicate_objects)
        return Counter(result)

    def non_overlapping_entities_counter(self):
        return {predicate_: len(self.non_overlapping_entities(predicate_)) for predicate_ in self.relation_groups()}

    def non_overlapping_entities(self, predicate):
        result = set()
        relation_groups = self.relation_groups()[predicate]
        for object_ in relation_groups:
            result = result.symmetric_difference(self.property_mapping[(predicate, object_)])
        return result

    def score(self, relation):
        pass
