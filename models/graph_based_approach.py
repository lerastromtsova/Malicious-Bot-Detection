"""
Steps:
1. Collect user features - DONE:
    - FOAF XML ya:created
    - FOAF XML ya:created timezone
    - FOAF XML ya:subscribersCount
    - FOAF XML ya:subscribedToCount
    - comment rate: number of all comments by user in db
    - deactivated - not used as a feature but used in
      cluster analysis later on. Already present in the db.
2. Write user similarity function - DONE
   Different for:
    - nominal data type
    - real data type
3. Construct multi-attributed graph Gm
4. Convert to similarity graph Gs using (2)
5. Construct similarity matrix from Gs - DONE
6. Apply Markov clustering to the matrix - DONE:
    - expansion
    - inflation
7. Analyse each cluster one by one
"""
import pymongo  # type: ignore
from data_parser import get_foaf_multithread, get_activity_count
from typing import Tuple
from datetime import datetime
import pandas as pd
import itertools
import networkx as nx
import markov_clustering as mc  # type: ignore
import json
import logging


def enrich_users_data(
        db_client: pymongo.MongoClient,
) -> None:
    users = db_client.dataVKnodup.users.find({'enriched': {'$ne': True}}).limit(20)
    foaf = get_foaf_multithread([u['vk_id'] for u in users])
    for user in foaf:
        # activity = get_activity_count(user['vk_id'], db_client)
        if user['created_at']:
            vk_age = (datetime(2022, 8, 1, 0, 0, 0) - user['created_at']).days
        else:
            vk_age = None
        db_client.dataVKnodup.users.update_one(
            {'vk_id': user['vk_id']},
            {'$set': {
                'created_at': user['created_at'],
                'vk_age': vk_age,
                'timezone': user['timezone'],
                'followee_rate': user['followee_rate'],
                'follower_rate': user['follower_rate'],
                'follower_to_followee': user['follower_to_followee'],
                'enriched': True
            }}
        )


def get_similarity(
        users: Tuple
) -> float:
    features = {
        'vk_age': 'real',
        'timezone': 'nominal',
        'followee_rate': 'real',
        'follower_rate': 'real',
        'follower_to_followee': 'real',
        'comment_rate': 'real'
    }
    similarities = []
    for feature, typ in features.items():
        if users[0][feature] and users[1][feature]:
            if typ == 'real':
                similarities.append(get_real_similarity((users[0][feature], users[1][feature])))
            elif typ == 'nominal':
                similarities.append(get_nominal_similarity((users[0][feature], users[1][feature])))
    avg_similarity = sum(similarities) / len(similarities)
    return avg_similarity


def get_nominal_similarity(
        values: Tuple
) -> bool:
    return values[0] == values[1]


def get_real_similarity(
        values: Tuple
) -> float:
    return 1 / (1 + abs(values[0] - values[1]))


class MarkovClusteringModel:
    def __init__(
            self,
            users,
            sim_threshold=0.6,
            inf_rate=1.1
    ):
        self.users = users
        self.sim_threshold = sim_threshold
        self.inflation_rate = inf_rate
        self.adjacency_matrix = {}
        self.clusters = []
        self.matrix = []
        self.raw_clusters = []
        self.modularity = 0

    def _get_adjacency_matrix(self):
        result = {}
        for pair in itertools.product(self.users, repeat=2):
            if pair[0]['vk_id'] != pair[1]['vk_id']:
                sim = get_similarity(pair)
                if sim >= self.sim_threshold:
                    result[(pair[0]['vk_id'], pair[1]['vk_id'])] = sim
        self.adjacency_matrix = result

    def _get_clusters(self):
        self.clusters = []
        edges = []
        for ids, adj in self.adjacency_matrix.items():
            edges.append((ids[0], ids[1], adj))
        graph = nx.from_edgelist([(item[0], item[1]) for item in edges])
        print(graph.nodes)
        graph.add_weighted_edges_from(edges)
        self.matrix = nx.to_scipy_sparse_array(graph)
        result = mc.run_mcl(self.matrix, inflation=self.inflation_rate)
        self.raw_clusters = mc.get_clusters(result)
        for cluster in self.raw_clusters:
            self.clusters.append(tuple(self.users[i]['vk_id'] for i in cluster))
        return result, self.raw_clusters

    def train(self, sim_thresholds, inflation_rates):
        logging.info("===TRAINING===")
        self._get_adjacency_matrix()
        modularities = {}
        for pair in itertools.product(sim_thresholds, inflation_rates):
            self.sim_threshold, self.inflation_rate = pair
            self._get_adjacency_matrix()
            result, clusters = self._get_clusters()
            modularity = mc.modularity(matrix=result, clusters=clusters)
            modularities[(self.sim_threshold, self.inflation_rate)] = modularity
        best_sim_threshold, best_infl_rate = max(modularities, key=modularities.get)
        best_modularity = max(modularities.values())
        logging.info(f"Found best params: "
                     f"{best_sim_threshold}, "
                     f"{best_infl_rate}, "
                     f"{best_modularity}")
        self.sim_threshold, self.inflation_rate = best_sim_threshold, best_infl_rate
        self.modularity = best_modularity
        self._get_adjacency_matrix()
        self._get_clusters()
        logging.info("===TRAINING END===")

    def draw_graph(self):
        mc.draw_graph(
            self.matrix,
            self.raw_clusters,
            node_size=50,
            with_labels=False,
            edge_color="silver"
        )

    def save(self, filepath):
        with open(filepath, 'w') as f:
            json.dump({
                'similarity_threshold': str(self.sim_threshold),
                'inflation_rate': str(self.inflation_rate),
                'modularity': str(self.modularity),
                'clusters': [
                    str(cluster) for cluster in self.clusters
                ]
            }, f)

    def read_from_saved(self, filepath):
        with open(filepath, 'r') as f:
            content = json.load(f)
            self.sim_threshold = float(content['similarity_threshold'])
            self.inflation_rate = float(content['inflation_rate'])
            self.modularity = float(content['modularity'])

    def get_clusters(self):
        self._get_adjacency_matrix()
        print(len(self.adjacency_matrix))
        self._get_clusters()
