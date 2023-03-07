import itertools
import json
import logging
from datetime import datetime
from typing import Tuple, Any

import markov_clustering as mc
import networkx as nx
import pymongo
from community import community_louvain

from data_parser import get_foaf_multithread


def bot_check_results(
        user: dict
) -> bool:
    """
    Checks whether a given user is a bot,
    :param user: a user to check.
    :return: True if the user is predicted to be a bot, otherwise False
    """
    bot_clusters = [1, 3, 7, 24, 35, 158]
    if user['cluster'] in bot_clusters or user['gosvon_bot']:
        return True
    return False


def enrich_users_data(
        db_client: pymongo.MongoClient,
) -> None:
    """
    Adds FOAF attributes to the users in the database.
    :param db_client: The Mongo client to connect to.
    :return:
    """
    users = db_client.dataVKnodup.users.find(
        {'enriched': {'$ne': True}}
    ).limit(20)
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
    """
    Get similarity of two VK users based on FOAF features.
    :param users: A Tuple of users to calculate similarity for.
    :return: Similarity score.
    """
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
                similarities.append(
                    get_real_similarity(
                        (users[0][feature], users[1][feature])
                    )
                )
            elif typ == 'nominal':
                similarities.append(
                    get_nominal_similarity(
                        (users[0][feature], users[1][feature])
                    )
                )
    avg_similarity = sum(similarities) / len(similarities)
    return avg_similarity


def get_nominal_similarity(
        values: Tuple
) -> bool:
    """
    Get the similarity of two nominal features.
    :param values: Values of two nominal features.
    :return: True if values are equal, otherwise False.
    """
    return values[0] == values[1]


def get_real_similarity(
        values: Tuple
) -> float:
    """
    Get the similarity of two real-number features.
    :param values: Values of two real-number features.
    :return: Similarity score for the values
    """
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
            self.clusters.append(
                tuple(self.users[i]['vk_id'] for i in cluster)
            )
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
            modularities[
                (self.sim_threshold, self.inflation_rate)
            ] = modularity
        best_sim_threshold, best_infl_rate = max(
            modularities, key=modularities.get
        )
        best_modularity = max(modularities.values())
        logging.info(f"Found best params: "
                     f"{best_sim_threshold}, "
                     f"{best_infl_rate}, "
                     f"{best_modularity}")
        self.sim_threshold = best_sim_threshold
        self.inflation_rate = best_infl_rate
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


def get_adj_matrix(
        similarities,
        with_weights=False
) -> list:
    """
    Get an adjacency matrix from user similarities.
    :param similarities: Array of dicts with similarities
            Example: [{'user1': 1, 'user2': 2, 'similarity': 0.5},...]
    :param with_weights: If the resulting graph should be weighted or not.
    :return: The adjacency matrix.
             Example: [(1,2),...] or [(1,2,0.5),...] if with_weights==True
    """
    if with_weights:
        adj_matrix: [Tuple[Any, Any, Any]] = []
        for sim in similarities:
            adj_matrix.append((sim['user1'], sim['user2'], sim['similarity']))
        return adj_matrix
    adj_matrix: [Tuple[Any, Any]] = []
    for sim in similarities:
        adj_matrix.append((sim['user1'], sim['user2']))
    return adj_matrix


def get_clusters(
        graph: nx.Graph,
) -> nx.Graph:
    """
    Cluster the users using Louvain algorithm.
    :param graph:
    :return: An updated graph
    """
    partition = community_louvain.best_partition(graph)
    nx.set_node_attributes(graph, partition, 'cluster')
    return graph


def get_is_friend(
        graph: nx.Graph,
) -> nx.Graph:
    """
    Step 2: Match the user ids with friends file (real users).
    :param graph:
    :return:
    """
    with open('friends.json', 'r') as f:
        friends = json.load(f)
    is_friend = dict.fromkeys(graph.nodes, 0)
    for k, v in friends.items():
        if int(k) in is_friend:
            is_friend[int(k)] = 1
        if v:
            for _ in v:
                if _ in is_friend:
                    is_friend[_] = 1
    nx.set_node_attributes(graph, is_friend, 'is_friend')
    return graph


def get_centrality_metrics(
        graph: nx.Graph
) -> nx.Graph:
    """
    Step 3: Calculate centrality metrics:
            - degree centrality
            - eigenvector centrality
            - clustering coefficient
    :param graph:
    :return: Three centrality metrics
    """
    degree_centrality = nx.degree_centrality(graph)
    # eigenvector_centrality = nx.eigenvector_centrality(graph)
    clustering_coefficient = nx.clustering(graph)
    for k, v in clustering_coefficient.items():
        clustering_coefficient[k] = float(round(v, 6))
    nx.set_node_attributes(
        graph,
        degree_centrality,
        'degree_centrality'
    )
    # nx.set_node_attributes(
    #     graph,
    #     eigenvector_centrality,
    #     'eigenvector_centrality'
    # )
    nx.set_node_attributes(
        graph,
        clustering_coefficient,
        'clustering_coefficient'
    )
    return graph


def analyse_sentiment(
        sentiment_analyser,
        text: str
) -> list:
    """
    Get a sentiment for a text.
    :param sentiment_analyser: Object that can analyse sentiments.
    :param text: Text to analyse.
    :return: List with positive and negative sentiments.
    """
    result = sentiment_analyser.getSentiment(text, score='dual')
    return result


def get_average_sentiment(
        graph: nx.Graph,
        db_client: pymongo.MongoClient
) -> nx.Graph:
    positive_sentiments = {}
    negative_sentiments = {}
    overall_sentiments = {}
    for u in graph.nodes:
        if 'avg_pos_sent' not in graph.nodes[u].keys():
            comments = db_client.dataVKnodup.comments.find(
                {'from_id': int(u), 'sentiment': {'$exists': 1}}
            )
            sentiments = [c['sentiment'] for c in comments]
            correct_sentiments = []
            for s in sentiments:
                if isinstance(s[0], list):
                    correct_sentiments.append(s[0])
                else:
                    correct_sentiments.append(s)
            sentiments = correct_sentiments
            pos_sent = [s[0] for s in sentiments]
            neg_sent = [s[1] for s in sentiments]
            sent = [(s[0] + s[1]) / 2 for s in sentiments]
            avg_pos_sent = sum(pos_sent) / len(pos_sent) if pos_sent else 0
            avg_neg_sent = sum(neg_sent) / len(neg_sent) if neg_sent else 0
            avg_sent = sum(sent) / len(sent) if sent else 0
            positive_sentiments[u] = avg_pos_sent
            negative_sentiments[u] = avg_neg_sent
            overall_sentiments[u] = avg_sent
        else:
            positive_sentiments[u] = graph.nodes[u]['avg_pos_sent']
            negative_sentiments[u] = graph.nodes[u]['avg_neg_sent']
            overall_sentiments[u] = graph.nodes[u]['avg_sent']
    nx.set_node_attributes(graph, positive_sentiments, 'avg_pos_sent')
    nx.set_node_attributes(graph, negative_sentiments, 'avg_neg_sent')
    nx.set_node_attributes(graph, overall_sentiments, 'avg_sent')
    return graph
