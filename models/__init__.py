import pymongo  # type: ignore
from community import community_louvain
from tqdm import tqdm

from data_parser import get_foaf_multithread, get_friends_graph
from typing import Tuple
from datetime import datetime
import itertools
import networkx as nx
import markov_clustering as mc  # type: ignore
import json
import logging
from sentistrength import PySentiStr


def bot_check_results(user_id):
    return True


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


def get_adj_matrix(similarities, with_weights=False):
    adj_matrix = []
    if with_weights:
        for sim in similarities:
            adj_matrix.append((sim['user1'], sim['user2'], sim['similarity']))
    else:
        for sim in similarities:
            adj_matrix.append((sim['user1'], sim['user2']))
    return adj_matrix


def get_clusters(db_client, api):
    start = datetime.now()
    users = list(db_client.dataVKnodup.users.find({'friends': 29}))
    edges = get_friends_graph(users, api, db_client)
    G = nx.Graph()
    G.add_edges_from(edges)
    print(f"Initial graph size: {len(G.nodes)}")
    # To remove unconnected users
    G = G.edge_subgraph(G.edges())
    print(f"After removal graph size: {len(G.nodes)}")
    partition = community_louvain.best_partition(G)
    clusters = {}
    for k, v in partition.items():
        if v in clusters.keys():
            clusters[v].append(k)
        else:
            clusters[v] = [k]
    # for k, v in clusters.items():
    #     db_client.dataVKnodup.clusters.insert_one({str(k): v})
    print('Number of clusters: ', len(clusters))
    print('Time elapsed: ', datetime.now() - start)
    return clusters


"""Code for the 5-step approach"""


# Step 1: Get graph and save it to a file
def get_clustered_graph(db_client, api):
    users = list(db_client.dataVKnodup.users.find(
        {"friends": {'$type': 'array'}},
        {'vk_id': 1, 'friends': 1, '_id': 0}
    ))
    vk_ids = set([u['vk_id'] for u in users])
    G = nx.Graph()
    clusters = list(db_client.dataVKnodup.clusters.find({}, {'_id': 0}))
    for cluster in clusters:
        for key, usrs in cluster.items():
            for user in usrs:
                if user in vk_ids:
                    G.add_node(user, cluster=key)
    edges = get_friends_graph(users, api, db_client)
    G.add_edges_from(edges)
    # To remove unconnected users
    G = G.edge_subgraph(G.edges())
    data = nx.node_link_data(G)
    print(f'Graph size: {len(G.nodes)}')
    with open('outputs/graph_friends.json', 'w') as f:
        json.dump(data, f)


# Step 2: See which users got into which graph
def get_user_characteristics(
        db_client,
        path_to_graph: str = 'outputs/graph_friends.json'
):
    with open(path_to_graph, 'r') as f:
        graph = json.load(f)
    G = nx.node_link_graph(graph)
    deactivated_values = dict.fromkeys(G.nodes)
    verified_values = dict.fromkeys(G.nodes)
    friends_values = dict.fromkeys(G.nodes)
    all_friends = set()
    with open('friends.json', 'r') as f:
        friends = json.load(f)
    for k, v in friends.items():
        all_friends.add(int(k))
        if v:
            for i in v:
                all_friends.add(i)
    for i, user in tqdm(enumerate(G.nodes)):
        record = db_client.dataVKnodup.users.find({'vk_id': user}, {
            'deactivated': 1,
            'verified': 1,
            '_id': 0
        })[0]
        deactivated_values[user] = record['deactivated']
        if 'verified' in record:
            verified_values[user] = record['verified']
        else:
            verified_values[user] = 0
        friends_values[user] = 1 if user in all_friends else 0
    nx.set_node_attributes(G, deactivated_values, 'deactivated')
    nx.set_node_attributes(G, verified_values, 'verified')
    nx.set_node_attributes(G, friends_values, 'is_friend')
    data = nx.node_link_data(G)
    with open('outputs/graph_friends_enriched.json', 'w') as f:
        json.dump(data, f)


# Step 3: Calculate centrality metrics
def get_centrality_metrics(
        path_to_graph: str = 'outputs/graph_friends_enriched.json'
) -> Tuple:
    with open(path_to_graph, 'r') as f:
        graph = json.load(f)
    G = nx.node_link_graph(graph)
    degree_centrality = nx.degree_centrality(G)
    eigenvector_centrality = nx.eigenvector_centrality(G)
    clustering_coefficient = nx.clustering(G)
    return degree_centrality, eigenvector_centrality, clustering_coefficient


# Step 4: Sentiment analysis
def analyse_sentiment(text):
    senti = PySentiStr()
    senti.setSentiStrengthPath('/Users/gingy/SentiStrength.jar')
    senti.setSentiStrengthLanguageFolderPath('/Users/gingy/SentiStrength_Data_RU')
    result = senti.getSentiment(text, score='dual')
    return result
