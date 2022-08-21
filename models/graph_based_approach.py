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
from data_parser import get_foaf_data, get_activity_count
from typing import Tuple
from datetime import datetime
import pandas as pd
import itertools
import random
import networkx as nx
import markov_clustering as mc
import matplotlib as plt


def enrich_users_data(
        db_client: pymongo.MongoClient,
) -> None:
    users = db_client.dataVKnodup.users.find({'enriched': {'$ne': True}})
    for i, user in enumerate(users):
        foaf = get_foaf_data(user['vk_id'])
        activity = get_activity_count(user['vk_id'], db_client)
        if foaf['created_at']:
            vk_age = (datetime(2022, 8, 1, 0, 0, 0) - foaf['created_at']).days
        else:
            vk_age = None
        db_client.dataVKnodup.users.update_one(
            {'_id': user['_id']},
            {'$set': {
                'created_at': foaf['created_at'],
                'vk_age': vk_age,
                'timezone': foaf['timezone'],
                'followee_rate': foaf['followee_rate'],
                'follower_rate': foaf['follower_rate'],
                'follower_to_followee': foaf['follower_to_followee'],
                'comment_rate': activity,
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


def construct_similarity_matrix(
        users: list,
        sim_threshold: float = 0.6
) -> pd.DataFrame:
    indices = [user['vk_id'] for user in users]
    result = pd.DataFrame(
        index=indices,
        columns=indices,
        dtype=int
    )
    for pair in itertools.product(users, repeat=2):
        if pair[0]['vk_id'] == pair[1]['vk_id']:
            result.at[pair[0]['vk_id'], pair[1]['vk_id']] = 0
        else:
            sim = get_similarity(pair)
            sim_int = int(sim * 10) if sim >= sim_threshold else 0
            result.at[pair[0]['vk_id'], pair[1]['vk_id']] = sim_int
    return result


def run_markov_clustering(
        similarity_matrix: pd.DataFrame
):
    graph = nx.from_pandas_adjacency(similarity_matrix)
    matrix = nx.to_scipy_sparse_array(graph)
    inflation_rates = [i / 10 for i in range(11, 51)]
    infl_mod = {}
    for inflation in inflation_rates:
        result = mc.run_mcl(matrix, inflation=inflation)
        clusters = mc.get_clusters(result)
        q = mc.modularity(matrix=result, clusters=clusters)
        infl_mod[inflation] = q
    best_inflation = max(infl_mod, key=infl_mod.get)
    print("Best inflation", best_inflation)
    result = mc.run_mcl(matrix, inflation=best_inflation)  # run MCL with default parameters
    clusters = mc.get_clusters(result)
    mc.draw_graph(matrix, clusters, node_size=50, with_labels=False, edge_color="silver")
    result = []
    for cluster in clusters:
        result.append(tuple(similarity_matrix.columns[i] for i in cluster))
    return result


db_client = pymongo.MongoClient(f"mongodb+srv://"
                                f"lerastromtsova:fiTmr8nKcKZEB7K"
                                f"@cluster0.ubfnhtk.mongodb.net/"
                                f"?retryWrites=true&w=majority",
                                tls=True,
                                tlsAllowInvalidCertificates=True)
users = db_client.dataVKnodup.users.find({'enriched': True})
sim_matrix = construct_similarity_matrix(list(users))
run_markov_clustering(sim_matrix)
