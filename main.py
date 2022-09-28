import logging
import os
import sys
from datetime import datetime

import networkx as nx
import pymongo
import vk
from dotenv import dotenv_values
from tqdm import tqdm

from data_parser import get_friends_graph
from models import get_average_sentiment, get_is_friend
from models import get_clusters
from models import get_centrality_metrics
from sentistrength import PySentiStr

config = dotenv_values(".env")
if not config:
    config = os.environ

logging.basicConfig(
    filename='log/data_parsing.log',
    encoding='utf-8',
    level=getattr(logging, config['LOG_LEVEL'].upper())
)
api = vk.API(access_token=config[sys.argv[1]])
db_client = pymongo.MongoClient(f"mongodb+srv://"
                                f"lerastromtsova:{config['MONGO_DB_PASSWORD']}"
                                f"@cluster0.ubfnhtk.mongodb.net/"
                                f"?retryWrites=true&w=majority",
                                tls=True,
                                tlsAllowInvalidCertificates=True)

senti = PySentiStr()
senti.setSentiStrengthPath('sentistrength/SentiStrength.jar')
senti.setSentiStrengthLanguageFolderPath('sentistrength/SentiStrength_Data_RU')


if __name__ == '__main__':

    db_client.dataVKnodup.users.update_many(
        {'friends': {'$type': 'array'}},
        {'$unset': {
            'cluster': 1,
            'degree_centrality': 1,
            'eigenvector_centrality': 1,
            'clustering_coefficient': 1
        }}
    )

    start_time = datetime.now()
    print("Start at: ", start_time)

    print('Construct user graph')
    users = list(db_client.dataVKnodup.users.find(
        {"friends": {'$type': 'array'}},
        {
            'vk_id': 1,
            'is_friend': 1,
            'friends': 1,
            'verified': 1,
            'deactivated': 1,
            'avg_pos_sent': 1,
            'avg_neg_sent': 1,
            'avg_sent': 1,
            '_id': 0
        }
    ))
    for u in users:
        if 'verified' not in u:
            u['verified'] = 0

    user_graph = nx.Graph()
    nodes = [
        (user['vk_id'],
         {
             'is_friend': user['is_friend'],
             'friends': user['friends'],
             'verified': user['verified'],
             'deactivated': user['deactivated'],
             'avg_pos_sent': user['avg_pos_sent'],
             'avg_neg_sent': user['avg_neg_sent'],
             'avg_sent': user['avg_sent'],
         })
        for user in users
    ]
    user_graph.add_nodes_from(nodes)

    edges = get_friends_graph(
        users,
        api,
        db_client,
        retrieve_friends_from_api=False
    )
    user_graph.add_edges_from(edges)
    user_graph = user_graph.edge_subgraph(user_graph.edges())

    print("Graph size: ", len(user_graph.nodes))

    print('Step 1: Cluster the users')
    user_graph = get_clusters(user_graph)

    print('Step 2: Is real person?')
    user_graph = get_is_friend(user_graph)

    print('Step 3: Calculate centrality metrics')
    user_graph = get_centrality_metrics(user_graph)

    print('Step 4: Compute average sentiment for each user')
    user_graph = get_average_sentiment(user_graph, db_client)

    for node in user_graph.nodes:
        del user_graph.nodes[node]['friends']

    print('Save to file')
    nx.write_gexf(user_graph, 'outputs/new_graph.gexf')

    print('Update data in the database')
    for node in tqdm(user_graph.nodes):
        db_client.dataVKnodup.users.update_one(
            {'vk_id': node},
            {'$set': {
                'cluster': user_graph.nodes[node]['cluster'],
                'is_friend': user_graph.nodes[node]['is_friend'],
                'degree_centrality':
                    user_graph.nodes[node]['degree_centrality'],
                'eigenvector_centrality':
                    user_graph.nodes[node]['eigenvector_centrality'],
                'clustering_coefficient':
                    user_graph.nodes[node]['clustering_coefficient'],
                'avg_pos_sent': user_graph.nodes[node]['avg_pos_sent'],
                'avg_neg_sent': user_graph.nodes[node]['avg_neg_sent'],
                'avg_sent': user_graph.nodes[node]['avg_sent'],
                'verified': user_graph.nodes[node]['verified']
            }}
        )

    end_time = datetime.now()
    print("End at: ", end_time)
