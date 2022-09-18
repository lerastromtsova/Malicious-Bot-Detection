import json
import logging
import os
import re
import sys

import pymongo  # type: ignore
import vk  # type: ignore
from community import community_louvain
from dotenv import dotenv_values  # type: ignore
from tqdm import tqdm

from data_parser import get_friends_graph
import matplotlib.pyplot as plt
import networkx as nx
from datetime import datetime

from sentistrength import PySentiStr
from database_adapter import detect_languages
from models import get_clustered_graph, get_user_characteristics, get_centrality_metrics, get_clusters, \
    analyse_sentiment

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


def filter_node(n):
    g = G.nodes[n]
    return "cluster" in g


if __name__ == '__main__':
    # get_clusters(db_client, api)
    # start_time = datetime.now()
    # print('Started at: ', start_time)
    # Step 1: Cluster the users and write clusters to a file
    # get_clustered_graph(db_client, api)
    # Step 2: Get bots/real users/undefined users
    # get_user_characteristics(db_client)
    # Step 3: Calculate centrality metrics
    # cent_metrics = get_centrality_metrics()
    # Step 4: Translate the comment texts
    # while True:
    #     detect_languages(db_client)
    # print('Finished in: ', datetime.now() - start_time)
    # with open('outputs/graph_friends_enriched.json', 'r') as f:
    #     graph = json.load(f)
    # G = nx.node_link_graph(graph)
    # print(len(G.nodes))
    # subgraph = nx.subgraph_view(G, filter_node=filter_node)
    # print(len(subgraph.nodes))
    # final_view = subgraph.edge_subgraph(subgraph.edges())
    # print(len(final_view.nodes))
    # nx.write_gexf(final_view, 'outputs/subgraph.gexf')

    # Step 5: Analyse sentiments of comments
    comment_count = 0
    sample_size = 10000
    comment_max = db_client.dataVKnodup.comments.count_documents({'language': 'ru', 'sentiment': {'$exists': 0}})
    while comment_count < comment_max:
        comments = list(db_client.dataVKnodup.comments.aggregate([
            {'$match': {'language': 'ru', 'sentiment': {'$exists': 0}}},
            {'$sample': {'size': sample_size}}
        ]))
        for c in tqdm(comments):
            sentiment = analyse_sentiment(senti, c['text'])
            db_client.dataVKnodup.comments.update_one(
                {'vk_id': c['vk_id']},
                {'$set': {'sentiment': sentiment}}
            )
        comment_count += sample_size

