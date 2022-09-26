import json
import logging
import os
import sys

import networkx as nx
import pymongo
import vk
from dotenv import dotenv_values

from models import get_clustered_graph, get_user_characteristics
from models import get_centrality_metrics, get_clusters
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


def filter_node(n):
    g = G.nodes[n]
    return "cluster" in g


if __name__ == '__main__':

    # logging.info('Step 1: Cluster the users and write clusters to a file')
    # get_clusters(db_client, api)
    # get_clustered_graph(db_client, api)

    # logging.info('Step 2: Get bots/real users/undefined users')
    # get_user_characteristics(db_client)

    logging.info('Step 3: Calculate centrality metrics')
    cent_metrics = get_centrality_metrics()

    with open('outputs/graph_with_centrality.json', 'r') as f:
        graph = json.load(f)
    G = nx.node_link_graph(graph)

    logging.info("Write to a Gephi file")
    nx.write_gexf(G, 'outputs/graph.gexf')
