import json
import logging
import os
import sys

import pymongo  # type: ignore
import vk  # type: ignore
from community import community_louvain
from dotenv import dotenv_values  # type: ignore

from data_parser import get_friends_graph
import matplotlib.pyplot as plt
import networkx as nx
from datetime import datetime

from models import get_clustered_graph, get_user_characteristics, get_centrality_metrics

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

if __name__ == '__main__':
    start_time = datetime.now()
    print('Started at: ', start_time)
    # Step 1: Cluster the users and write clusters to a file
    # get_clustered_graph(db_client, api)
    # Step 2: Get bots/real users/undefined users
    # get_user_characteristics(db_client)
    # Step 3: Calculate centrality metrics
    cent_metrics = get_centrality_metrics()
    print('Finished in: ', datetime.now() - start_time)