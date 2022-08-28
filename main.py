from dotenv import dotenv_values  # type: ignore
import logging
import vk  # type: ignore
import pymongo  # type: ignore
import os
import sys

from database_adapter import populate_similarities
from models.graph_based_approach import MarkovClusteringModel

config = dotenv_values(".env")
if not config:
    config = os.environ

logging.basicConfig(
    filename='log/data_parsing.log',
    encoding='utf-8',
    level=getattr(logging, config['LOG_LEVEL'].upper())
)
# api = vk.API(access_token=config[sys.argv[1]])
db_client = pymongo.MongoClient(f"mongodb+srv://"
                                f"lerastromtsova:{config['MONGO_DB_PASSWORD']}"
                                f"@cluster0.ubfnhtk.mongodb.net/"
                                f"?retryWrites=true&w=majority",
                                tls=True,
                                tlsAllowInvalidCertificates=True)

if __name__ == '__main__':
    # TRAINING
    # users = list(db_client.dataVKnodup.users.find({}).limit(10**2))
    # model = MarkovClusteringModel(users)
    # sim_thresholds = [i/100 for i in range(20, 100, 5)]
    # inflation_rates = [i/10 for i in range(11, 51)]
    # model.train(sim_thresholds, inflation_rates)
    # model.save('models/model_outputs/graph_based_approach.json')

    # SIMILARITIES
    populate_similarities(db_client)

    # GETTING CLUSTERS
    # users = list(db_client.dataVKnodup.users.find({}))
    # model = MarkovClusteringModel(users)
    # model.read_from_saved('models/model_outputs/graph_based_approach.json')
    # clusters = model.get_clusters()
    # model.save('models/model_outputs/graph_based_approach_all_clusters.json')

