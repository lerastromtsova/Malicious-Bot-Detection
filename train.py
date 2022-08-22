import pymongo
from dotenv import dotenv_values

from models.graph_based_approach import MarkovClusteringModel


config = dotenv_values(".env")
db_client = pymongo.MongoClient(f"mongodb+srv://"
                                f"lerastromtsova:{config['MONGO_DB_PASSWORD']}"
                                f"@cluster0.ubfnhtk.mongodb.net/"
                                f"?retryWrites=true&w=majority",
                                tls=True,
                                tlsAllowInvalidCertificates=True)
users = db_client.dataVKnodup.users.find({'enriched': True, 'deactivated': {'$ne': 'deleted'}})
model = MarkovClusteringModel(list(users))
similarities = [i/100 for i in range(30, 100, 5)]
inflations = [i/10 for i in range(11, 51)]
model.train(similarities, inflations)
model.draw_graph()
model.save('model_outputs/graph_based_approach.json')
