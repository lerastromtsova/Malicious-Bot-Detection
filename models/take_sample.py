import pymongo
from dotenv import dotenv_values
import pandas as pd


config = dotenv_values("../.env")
db_client = pymongo.MongoClient(f"mongodb+srv://"
                                f"lerastromtsova:{config['MONGO_DB_PASSWORD']}"
                                f"@cluster0.ubfnhtk.mongodb.net/"
                                f"?retryWrites=true&w=majority",
                                tls=True,
                                tlsAllowInvalidCertificates=True)

sample = db_client.dataVKnodup.users.aggregate(
    [
        {'$match': {'cluster': {'$exists': 1}}},
        {'$sample': {'size': 100}}
    ]
)

info = [(u['vk_id'], u['cluster']) for u in sample]
df = pd.DataFrame(
    {'vk_id': [i[0] for i in info], 'cluster': [i[1] for i in info]}
)
df.to_csv('../outputs/sample.csv')
