from dotenv import dotenv_values  # type: ignore
import logging
import vk  # type: ignore
import pymongo  # type: ignore
import os
import sys

from models.graph_based_approach import enrich_users_data

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
    # enrich_users_data(db_client)
    agg = list(db_client.dataVKnodup.comments.aggregate([
        {'$group': {'_id': "$from_id",
                    'count': {'$sum': 1}
                    }
         }
    ]))
    users = list(db_client.dataVKnodup.users.find({'enriched': {'$ne': True},
                                                   'comment_rate': {'$exists': False}}))
    for user in users:
        for a in agg:
            if user['vk_id'] == a['_id']:
                db_client.dataVKnodup.users.update_one(
                    {'_id': user['vk_id']},
                    {'$set': {
                        'comment_rate': a['count']
                    }}
                )

