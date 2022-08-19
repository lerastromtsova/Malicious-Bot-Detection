from dotenv import dotenv_values  # type: ignore
import logging
import vk  # type: ignore
import pymongo  # type: ignore
import os
import multiprocessing as mp

from data_parser import parse_comment_data
from database_adapter import write_comment_to_db
from models.graph_based_approach import enrich_users_data, get_similarity

config = dotenv_values(".env")
if not config:
    config = os.environ

logging.basicConfig(
    filename='log/data_parsing.log',
    encoding='utf-8',
    level=getattr(logging, config['LOG_LEVEL'].upper())
)
api = vk.API(access_token=config['VK_API_TOKEN'])
db_client = pymongo.MongoClient(f"mongodb+srv://"
                                f"lerastromtsova:{config['MONGO_DB_PASSWORD']}"
                                f"@cluster0.ubfnhtk.mongodb.net/"
                                f"?retryWrites=true&w=majority",
                                tls=True,
                                tlsAllowInvalidCertificates=True)

if __name__ == '__main__':
    # pool = mp.Pool(mp.cpu_count())
    # insert_comment_ids(db_client, api)
    # for comment in parse_comment_data(db_client, api):
    # write_comment_to_db(comment, db_client)
    # pool.apply_async(write_comment_to_db, args=(comment, db_client))
    # pool.close()
    #     The idea for testing the models is to test on a small sample (e.g. 10) of comment/user ids
    # Then, after collecting all the data, we can run the models remotely on a server on all the dataset
    # enrich_users_data(db_client)
    users = db_client.dataVKnodup.users.find({'enriched': True})
    for user1 in users:
        for user2 in users:
            if user1 != user2:
                print(get_similarity((user1, user2)))
