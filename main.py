import json
import logging
import os
import sys
import time

import pymongo  # type: ignore
import vk  # type: ignore
from dotenv import dotenv_values  # type: ignore

from data_parser import parse_comment_data, get_friends_of_friends
from database_adapter import write_comment_to_db

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
    # friends_of_friends = get_friends_of_friends(db_client, api)
    # with open('friends.json', 'r') as f:
    #     friends_of_friends = json.load(f)
    users = db_client.dataVKnodup.users.find({'verified': {'$exists': 0}}, {'vk_id': 1, '_id': 0})
    # users_set = set(u['vk_id'] for u in users)
    # my_friends = set([int(f) for f in friends_of_friends.keys()])
    # flattened_friends = [element for sublist in friends_of_friends.values() if sublist for element in sublist]
    # other_friends = set(flattened_friends)
    # my_friends_in_db = users_set.intersection(my_friends)
    # other_friends_in_db = users_set.intersection(other_friends)
    # # 3 {35738768, 1985833, 1058786}
    # print(len(my_friends_in_db), my_friends_in_db)
    # # 308
    # print(len(other_friends_in_db), other_friends_in_db)
    for user in users:
        time.sleep(0.3)
        try:
            user_info = api.users.get(
                user_id=user['vk_id'],
                fields='verified',
                v='5.131'
            )[0]
            if 'verified' in user_info:
                db_client.dataVKnodup.users.update_one(
                    {'vk_id': user['vk_id']},
                    {'$set': {'verified': user_info['verified']}}
                )
        except vk.exceptions.VkAPIError:
            pass
    db_client.close()
