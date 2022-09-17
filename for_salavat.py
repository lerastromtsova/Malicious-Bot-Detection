import time

import pymongo
import vk
from tqdm import tqdm

db_client = pymongo.MongoClient(f"mongodb+srv://"
                                f"lerastromtsova: mongodb password"
                                f"@cluster0.ubfnhtk.mongodb.net/"
                                f"?retryWrites=true&w=majority",
                                tls=True,
                                tlsAllowInvalidCertificates=True)
api = vk.API(access_token='vk access token')


def get_friends_graph(
        users,
        api,
        db_client
):
    quantity_limit_error_code = 29
    for i in tqdm(range(0, len(users), 25)):
        error_count = 0
        time.sleep(0.3)
        user_ids = [str(u['vk_id']) for u in users[i:i+25] if 'friends' not in u or u['friends'] == quantity_limit_error_code]
        response = api.execute(
            code=f'var i = 0;'
                 f'var user;'
                 f'var users = [];'
                 f'var user_ids = {"[" + ",".join(user_ids) + "]"};'
                 f'while (i != 25) {{'
                 f'user = API.friends.get('
                 f'{{'
                 f'"user_id": (user_ids[i]), '
                 f'"v": "5.131", '
                 f'}}'
                 f'); '
                 f'i = i + 1;'
                 f'users.push(user);'
                 f'}};'
                 f'return users;',
            v="5.131"
        )
        response = list(response)
        for j in range(len(response)):
            if response[j]:
                db_client.dataVKnodup.users.update_one(
                    {'vk_id': int(user_ids[j])},
                    {'$set': {'friends': response[j]['items']}}
                )
            else:
                db_client.dataVKnodup.users.update_one(
                    {'vk_id': int(user_ids[j])},
                    {'$set': {'friends': 30}}
                )
                error_count += 1
        print(f"Percentage of errors {(error_count / 25)*100}%")


users = list(db_client.dataVKnodup.users.find({'friends': 29}))
get_friends_graph(users, api, db_client)
