import json
import pymongo
from dotenv import dotenv_values

# import pandas as pd
# from tqdm import tqdm
# import matplotlib.pyplot as plt

# with open('../data/bots_from_gosvon.json', 'r') as f:
#     bots = json.load(f)[0]['items']
#
# bot_ids = set([int(u['id']) for u in bots])

config = dotenv_values("../.env")
db_client = pymongo.MongoClient(f"mongodb+srv://"
                                f"lerastromtsova:{config['MONGO_DB_PASSWORD']}"
                                f"@cluster0.ubfnhtk.mongodb.net/"
                                f"?retryWrites=true&w=majority",
                                tls=True,
                                tlsAllowInvalidCertificates=True)

users = db_client.dataVKnodup.users.find({'cluster': {'$exists': 1}})
# user_ids = set([u['vk_id'] for u in users])
# print(len(user_ids))
#
# common_ids = list(user_ids.intersection(bot_ids))
# print(common_ids)
# print(len(common_ids))
#
# db_client.dataVKnodup.users.update_many({
#   'vk_id': {'$in': common_ids}}, {'$set': {'gosvon_bot': 1}
# })
# db_client.dataVKnodup.users.update_many({
#   'vk_id': {'$nin': common_ids}}, {'$set': {'gosvon_bot': 0}
# })
#
# print(db_client.dataVKnodup.users.count_documents({'gosvon_bot': 1}))
# print(db_client.dataVKnodup.users.count_documents({'gosvon_bot': 0}))
# print(db_client.dataVKnodup.users.count_documents({
#   'gosvon_bot': {'$exists': 1}
# }))

# bots_in_clusters = dict.fromkeys(range(5488))
# for u in tqdm(users):
#     if bots_in_clusters[u['cluster']] is None:
#         bots_in_clusters[u['cluster']] = {'bots': 0, 'total': 0}
#     bots_in_clusters[u['cluster']]['total'] += 1
#     if u['gosvon_bot'] == 1:
#         bots_in_clusters[u['cluster']]['bots'] += 1
#
# for cluster in tqdm(bots_in_clusters.keys()):
#     bots_in_clusters[cluster]['ratio'] = bots_in_clusters[cluster]['bots'] \
#                                          / bots_in_clusters[cluster]['total']
#
# with open('../outputs/bots_in_clusters.json', 'w') as f:
#     json.dump(bots_in_clusters, f)
#

with open('../outputs/bots_in_clusters.json', 'r') as f:
    bots_in_clusters = json.load(f)

clusters = [k for k in bots_in_clusters.keys()
            if bots_in_clusters[k]['ratio'] >= 0.5]
print(clusters)
bot_ratio = [bots_in_clusters[k]['ratio'] for k in bots_in_clusters.keys()
             if bots_in_clusters[k]['ratio'] > 0]

# df = pd.DataFrame({'clusters': clusters, 'bot_ratio': bot_ratio})
# df.plot(column=bot_ratio, kind='line')
# plt.show()

our_clusters = {'1', '3', '7', '24', '35', '158'}
print(our_clusters.intersection(set(clusters)))
