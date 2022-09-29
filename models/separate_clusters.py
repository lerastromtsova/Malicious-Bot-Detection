import networkx as nx
import pandas as pd
import json
import matplotlib.pyplot as plt
from tqdm import tqdm

# num_clusters = 5487
import pymongo
from dotenv import dotenv_values

num_clusters = 5487
total_users = 73253
clusters = range(0, num_clusters + 1)


try:
    with open('../outputs/cluster_characteristics.json', 'r') as f:
        characteristics = json.load(f)
except FileNotFoundError:
    config = dotenv_values("../.env")
    db_client = pymongo.MongoClient(f"mongodb+srv://"
                                    f"lerastromtsova:{config['MONGO_DB_PASSWORD']}"
                                    f"@cluster0.ubfnhtk.mongodb.net/"
                                    f"?retryWrites=true&w=majority",
                                    tls=True,
                                    tlsAllowInvalidCertificates=True)

    avg_ages = list(db_client.dataVKnodup.users.aggregate([
        {
            '$group':
                {
                    '_id': "$cluster",
                    'avg_age': {'$avg': '$vk_age'}
                }
        }
    ]))

    graph = nx.read_gexf('../outputs/new_graph.gexf')
    characteristics = dict.fromkeys(clusters)

    for cluster in tqdm(clusters):
        characteristics[cluster] = {}
        nodes = [
            n for n in graph.nodes if graph.nodes[n]['cluster'] == cluster
        ]
        len_nodes = len(nodes)
        characteristics[cluster]['num_users'] = len_nodes
        verified = [
            graph.nodes[node]['verified']
            for node in nodes
        ]
        characteristics[cluster]['verified_ratio'] = sum(verified) / len_nodes
        is_friend = [
            graph.nodes[node]['is_friend']
            for node in nodes
        ]
        characteristics[cluster]['is_friend_ratio'] = \
            sum(is_friend) / len_nodes
        banned = [
            1 if graph.nodes[node]['cluster'] == cluster
            and graph.nodes[node]['deactivated'] == 'banned'
            else 0 for node in graph.nodes
        ]
        characteristics[cluster]['banned_ratio'] = sum(banned) / len_nodes

        characteristics[cluster]['avg_age'] = [c['avg_age'] for c in avg_ages if c['_id'] == cluster][0]

    with open('../outputs/cluster_characteristics.json', 'w') as f:
        json.dump(characteristics, f)

with open('../outputs/bots_in_clusters.json', 'r') as f:
    bots_in_clusters = json.load(f)

x = list(clusters)
y1 = [
    i['verified_ratio'] for i in list(
        characteristics.values()
    )[:num_clusters + 1]
]
y2 = [
    i['is_friend_ratio'] for i in list(
        characteristics.values()
    )[:num_clusters + 1]
]
y3 = [
    i['banned_ratio'] for i in list(
        characteristics.values()
    )[:num_clusters + 1]
]
y4 = [
    i['ratio'] for i in list(
        bots_in_clusters.values()
    )[:num_clusters + 1]
]
y5 = [
    i['avg_age'] for i in list(
        characteristics.values()
    )[:num_clusters + 1]
]
df = pd.DataFrame({
    'cluster': x,
    'verified_ratio': y1,
    'is_friend_ratio': y2,
    'banned_ratio': y3,
    'gosvon_ratio': y4,
    'avg_age': y5
})
df.plot(
    x="cluster",
    y=["is_friend_ratio"], kind="line",
    color=['blue']
)
plt.show()

df.plot(
    x="cluster",
    y=['banned_ratio'], kind="line",
    color=['red']
)
plt.show()

df.plot(
    x="cluster",
    y=["verified_ratio"], kind="line",
    color=['green']
)
plt.show()

df.plot(
    x="cluster",
    y=["gosvon_ratio"], kind="line",
    color=['red']
)
plt.show()

df.plot(
    x="cluster",
    y=["avg_age"], kind="line",
    color=['red']
)
plt.show()

possible_bot_clusters = []
possible_human_clusters = []

step = 0.01
threshold_for_banned = 0.0
threshold_for_friends = 0.0025
threshold_for_verified = 0.0005
threshold_for_bots = 0.0

for cluster, chars in characteristics.items():
    if chars['banned_ratio'] > threshold_for_banned \
            and chars['verified_ratio'] <= threshold_for_verified \
            and chars['is_friend_ratio'] <= threshold_for_friends:
        possible_bot_clusters.append(cluster)
    else:
        possible_human_clusters.append(cluster)

possible_human_users = sum(characteristics[c]['num_users']
                           for c in possible_human_clusters)
possible_bot_users = sum(characteristics[c]['num_users']
                         for c in possible_bot_clusters)

print(
    "Human: ",
    possible_human_users / total_users,
    len(possible_human_clusters),
    possible_human_clusters
)
print(
    "Bot: ",
    possible_bot_users / total_users,
    len(possible_bot_clusters),
    possible_bot_clusters
)

print(df.corr())
