import networkx as nx
import pandas as pd
import json

# num_clusters = 5487
num_clusters = 5487
total_users = 73253
clusters = range(0, num_clusters + 1)

try:
    with open('../outputs/cluster_characteristics.json', 'r') as f:
        characteristics = json.load(f)
except FileNotFoundError:
    graph = nx.read_gexf('../outputs/new_graph.gexf')
    characteristics = dict.fromkeys(clusters)

    for cluster in clusters:
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

    with open('../outputs/cluster_characteristics.json', 'w') as f:
        json.dump(characteristics, f)

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
df = pd.DataFrame({
    'cluster': x,
    'verified_ratio': y1,
    'is_friend_ratio': y2,
    'banned_ratio': y3
})
# df.plot(
#     x="cluster",
#     y=["is_friend_ratio"], kind="line",
#     color=['blue']
# )
# plt.show()
#
# df.plot(
#     x="cluster",
#     y=['banned_ratio'], kind="line",
#     color=['red']
# )
# plt.show()
#
# df.plot(
#     x="cluster",
#     y=["verified_ratio"], kind="line",
#     color=['green']
# )
# plt.show()

possible_bot_clusters = []
possible_human_clusters = []

threshold_for_banned = 0
threshold_for_friends = 0.0025
threshold_for_verified = 0.0005

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
