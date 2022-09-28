import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt


graph = nx.read_gexf('../outputs/new_graph.gexf')
clusters = range(0, 40)
characteristics = dict.fromkeys(clusters)
for cluster in clusters:
    characteristics[cluster] = {}
    nodes = [n for n in graph.nodes if graph.nodes[n]['cluster'] == cluster]
    len_nodes = len(nodes)
    verified = [
        graph.nodes[node]['verified']
        for node in nodes
    ]
    characteristics[cluster]['verified_ratio'] = sum(verified) / len_nodes
    is_friend = [
        graph.nodes[node]['is_friend']
        for node in nodes
    ]
    characteristics[cluster]['is_friend_ratio'] = sum(is_friend) / len_nodes
    banned = [
        1 if graph.nodes[node]['cluster'] == cluster
        and graph.nodes[node]['deactivated'] == 'banned'
        else 0 for node in graph.nodes
    ]
    characteristics[cluster]['banned_ratio'] = sum(banned) / len_nodes

x = list(clusters)
y1 = [i['verified_ratio'] for i in characteristics.values()]
y2 = [i['is_friend_ratio'] for i in characteristics.values()]
y3 = [i['banned_ratio'] for i in characteristics.values()]
df = pd.DataFrame({
    'cluster': x,
    'verified_ratio': y1,
    'is_friend_ratio': y2,
    'banned_ratio': y3
})
df.plot(
    x="cluster",
    y=["verified_ratio", "is_friend_ratio", 'banned_ratio'], kind="bar")
plt.show()
