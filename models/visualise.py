import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt

graph = nx.read_gexf('../outputs/new_graph.gexf')
nodes = graph.nodes(data=True)
nodes_array = []
for i, value in nodes:
    nodes_array.append({'id': i, **value})

df = pd.DataFrame(nodes_array)
df.boxplot(column=[
    'degree_centrality',
    'eigenvector_centrality',
    'clustering_coefficient'
])
plt.title('Full centrality metrics distribution')
plt.show()

df_filtered = df[~df.cluster.isin([1, 3, 7, 24, 35, 158])]
df_filtered.boxplot(column=[
    'degree_centrality',
    'eigenvector_centrality',
    'clustering_coefficient'
])
plt.title('Filtered centrality metrics distribution')
plt.show()

df.boxplot(column=[
    'avg_neg_sent',
    'avg_pos_sent',
    'avg_sent'
])
plt.title('Full sentiments distribution')
plt.show()

df_filtered.boxplot(column=[
    'avg_neg_sent',
    'avg_pos_sent',
    'avg_sent'
])
plt.title('Filtered sentiments distribution')
plt.show()
