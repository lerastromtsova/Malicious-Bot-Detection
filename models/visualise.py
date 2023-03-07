import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import pymongo
from dotenv import dotenv_values

from models import get_centrality_metrics, get_average_sentiment

config = dotenv_values("../.env")
db_client = pymongo.MongoClient('mongodb+srv://' +
                                f'{config["MONGO_DB_USERNAME"]}:' +
                                f'{config["MONGO_DB_PASSWORD"]}' +
                                f'@{config["MONGO_DB_HOST"]}' +
                                f'?tls=true&authSource=admin&replicaSet='
                                f'{config["MONGO_REPLICA_SET"]}'
                                f'&tlsInsecure=true')


graph = nx.read_gexf('../outputs/bipartire_url_sharing.gexf')
graph = get_centrality_metrics(graph)
graph = get_average_sentiment(graph, db_client)
nodes_all = []
for i, node in graph.nodes(data=True):
    nodes_all.append({
        'id': i,
        'degree_centrality': node['degree_centrality'],
        'clustering_coefficient': node['clustering_coefficient'],
        'avg_neg_sent': node['avg_neg_sent'],
        'avg_pos_sent': node['avg_pos_sent'],
        'avg_sent': node['avg_sent']
    })

to_keep = list(n for n, d in graph.degree() if d <= 5)
filtered = graph.subgraph(to_keep)
nodes_filtered = []

for i, node in filtered.nodes(data=True):
    nodes_filtered.append({
        'id': i,
        'degree_centrality': node['degree_centrality'],
        'clustering_coefficient': node['clustering_coefficient'],
        'avg_neg_sent': node['avg_neg_sent'],
        'avg_pos_sent': node['avg_pos_sent'],
        'avg_sent': node['avg_sent']
    })

df = pd.DataFrame(nodes_all)

df.boxplot(column=[
    'degree_centrality',
    # 'eigenvector_centrality',
    'clustering_coefficient'
])
plt.figure()
plt.title('Full centrality metrics distribution')
plt.savefig('../outputs/full-centrality.pdf')

plt.figure()
df_filtered = pd.DataFrame(nodes_filtered)
df_filtered.boxplot(column=[
    'degree_centrality',
    # 'eigenvector_centrality',
    'clustering_coefficient'
])
plt.title('Filtered centrality metrics distribution')
plt.savefig('../outputs/filtered-centrality.pdf')
#
# df.boxplot(column=[
#     'avg_neg_sent',
#     'avg_pos_sent',
#     'avg_sent'
# ])
# plt.title('Full sentiments distribution')
# plt.show()
#
# df_filtered.boxplot(column=[
#     'avg_neg_sent',
#     'avg_pos_sent',
#     'avg_sent'
# ])
# plt.title('Filtered sentiments distribution')
# plt.show()

df.describe().to_csv('../outputs/full_characteristics.csv')
df_filtered.describe().to_csv('../outputs/filtered_characteristics.csv')
