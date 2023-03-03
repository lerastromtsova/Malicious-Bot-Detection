import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import pymongo
from dotenv import dotenv_values

graph = nx.read_gexf('../outputs/new_graph.gexf')
nodes = graph.nodes(data=True)
nodes_array = []
config = dotenv_values("../.env")
db_client = pymongo.MongoClient(f"mongodb+srv://"
                                f"lerastromtsova:{config['MONGO_DB_PASSWORD']}"
                                f"@cluster0.ubfnhtk.mongodb.net/"
                                f"?retryWrites=true&w=majority",
                                tls=True,
                                tlsAllowInvalidCertificates=True)

bot_users = list(db_client.dataVKnodup.users.find(
    {'cluster': {'$exists': 1}, 'gosvon_bot': 1}, {'_id': 0, 'vk_id': 1}
))
vk_ids = [_['vk_id'] for _ in bot_users]

for i, value in nodes:
    if i in vk_ids:
        nodes_array.append({'id': i, 'gosvon_bot': 1, **value})
    else:
        nodes_array.append({'id': i, 'gosvon_bot': 0, **value})

df = pd.DataFrame(nodes_array)
df.boxplot(column=[
    'degree_centrality',
    'eigenvector_centrality',
    'clustering_coefficient'
])
plt.title('Full centrality metrics distribution')
plt.show()

df_filtered = df[~df.cluster.isin([1, 3, 7, 24, 35, 158])]
df_filtered = df_filtered[df_filtered.gosvon_bot != 1]
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

df.describe().to_csv('../outputs/full_characteristics.csv')
df_filtered.describe().to_csv('../outputs/filtered_characteristics.csv')
