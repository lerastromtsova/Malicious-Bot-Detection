import re
import json
from time import sleep
from tqdm import tqdm
import networkx as nx

from io import BytesIO
from PIL import Image
import requests

import pymongo
from dotenv import dotenv_values


def retrieve_hashtags(db_client):
    pattern = r"#(\w+)"
    regx = re.compile(pattern, re.IGNORECASE)
    comments_with_hashtags = list(db_client.dataVKnodup.comments.find({'text': regx}))
    ks = {c['from_id'] for c in comments_with_hashtags}
    all_hashtags = dict(zip(ks, ([] for _ in ks)))

    for comment in comments_with_hashtags:
        hashtags = re.findall(pattern, comment['text'])
        if len(hashtags) > 0:
            all_hashtags[comment['from_id']].append({comment['vk_id']: hashtags})
    return all_hashtags


config = dotenv_values("../.env")
db_client = pymongo.MongoClient('mongodb+srv://' +
                                f'{config["MONGO_DB_USERNAME"]}:' +
                                f'{config["MONGO_DB_PASSWORD"]}' +
                                f'@{config["MONGO_DB_HOST"]}' +
                                f'?tls=true&authSource=admin&replicaSet={config["MONGO_REPLICA_SET"]}&tlsInsecure=true')

with open("hashtags.json", "w") as f:
    json.dump(retrieve_hashtags(db_client), f)

with open("hashtags.json", "r") as f:
    hashtags = json.loads(f.read())

def get_jaccard_similarity(vec1, vec2):
    v1, v2 = set([_.lower() for _ in vec1]), set([_.lower() for _ in vec2])
    if v1 and v2:
        return len(v1.intersection(v2)) / len(v1.union(v2))
    return 0


def get_weighted_edge(user1, user2, hashtags):
    user1_vector = []
    user2_vector = []
    for comment in hashtags[user1]:
        for vk_id, hstg in comment.items():
            for i in hstg:
                user1_vector.append(i)
    for comment in hashtags[user2]:
        for vk_id, hstg in comment.items():
            for i in hstg:
                user2_vector.append(i)
    return user1, user2, get_jaccard_similarity(user1_vector, user2_vector)

# 1. Build user graph based on hashtag similarity

G = nx.Graph()
G.add_nodes_from([k for k in hashtags.keys()])
users = list(hashtags.keys())
edges = []
for u1 in range(len(users)):
    for u2 in range(u1+1, len(users)):
        edge = get_weighted_edge(users[u1], users[u2], hashtags)
        if edge[2] != 0:
            edges.append(edge)

G.add_weighted_edges_from(edges)
G.remove_nodes_from(list(nx.isolates(G)))
Gcc = sorted(nx.connected_components(G), key=len, reverse=True)
print(Gcc)

nx.write_gexf(G, '../outputs/hashtag_sequences.gexf')

G = nx.read_gexf('../outputs/hashtag_sequences.gexf')
print(list([int(n) for n in G.nodes]))
Gcc = sorted(nx.connected_components(G), key=len, reverse=True)
component_size_limit = 5
largest_components = [el for el in Gcc if len(el) >= component_size_limit]
print(largest_components)
