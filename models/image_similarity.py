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

from models.common import get_weighted_edge


def retrieve_pictures(db_client):
    comments_with_pictures = list(
        db_client.dataVKnodup.comments.find({
            'text': {'$regex': "photo|img|image|jpg|png", '$options': 'i'}
        }))
    ks = {c['from_id'] for c in comments_with_pictures}
    pictures_urls = dict(zip(ks, ([] for _ in ks)))
    url_pattern = "^https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\" \
                  ".[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9(" \
                  ")@:%_\\+.~#?&\\/=]*)$"
    for comment in comments_with_pictures:
        splitted_text = comment['text'].split(' ')
        urls = []
        for it in splitted_text:
            url = re.match(url_pattern, it)
            if url:
                urls.append(url.group())
        if len(urls) > 0:
            pictures_urls[comment['from_id']].append({comment['vk_id']: urls})
    return pictures_urls


config = dotenv_values("../.env")
db_client = pymongo.MongoClient('mongodb+srv://' +
                                f'{config["MONGO_DB_USERNAME"]}:' +
                                f'{config["MONGO_DB_PASSWORD"]}' +
                                f'@{config["MONGO_DB_HOST"]}' +
                                f'?tls=true&authSource=admin&replicaSet='
                                f'{config["MONGO_REPLICA_SET"]}'
                                f'&tlsInsecure=true')

with open("images.json", "w") as f:
    json.dump(retrieve_pictures(db_client), f)

with open("images.json", "r") as f:
    images = json.loads(f.read())


def get_image_histograms(images):
    error_count = 0
    success_count = 0
    histograms = []

    for user, comments in tqdm(images.items()):
        for comment in comments:
            for val in comment.values():
                for image in val:
                    sleep(1)
                    try:
                        resp = requests.get(image, stream=True)
                        img = Image.open(BytesIO(resp.content))
                    except Exception:
                        error_count += 1
                        continue
                    histogram = img.histogram()
                    histograms.append((user, histogram))
                    success_count += 1
        print(error_count, success_count)
    with open('histograms.txt', 'w') as f:
        json.dump(histograms, f)


G = nx.Graph()
G.add_nodes_from([k for k in images.keys()])
users = list(images.keys())
edges = []
for u1 in range(len(users)):
    for u2 in range(u1 + 1, len(users)):
        edge = get_weighted_edge(users[u1], users[u2], images)
        if edge[2] != 0:
            edges.append(edge)

G.add_weighted_edges_from(edges)
G.remove_nodes_from(list(nx.isolates(G)))
print([int(n) for n in G.nodes])
nx.write_gexf(G, '../outputs/image_similarity.gexf')
