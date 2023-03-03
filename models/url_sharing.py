import re
import json
import networkx as nx

# from models.common import get_jaccard_similarity, get_weighted_edge
# from time import sleep
# from tqdm import tqdm
# from io import BytesIO
# from PIL import Image
# import requests
# import pymongo
# from dotenv import dotenv_values


def retrieve_urls(db_client):
    url_pattern = "https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\." \
                  "[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9(" \
                  ")@:%_\\+.~#?&\\/=]*)$"
    regx = re.compile(url_pattern, re.IGNORECASE)
    comments_with_urls = list(
        db_client.dataVKnodup.comments.find({'text': regx})
    )
    ks = {c['from_id'] for c in comments_with_urls}
    all_urls = dict(zip(ks, ([] for _ in ks)))

    for comment in comments_with_urls:
        splitted_text = comment['text'].split(' ')
        us = []
        for it in splitted_text:
            url = re.match(url_pattern, it)
            if url:
                us.append(url.group())
        if len(us) > 0:
            all_urls[comment['from_id']].append({comment['vk_id']: us})
    return all_urls


# config = dotenv_values("../.env")
# db_client = pymongo.MongoClient('mongodb+srv://' +
#                                 f'{config["MONGO_DB_USERNAME"]}:' +
#                                 f'{config["MONGO_DB_PASSWORD"]}' +
#                                 f'@{config["MONGO_DB_HOST"]}' +
#                                 f'?tls=true&authSource=admin&replicaSet={config["MONGO_REPLICA_SET"]}&tlsInsecure=true')
#
# with open("urls.json", "w") as f:
#     json.dump(retrieve_urls(db_client), f)

with open("urls.json", "r") as f:
    urls = json.loads(f.read())
#
# # 1. Create histograms for each image
#
#
# def get_image_histograms(images):
#     error_count = 0
#     success_count = 0
#     histograms = []
#
#     for user, comments in tqdm(images.items()):
#         for comment in comments:
#             for val in comment.values():
#                 for image in val:
#                     sleep(1)
#                     try:
#                         resp = requests.get(image, stream=True)
#                         img = Image.open(BytesIO(resp.content))
#                     except Exception as e:
#                         error_count += 1
#                         continue
#                     histogram = img.histogram()
#                     histograms.append((user, histogram))
#                     success_count += 1
#         print(error_count, success_count)
#     with open('histograms.txt', 'w') as f:
#         json.dump(histograms, f)
#

# 2. Build user graph based on image similarity

# G = nx.Graph()
# G.add_nodes_from([k for k in urls.keys()])
# users = list(urls.keys())
# edges = []
# for u1 in range(len(users)):
#     for u2 in range(u1+1, len(users)):
#         edge = get_weighted_edge(users[u1], users[u2], urls)
#         if edge[2] != 0:
#             edges.append(edge)
#
# G.add_weighted_edges_from(edges)
# G.remove_nodes_from(list(nx.isolates(G)))
# Gcc = sorted(nx.connected_components(G), key=len, reverse=True)
# print(Gcc)
#
# nx.write_gexf(G, '../outputs/url_sharing.gexf')


G = nx.read_gexf('../outputs/url_sharing.gexf')
print(list([int(n) for n in G.nodes]))
Gcc = sorted(nx.connected_components(G), key=len, reverse=True)
component_size_limit = 5
largest_components = [el for el in Gcc if len(el) >= component_size_limit]
print(largest_components)
