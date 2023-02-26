import json

import networkx as nx


def get_final_graph(nodes_1, nodes_2, edges):
    g = nx.Graph()
    g.add_nodes_from(nodes_1, bipartite=0)
    g.add_nodes_from(nodes_2, bipartite=1)
    g.add_weighted_edges_from(edges)
    g_projected = nx.bipartite.overlap_weighted_projected_graph(g, nodes_1, jaccard=True)
    g_projected.remove_nodes_from(list(nx.isolates(g_projected)))
    return g_projected


def get_nodes_edges_from_urls(urls):
    user_nodes = set()
    url_nodes = set()
    user_to_url = set()
    for from_id, comment_array in urls.items():
        if len(comment_array) > 0:
            user_nodes.add(from_id)
            urls_of_user = []
            for comment in comment_array:
                for comment_id, url_values in comment.items():
                    for u in url_values:
                        url_nodes.add(u)
                        urls_of_user.append(u)
            user_to_url.update({(from_id, _, urls_of_user.count(_)) for _ in urls_of_user})
    return user_nodes, url_nodes, user_to_url


def get_nodes_edges_from_hashtags(hashtags):
    user_nodes = set()
    hashtag_nodes = set()
    user_to_hashtag = set()
    for from_id, comment_array in hashtags.items():
        if len(comment_array) > 0:
            user_nodes.add(from_id)
            hashtags_of_user = []
            for comment in comment_array:
                for comment_id, hashtag_values in comment.items():
                    for h in hashtag_values:
                        hashtag_nodes.add(h)
                        hashtags_of_user.append(h)
            user_to_hashtag.update({(from_id, _, hashtags_of_user.count(_)) for _ in hashtags_of_user})
    return user_nodes, hashtag_nodes, user_to_hashtag


# FOR URL SHARING
# with open("urls.json", "r") as f:
#     urls = json.loads(f.read())
#
# final_graph = get_final_graph(*get_nodes_edges_from_urls(urls))
# nx.write_gexf(final_graph, '../outputs/bipartire_url_sharing.gexf')

# FOR HASHTAG SEQUENCES
with open("hashtags.json", "r") as f:
    hashtags = json.loads(f.read())

final_graph = get_final_graph(*get_nodes_edges_from_hashtags(hashtags))
nx.write_gexf(final_graph, '../outputs/bipartire_hashtags_sequences.gexf')