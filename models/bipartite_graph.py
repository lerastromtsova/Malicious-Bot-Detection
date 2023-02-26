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


def get_nodes_edges_from_feature(feature_array):
    user_nodes = set()
    feature_nodes = set()
    user_to_feature = set()
    for from_id, comment_array in feature_array.items():
        if len(comment_array) > 0:
            user_nodes.add(from_id)
            features_of_user = []
            for comment in comment_array:
                for comment_id, feature_values in comment.items():
                    for fv in feature_values:
                        feature_nodes.add(fv)
                        features_of_user.append(fv)
            user_to_feature.update({(from_id, _, features_of_user.count(_)) for _ in features_of_user})
    return user_nodes, feature_nodes, user_to_feature


# FOR URL SHARING
# with open("urls.json", "r") as f:
#     urls = json.loads(f.read())
#
# final_graph = get_final_graph(*get_nodes_edges_from_feature(urls))
# nx.write_gexf(final_graph, '../outputs/bipartire_url_sharing.gexf')

# FOR HASHTAG SEQUENCES
with open("hashtags.json", "r") as f:
    hashtags = json.loads(f.read())

final_graph = get_final_graph(*get_nodes_edges_from_feature(hashtags))
nx.write_gexf(final_graph, '../outputs/bipartire_hashtags_sequences.gexf')