import json

import networkx as nx
from tqdm import tqdm


def get_final_graph(nodes_1, nodes_2, edges, threshold=0.0):
    print('getting projection')
    g = nx.Graph()
    g.add_nodes_from(nodes_1, bipartite=0)
    print('added 1st set of nodes')
    g.add_nodes_from(nodes_2, bipartite=1)
    print('added 2nd set of nodes')
    g.add_weighted_edges_from(edges)
    print('added edges')
    g_projected = nx.bipartite.overlap_weighted_projected_graph(g, nodes_1, jaccard=True)
    print('filtering edges and nodes')
    g_projected.remove_edges_from((n1, n2) for n1, n2, w in g_projected.edges(data="weight") if w < threshold)
    g_projected.remove_nodes_from(list(nx.isolates(g_projected)))
    return g_projected


def get_nodes_edges_from_feature(feature_array):
    print('getting nodes and edges')
    user_nodes = set()
    feature_nodes = set()
    user_to_feature = set()
    for from_id, comment_array in tqdm(feature_array.items()):
        if len(comment_array) > 0:
            user_nodes.add(from_id)
            features_of_user = []
            for comment in comment_array:
                for comment_id, feature_values in comment.items():
                    for fv in feature_values:
                        feature_nodes.add(fv)
                        features_of_user.append(fv)
            user_to_feature.update({(from_id, _, features_of_user.count(_)) for _ in features_of_user})
    print('created nodes and edges')
    return user_nodes, feature_nodes, user_to_feature


# FOR URL SHARING
# with open("urls.json", "r") as f:
#     urls = json.loads(f.read())
#
# final_graph = get_final_graph(*get_nodes_edges_from_feature(urls))
# nx.write_gexf(final_graph, '../outputs/bipartire_url_sharing.gexf')

# FOR HASHTAG SEQUENCES
# with open("hashtags.json", "r") as f:
#     hashtags = json.loads(f.read())
#
# final_graph = get_final_graph(*get_nodes_edges_from_feature(hashtags))
# nx.write_gexf(final_graph, '../outputs/bipartire_hashtags_sequences.gexf')

# FOR SYNCHRONISED ACTION
print('getting comments')
with open("binned_comments.json", "r") as f:
    comments = json.loads(f.read())

filtered_comments = {}
for i, c in comments.items():
    if len(c) >= 3:
        filtered_comments[i] = c
print('filtered comments')

final_graph = get_final_graph(*get_nodes_edges_from_feature(filtered_comments), threshold=0.9)
print('writing graph to file')
nx.write_gexf(final_graph, '../outputs/bipartite_synchronised_action.gexf')
