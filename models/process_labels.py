import pymongo
from dotenv import dotenv_values
import networkx as nx
import pandas as pd

config = dotenv_values("../.env")
db_client = pymongo.MongoClient('mongodb+srv://' +
                                f'{config["MONGO_DB_USERNAME"]}:' +
                                f'{config["MONGO_DB_PASSWORD"]}' +
                                f'@{config["MONGO_DB_HOST"]}' +
                                f'?tls=true&authSource=admin&replicaSet='
                                f'{config["MONGO_REPLICA_SET"]}'
                                f'&tlsInsecure=true')


def get_summarised_label():
    labelled_users = list(db_client.dataVKnodup.users.aggregate(
        [
            {'$project':
                {
                    'vk_id': 1,
                    'labels': 1,
                    'labels_count': {'$size': {"$ifNull": ["$labels", []]}}
                }
            },
            {'$match': {"labels_count": {'$gt': 0}}}
        ])
    )
    for user in labelled_users:
        db_client.dataVKnodup.users.update_one(
            {'vk_id': user['vk_id']},
            {'$unset': {'labelling_result': 1}}
        )
        labels = user['labels']
        bot_counts = sum(map(lambda x: x['result'] == 'BOT', labels))
        human_counts = sum(map(lambda x: x['result'] == 'HUMAN', labels))
        final_result = 'UNKNOWN'
        if bot_counts > human_counts:
            final_result = 'BOT'
        elif bot_counts < human_counts:
            final_result = 'HUMAN'
        print(f'likely {final_result}: {user["vk_id"]}')
        db_client.dataVKnodup.users.update_one(
            {'vk_id': user['vk_id']},
            {'$set': {'labelling_result': final_result}}
        )


def check_labels_against_model(model_graph):
    labelled_users = db_client.dataVKnodup.users.find({'labelling_result': {'$exists': True}})
    labelled_user_ids = {str(u['vk_id']): u['labelling_result'] for u in labelled_users}
    common_users = {k: {'labelling_result': v} for k, v in labelled_user_ids.items() if k in model_graph.nodes}
    nx.set_node_attributes(model_graph, common_users)
    return model_graph


# g_url_sharing = nx.read_gexf('../outputs/bipartire_url_sharing.gexf')
# updated_g_url_sharing = check_labels_against_model(g_url_sharing)
# nx.write_gexf(updated_g_url_sharing, '../outputs/url_sharing_with_labels.gexf')
#
# g_hashtag_sequences = nx.read_gexf('../outputs/bipartire_hashtags_sequences.gexf')
# updated_g_hashtag_sequences = check_labels_against_model(g_hashtag_sequences)
# nx.write_gexf(g_hashtag_sequences, '../outputs/hashtag_sequences_with_labels.gexf')

# df = pd.read_csv('../outputs/full_graph.csv')
# Graphtype = nx.Graph()
# G = nx.from_pandas_edgelist(df, edge_attr='weight', create_using=Graphtype)
# nx.write_gexf(G, '../outputs/full_graph.gexf')
#
# g_friendship_relations = nx.read_gexf('../outputs/full_graph.gexf')
# updated_g_friendship_relations = check_labels_against_model(g_friendship_relations)
# nx.write_gexf(g_friendship_relations, '../outputs/friendship_relations_with_labels.gexf')

def compare_labels_with_louvain_model():
    bot_clusters_according_to_louvain = {1: [], 3: [], 7: [], 24: [], 35: [], 158: []}
    human_clusters_according_to_louvain = dict()
    labelled_users = list(db_client.dataVKnodup.users.find({'labelling_result': {'$exists': True}}))
    for user in labelled_users:
        if 'cluster' in user:
            cluster = int(user['cluster'])
            if cluster in bot_clusters_according_to_louvain.keys():
                bot_clusters_according_to_louvain[cluster].append(user['labelling_result'])
            else:
                if cluster in human_clusters_according_to_louvain.keys():
                    human_clusters_according_to_louvain[cluster].append(user['labelling_result'])
                else:
                    human_clusters_according_to_louvain[cluster] = []
    print(bot_clusters_according_to_louvain)
    print(human_clusters_according_to_louvain)

