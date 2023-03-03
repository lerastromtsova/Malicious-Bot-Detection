import pymongo
from dotenv import dotenv_values

config = dotenv_values("../.env")
db_client = pymongo.MongoClient('mongodb+srv://' +
                                f'{config["MONGO_DB_USERNAME"]}:' +
                                f'{config["MONGO_DB_PASSWORD"]}' +
                                f'@{config["MONGO_DB_HOST"]}' +
                                f'?tls=true&authSource=admin&replicaSet='
                                f'{config["MONGO_REPLICA_SET"]}'
                                f'&tlsInsecure=true')

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


def get_summarised_label(users):
    for user in users:
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

