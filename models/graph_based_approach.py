"""
Steps:
1. Collect user features - DONE:
    - FOAF XML ya:created
    - FOAF XML ya:created timezone
    - FOAF XML ya:subscribersCount
    - FOAF XML ya:subscribedToCount
    - comment rate: number of all comments by user in db
    - deactivated - not used as a feature but used in
      cluster analysis later on. Already present in the db.
2. Write user similarity function - DONE
   Different for:
    - nominal data type
    - real data type
3. Construct multi-attributed graph Gm
4. Convert to similarity graph Gs using (2)
5. Construct similarity matrix from Gs
6. Apply Markov clustering to the matrix:
    - expansion
    - inflation
7. Analyse each cluster one by one
"""
import pymongo  # type: ignore
from data_parser import get_foaf_data, get_activity_count
from typing import Tuple
from datetime import datetime


def enrich_users_data(
        db_client: pymongo.MongoClient,
) -> None:
    users = db_client.dataVKnodup.users.find({'enriched': {'$ne': True}})
    for i, user in enumerate(users):
        foaf = get_foaf_data(user['vk_id'])
        activity = get_activity_count(user['vk_id'], db_client)
        if foaf['created_at']:
            vk_age = (datetime(2022, 8, 1, 0, 0, 0) - foaf['created_at']).days
        else:
            vk_age = None
        db_client.dataVKnodup.users.update_one(
            {'_id': user['_id']},
            {'$set': {
                'created_at': foaf['created_at'],
                'vk_age': vk_age,
                'timezone': foaf['timezone'],
                'followee_rate': foaf['followee_rate'],
                'follower_rate': foaf['follower_rate'],
                'follower_to_followee': foaf['follower_to_followee'],
                'comment_rate': activity,
                'enriched': True
            }}
        )


def get_similarity(
        users: Tuple
) -> float:
    features = {
        'vk_age': 'real',
        'timezone': 'nominal',
        'followee_rate': 'real',
        'follower_rate': 'real',
        'follower_to_followee': 'real',
        'comment_rate': 'real'
    }
    similarities = []
    for feature, typ in features.items():
        if users[0][feature] and users[1][feature]:
            if typ == 'real':
                similarities.append(get_real_similarity((users[0][feature], users[1][feature])))
            elif typ == 'nominal':
                similarities.append(get_nominal_similarity((users[0][feature], users[1][feature])))
    avg_similarity = sum(similarities) / len(similarities)
    return avg_similarity


def get_nominal_similarity(
        values: Tuple
) -> bool:
    return values[0] == values[1]


def get_real_similarity(
        values: Tuple
) -> float:
    return 1 / (1 + abs(values[0] - values[1]))
