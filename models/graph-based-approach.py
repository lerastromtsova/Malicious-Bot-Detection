"""
Steps:
1. Collect user features:
    - FOAF XML ya:created
    - FOAF XML ya:created timezone
    - FOAF XML ya:subscribersCount
    - FOAF XML ya:subscribedToCount
    - comment rate: number of all comments by user in db
    - deactivated - not used as a feature but used in
      cluster analysis later on. Already present in the db.
2. Write user similarity function. Different for:
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


def enrich_users_data(
        db_client: pymongo.MongoClient
) -> None:
    users = db_client.dataVKnodup.users.find({'enriched': {'$ne': True}})
    for user in users:
        foaf = get_foaf_data(user['vk_id'])
        activity = get_activity_count(user['vk_id'], db_client)
        db_client.dataVKnodup.users.update_one(
            {'_id': user['_id']},
            {'$set': {
                'created_at': foaf['created_at'],
                'timezone': foaf['timezone'],
                'followee_rate': foaf['followee_rate'],
                'follower_rate': foaf['follower_rate'],
                'follower_to_followee': foaf['follower_to_followee'],
                'comment_rate': activity,
                'enriched': True
            }}
        )
