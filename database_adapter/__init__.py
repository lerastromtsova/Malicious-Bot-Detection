import logging
import re
import time

import pymongo
from datetime import datetime
import os

# from dotenv import dotenv_values
from vk import API, exceptions
from langdetect import detect, lang_detect_exception


def write_comment_to_db(
        comment: dict,
        db_client: pymongo.MongoClient
) -> None:
    """
    A function that writes a comment to Mongo db.
    :param comment:
    {
        "items": [
            {
                "id": 5742639,
                "from_id": 476028004,
                "date": 1646080752,
                "text": "[id173699767|Елена], там все
                несколько сложнее. Если тупо печатать бумагу при
                нулевой ставке, будет гиперинфляция - как в РФ в
                начале 90ых.",
                "post_id": 5742449,
                "owner_id": -76982440,
                "parents_stack": [
                    5742516
                ],
                "likes": {
                    "can_like": 1,
                    "count": 2,
                    "user_likes": 0,
                    "can_publish": 1
                },
                "reply_to_user": 173699767,
                "reply_to_comment": 5742620
            }
        ],
        "profiles": [
            {
                "id": 476028004,
                "sex": 2,
                "screen_name": "xhdhdhdhdhdh",
                "photo_50": "https://vk.com/images/camera_50.png",
                "photo_100": "https://vk.com/images/camera_100.png",
                "online_info": {
                    "visible": true,
                    "last_seen": 1660068001,
                    "is_online": false,
                    "app_id": 2274003,
                    "is_mobile": true
                },
                "online": 0,
                "first_name": "Alexander",
                "last_name": "Smirnov",
                "can_access_closed": true,
                "is_closed": false
            }
        ],
        "groups": [],
        "can_post": true,
        "show_reply_button": true,
        "groups_can_post": true
    }
    :param db_client:
    :return:
    """
    db = db_client.dataVKnodup
    if 'invalid' in comment and comment['invalid']:
        db.comments.update_one(
            {'vk_id': comment['vk_id']},
            {'$set': comment}
        )
        return
    if comment['items']:
        for item in comment['items']:
            item['date'] = datetime.utcfromtimestamp(item['date'])
            item['vk_id'] = str(item['id'])
            del item['id']
            item['processed'] = True
            try:
                db.comments.update_one(
                    {'vk_id': item['vk_id']},
                    {'$set': item}
                )
            except pymongo.errors.DuplicateKeyError:
                logging.warning("Trying to insert duplicate key in comments")
    if comment['groups']:
        for group in comment['groups']:
            group['vk_id'] = group['id']
            del group['id']
        try:
            db.groups.insert_many(comment['groups'])
        except pymongo.errors.BulkWriteError:
            logging.warning("Trying to insert duplicate key in groups")
    if comment['profiles']:
        for profile in comment['profiles']:
            profile['vk_id'] = profile['id']
            del profile['id']
        try:
            db.users.insert_many(comment['profiles'])
        except pymongo.errors.BulkWriteError:
            logging.warning("Trying to insert duplicate key in users")


def check_num_of_collection(
        db_client: pymongo.MongoClient,
        collection: str
):
    """
    Checks the number of documents in a collection.
    :param db_client:
    :param collection:
    :return:
    """
    db = db_client.dataVKnodup
    if collection == 'users':
        return db.users.count_documents({})
    return db.comments.count_documents({})


def delete_duplicates(
        db_client: pymongo.MongoClient,
        collection: str
):
    """
    Deletes duplicates in a collection.
    :param db_client:
    :param collection:
    :return:
    """
    if collection == 'users':
        cursor = db_client.dataVKnodup.users.aggregate(
            [
                {"$group": {
                    "_id": "$vk_id",
                    "unique_ids": {"$addToSet": "$_id"},
                    "count": {"$sum": 1}
                }},
                {"$match": {"count": {"$gte": 2}}}
            ]
        )
        response = []
        for doc in cursor:
            del doc["unique_ids"][0]
            for i in doc["unique_ids"]:
                response.append(i)
        db_client.dataVKnodup.users.delete_many({"_id": {"$in": response}})
    elif collection == 'comments':
        cursor = db_client.dataVKnodup.comments.aggregate(
            [
                {"$group": {
                    "_id": "$vk_id",
                    "unique_ids": {"$addToSet": "$_id"},
                    "count": {"$sum": 1}
                }},
                {"$match": {"count": {"$gte": 2}}}
            ]
        )
        response = []
        for doc in cursor:
            del doc["unique_ids"][0]
            for i in doc["unique_ids"]:
                response.append(i)

        db_client.dataVKnodup.comments.delete_many({"_id": {"$in": response}})
    elif collection == 'groups':
        cursor = db_client.dataVKnodup.groups.aggregate(
            [
                {"$group": {
                    "_id": "$vk_id",
                    "unique_ids": {"$addToSet": "$_id"},
                    "count": {"$sum": 1}
                }},
                {"$match": {"count": {"$gte": 2}}}
            ]
        )
        response = []
        for doc in cursor:
            del doc["unique_ids"][0]
            for i in doc["unique_ids"]:
                response.append(i)

        db_client.dataVKnodup.groups.delete_many({"_id": {"$in": response}})


def insert_comment_ids(
        db_client: pymongo.MongoClient,
        api: API
) -> None:
    """
    Forms an initial comments collection populated by comment ids.
    :param db_client:
    :param api:
    :return:
    """
    for root, dirs, files in os.walk("./data"):
        if root not in [
            './data',
            './data/independent',
            './data/state-affiliated',
            './data/output'
        ]:
            media_name = root.split('/')[-1]
            media_id = api.utils.resolveScreenName(
                screen_name=media_name, v='5.131'
            )['object_id']
            for file in files:
                if file != '.DS_Store':
                    filepath = os.path.join(root, file)
                    with open(filepath, 'r') as f:
                        comment_ids = f.read().split('\n')
                        result = [{
                            "vk_id": i,
                            "media_name": media_name,
                            "media_id": media_id,
                            "processed": False
                        } for i in comment_ids]
                        try:
                            db_client.dataVKnodup.comments.insert_many(result)
                        except pymongo.errors.BulkWriteError:
                            logging.warning("Trying to insert duplicate key")


def get_writing_speed(
        db_client: pymongo.MongoClient,
        time_to_sleep: int = 10
) -> float:
    """
    A utility function to estimate the speed with
    which the comments colection is populated.
    :param db_client:
    :param time_to_sleep:
    :return: Number of comments processed per second.
    """
    start_count = db_client.dataVKnodup.comments.count_documents(
        {'processed': True}
    )
    time.sleep(time_to_sleep)
    end_count = db_client.dataVKnodup.comments.count_documents(
        {'processed': True}
    )
    return (end_count - start_count) / time_to_sleep


def get_user_by_id(
        db_client: pymongo.MongoClient,
        user_id: int
) -> list:
    """
    Retrieve a user by their VK ID.
    :param db_client:
    :param user_id:
    :return:
    """
    users = db_client.dataVKnodup.users.find({'vk_id': int(user_id)})
    return list(users)


def get_comments_by_user(
        db_client: pymongo.MongoClient,
        user_id: int
) -> list:
    """
    Get all the comments that a particular user left in the database.
    :param db_client:
    :param user_id:
    :return:
    """
    comments = list(db_client.dataVKnodup.comments.find(
        {'from_id': int(user_id)}
    ))
    for c in comments:
        split = re.split(r'\[*\], ', c['text'])
        if len(split) >= 2:
            c['text'] = ' '.join(split[1:])
    return comments


def get_users_by_name(
        db_client: pymongo.MongoClient,
        query: str,
        users_limit: int = 10
) -> list:
    """
    Search for users using their first or last name.
    :param db_client:
    :param query:
    :param users_limit:
    :return:
    """
    to_search = query.split()
    if len(to_search) == 2:
        users = list(db_client.dataVKnodup.users.find(
            {'first_name': {'$regex': to_search[0], '$options': 'i'},
             'last_name': {'$regex': to_search[1], '$options': 'i'},
             'cluster': {'$exists': 1}}
        ).limit(users_limit))
    else:
        users_by_lname = db_client.dataVKnodup.users.find(
            {'last_name': {
                '$regex': to_search[0],
                '$options': 'i'
            }, 'cluster': {'$exists': 1}}
        ).limit(users_limit)
        users_by_fname = db_client.dataVKnodup.users.find(
            {'first_name': {
                '$regex': to_search[0],
                '$options': 'i'
            }, 'cluster': {'$exists': 1}}
        ).limit(users_limit)
        users = list(users_by_lname) + list(users_by_fname)
    return users


def add_verified_users(
        db_client: pymongo.MongoClient,
        api: API
) -> None:
    """
    Add the 'verified' fiels to each user in the database.
    :param db_client:
    :param api:
    :return:
    """
    users = db_client.dataVKnodup.users.find({
        'verified': {'$exists': 0}},
        {'vk_id': 1, '_id': 0}
    )
    for user in users:
        time.sleep(0.3)
        try:
            user_info = api.users.get(
                user_id=user['vk_id'],
                fields='verified',
                v='5.131'
            )[0]
            if 'verified' in user_info:
                db_client.dataVKnodup.users.update_one(
                    {'vk_id': user['vk_id']},
                    {'$set': {'verified': user_info['verified']}}
                )
        except exceptions.VkAPIError:
            pass
    db_client.close()


def remove_emojis(
        data: str
) -> str:
    """
    Remove all emojis from a given string.
    :param data:
    :return:
    """
    emoj = re.compile("["
                      u"\U0001F600-\U0001F64F"  # emoticons
                      u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                      u"\U0001F680-\U0001F6FF"  # transport & map symbols
                      u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                      u"\U00002500-\U00002BEF"  # chinese char
                      u"\U00002702-\U000027B0"
                      u"\U00002702-\U000027B0"
                      u"\U000024C2-\U0001F251"
                      u"\U0001f926-\U0001f937"
                      u"\U00010000-\U0010ffff"
                      u"\u2640-\u2642"
                      u"\u2600-\u2B55"
                      u"\u200d"
                      u"\u23cf"
                      u"\u23e9"
                      u"\u231a"
                      u"\ufe0f"  # dingbats
                      u"\u3030"
                      "]+", re.UNICODE)
    return re.sub(emoj, '', data)


def detect_languages(
        db_client: pymongo.MongoClient
) -> None:
    """
    Mark all comments in the database with the language they are written in.
    :param db_client:
    :return:
    """
    comments = db_client.dataVKnodup.comments.aggregate([
        {'$match': {'language': {'$exists': 0}, 'text': {'$exists': 1}}},
        {'$sample': {'size': 10000}}
    ])
    for comment in comments:
        if comment['text']:
            text = remove_emojis(comment['text'])
            if text:
                # To remove "Replies"
                text = re.sub(r'\[.*[a-zA-Z]+.*\], ', '', comment['text'])
                if text:
                    try:
                        language = detect(text)
                        db_client.dataVKnodup.comments.update_one(
                            {"_id": comment['_id']},
                            {'$set': {'language': language}}
                        )
                    except lang_detect_exception.LangDetectException:
                        db_client.dataVKnodup.comments.update_one(
                            {"_id": comment['_id']},
                            {'$set': {'language': 'unknown'}}
                        )
                        pass
                else:
                    db_client.dataVKnodup.comments.update_one(
                        {"_id": comment['_id']},
                        {'$set': {'language': 'unknown'}}
                    )
            else:
                db_client.dataVKnodup.comments.update_one(
                    {"_id": comment['_id']},
                    {'$set': {'language': 'unknown'}}
                )
        else:
            db_client.dataVKnodup.comments.update_one(
                {"_id": comment['_id']},
                {'$set': {'language': 'unknown'}}
            )


def generate_database_sample(
        db_client: pymongo.MongoClient,
        sample_size: int,
        comment_limit: int
):
    db_client.dataVKnodup.users.update_many(
        {},
        {"$set": {"user_to_label": False}}
    )
    users = db_client.dataVKnodup.users.aggregate([
        {'$match': {'comment_rate': {'$lte': comment_limit}}},
        {'$sample': {'size': sample_size}}
    ])
    for u in users:
        db_client.dataVKnodup.users.update_one(
            {"_id": u['_id']},
            {"$set": {"user_to_label": True}}
        )


# # To generate a new random sample
# config = dotenv_values("../.env")
# db_client = pymongo.MongoClient('mongodb+srv://' +
#                                 f'{config["MONGO_DB_USERNAME"]}:' +
#                                 f'{config["MONGO_DB_PASSWORD"]}' +
#                                 f'@{config["MONGO_DB_HOST"]}' +
#                                 f'?tls=true&authSource=admin&replicaSet={config["MONGO_REPLICA_SET"]}&tlsInsecure=true')
# db_client.dataVKnodup.users.update_many(
# {"user_to_label": True}, {"$set": {"labels": []}}
# )
# generate_database_sample(db_client, 100, 8)
