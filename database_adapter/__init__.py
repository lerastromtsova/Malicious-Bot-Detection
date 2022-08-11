import pymongo  # type: ignore
from datetime import datetime


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
    db = db_client.dataVK
    for item in comment['items']:
        item['date'] = datetime.utcfromtimestamp(item['date'])
    if comment['items']:
        db.comments.insert_many(comment['items'])
    if comment['groups']:
        db.groups.insert_many(comment['groups'])
    if comment['profiles']:
        db.users.insert_many(comment['profiles'])
