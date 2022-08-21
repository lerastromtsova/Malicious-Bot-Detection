from dotenv import dotenv_values  # type: ignore
import logging
import vk  # type: ignore
import pymongo  # type: ignore
import os
import multiprocessing as mp
import sys

from data_parser import parse_comment_data
from database_adapter import write_comment_to_db
from models.graph_based_approach import enrich_users_data, get_similarity

config = dotenv_values(".env")
if not config:
    config = os.environ

logging.basicConfig(
    filename='log/data_parsing.log',
    encoding='utf-8',
    level=getattr(logging, config['LOG_LEVEL'].upper())
)
api = vk.API(access_token=config[sys.argv[1]])
db_client = pymongo.MongoClient(f"mongodb+srv://"
                                f"lerastromtsova:{config['MONGO_DB_PASSWORD']}"
                                f"@cluster0.ubfnhtk.mongodb.net/"
                                f"?retryWrites=true&w=majority",
                                tls=True,
                                tlsAllowInvalidCertificates=True)

if __name__ == '__main__':
    while True:
        parse_comment_data(db_client, api, write_comment_to_db)

# #
# var i = 0;
# var comment;
# var comments = [];
# var comment_ids = [797550];
# var media_ids = [-11982368];
# while (i != 1) {
#     comment = API.wall.getComment({"owner_id": (media_ids[i]), "comment_id": (comment_ids[i]), "v": 5.131, "extended": 1});
#     comments.push(comment);
#     i = i + 1;
# };
# return comments;
