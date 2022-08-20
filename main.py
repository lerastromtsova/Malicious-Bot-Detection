from dotenv import dotenv_values  # type: ignore
from data_parser import parse_comment_data
from database_adapter import write_comment_to_db
import logging
import vk  # type: ignore
import pymongo  # type: ignore
import os
import multiprocessing as mp
import sys

from data_parser import parse_comment_data
from database_adapter import write_comment_to_db

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
    # insert_comment_ids(db_client, api)
    for comment in parse_comment_data(db_client, api):
        # write_comment_to_db(comment, db_client)
        write_comment_to_db(comment, db_client)

#
# var c = 25;
# while (c != 0) {
#     c = c - 1;
#     var comment = API.wall.getComment({"owner_id"="-129244038","comment_id"="2787368","v"="5.131","extended"="1"});
# };
# return comment;