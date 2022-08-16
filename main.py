from dotenv import dotenv_values  # type: ignore
from data_parser import get_foaf_data
import logging
import vk  # type: ignore
import pymongo  # type: ignore
import os

config = dotenv_values(".env")
if not config:
    config = os.environ

logging.basicConfig(
    filename='log/data_parsing.log',
    encoding='utf-8',
    level=getattr(logging, config['LOG_LEVEL'].upper())
)
api = vk.API(access_token=config['VK_API_TOKEN'])
db_client = pymongo.MongoClient(f"mongodb+srv://"
                                f"lerastromtsova:{config['MONGO_DB_PASSWORD']}"
                                f"@cluster0.ubfnhtk.mongodb.net/"
                                f"?retryWrites=true&w=majority",
                                tls=True,
                                tlsAllowInvalidCertificates=True)

if __name__ == '__main__':
    # insert_comment_ids(db_client, api)
    # for comment in parse_comment_data(db_client, api):
    #     write_comment_to_db(comment, db_client)
    for i in ['635410348', '3169374', '115014625', '303693052', '93584656']:
        print(get_foaf_data(i))
