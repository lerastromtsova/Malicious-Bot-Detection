import pymongo
from dotenv import dotenv_values
from flask import Flask, render_template, request

from database_adapter import get_user_data, get_comments_by_user

app = Flask(__name__)

config = dotenv_values(".env")
db_client = pymongo.MongoClient(f"mongodb+srv://"
                                f"lerastromtsova:{config['MONGO_DB_PASSWORD']}"
                                f"@cluster0.ubfnhtk.mongodb.net/"
                                f"?retryWrites=true&w=majority",
                                tls=True,
                                tlsAllowInvalidCertificates=True)

@app.route("/search")
def index():
    if request.args:
        user_id = request.args.get('user')
        users = get_user_data(db_client, user_id)
        comments = get_comments_by_user(db_client, user_id)
        return render_template('index.html', users=users, comments=comments)
    return render_template('index.html')
