import os

import pymongo
from bson.json_util import dumps
from bson.json_util import loads
from dotenv import dotenv_values
from flask import Flask, render_template, request
from flask import session, redirect
from flask_babel import Babel

from database_adapter import get_user_by_id, get_users_by_name
from database_adapter import get_comments_by_user
from models import bot_check_results

import iuliia

app = Flask(__name__)
babel = Babel(app)

config = dotenv_values(".env")
if not config:
    config = os.environ

if config['LOCAL_DB'] != '0':
    db_client = pymongo.MongoClient(host="localhost", port=27017)
else:
    conn_uri = f'mongodb+srv://' \
               f'{config["MONGO_DB_USERNAME"]}:' \
               f'{config["MONGO_DB_PASSWORD"]}' \
               f'@{config["MONGO_DB_HOST"]}' \
               f'?tls=true&authSource=admin&' \
               f'replicaSet={config["MONGO_REPLICA_SET"]}&tlsInsecure=true'
    db_client = pymongo.MongoClient(conn_uri)

USERS_LIMIT = 10
USERS_TO_LABEL_LIMIT = 10


@app.route("/")
def index():
    return redirect('search')


@app.route("/search")
def search():
    if request.args:
        query = request.args.get('user')
        if query.isdigit():
            users = get_user_by_id(db_client, query)
        else:
            query = iuliia.translate(query, schema=iuliia.WIKIPEDIA)
            users = get_users_by_name(
                db_client,
                query,
                users_limit=USERS_LIMIT
            )
        if users:
            # comments = get_comments_by_user(db_client, user_id)
            return render_template('index.html', users=users, comments=[])
        return render_template('index.html', error='Not Found')
    return render_template('index.html')


@app.route("/is_bot")
def is_bot():
    if request.args:
        user_id = int(request.args.get('user'))
        users = get_user_by_id(db_client, user_id)
        bot_check_result = bot_check_results(users[0])
        comments = get_comments_by_user(db_client, user_id)
        return render_template(
            'bot-check-results.html',
            user=users[0],
            is_bot=bot_check_result,
            comments=comments
        )
    return render_template('index.html')


@app.route("/contact")
def contact():
    return render_template('contact.html')


@app.route("/methods")
def methods():
    return render_template('methods.html')


@app.route('/language=<language>')
def set_language(language=None):
    session['language'] = language
    return redirect(request.referrer)


@babel.localeselector
def get_locale():
    if request.args.get('language'):
        session['language'] = request.args.get('language')
    return session.get('language', 'en')


app.config['LANGUAGES'] = {
    'en': '🇬🇧 English',
    'ru': '🇷🇺 Русский',
    'uk': '🇺🇦 Ukrainian'
}

app.secret_key = config['WEB_SECRET']


@app.context_processor
def inject_conf_var():
    return dict(AVAILABLE_LANGUAGES=app.config['LANGUAGES'],
                CURRENT_LANGUAGE=session.get(
                    'language',
                    request.accept_languages.best_match(
                        app.config['LANGUAGES'].keys()
                    )
                ))


@app.route("/labelling")
def labelling():
    if request.args.get('prolific_id') and 'users_to_label' not in session:
        session['prolific_id'] = request.args.get('prolific_id')
        aggregation = db_client.dataVKnodup.users.aggregate([
            {'$match': {"$and": [
                {
                    "labels": {"$not": {
                        "$elemMatch": {"by": session['prolific_id']}
                    }}
                },
                {
                    "user_to_label": True
                }
            ]}},
            {'$project': {
                'vk_id': 1,
                'photo_100': 1,
                'screen_name': 1,
                'first_name': 1,
                'last_name': 1,
                'deactivated': 1
            }},
            {'$sample': {'size': USERS_TO_LABEL_LIMIT}}
        ])
        users_to_label = dumps(list(aggregation), separators=(',', ':'))
        session['users_to_label'] = users_to_label
        session['total_to_label'] = USERS_TO_LABEL_LIMIT + 1
    if 'prolific_id' in session and 'users_to_label' in session:
        prolific_id = session['prolific_id']
        users_to_label = loads(session['users_to_label'])

        prev_user_id = int(request.args.get('prev_user_id'))
        if request.args.get('prev_user_result'):
            prev_user_result = request.args.get('prev_user_result')
            db_client.dataVKnodup.users.update_one(
                {'vk_id': int(users_to_label[prev_user_id]['vk_id'])},
                {'$push': {'labels': {
                    'by': prolific_id,
                    'result': prev_user_result
                }}}
            )
        session['total_to_label'] = USERS_TO_LABEL_LIMIT - prev_user_id - 1
        if session['total_to_label'] == 0:
            #     no more users to label
            return redirect('labelling-end')
        next_user_id = prev_user_id + 1
        user = users_to_label[next_user_id]
        comments = get_comments_by_user(db_client, user['vk_id'])
        return render_template(
            'labelling.html',
            prolific_id=prolific_id,
            current_user=user,
            comments=comments,
            count=next_user_id
        )
    return render_template('labelling.html')


@app.route("/labelling-end")
def labelling_end():
    if request.args.get('explain_decisions'):
        db_client.dataVKnodup.free_responses.insert_one({
            'prolific_id': session['prolific_id'],
            'free_text': request.args.get('explain_decisions')
        })
        session.pop('prolific_id', None)
        session.pop('users_to_label', None)
        return render_template(
            'labelling-end.html',
            completion_code=config['COMPLETION_CODE']
        )
    return render_template('labelling-end.html')


if __name__ == "__main__":
    app.run()
