import os

import pymongo
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

if config['LOCAL_DB']:
    db_client = pymongo.MongoClient(host="localhost", port=27017)
else:
    db_client = pymongo.MongoClient(f"mongodb+srv://"
                                    f"lerastromtsova:{config['MONGO_DB_PASSWORD']}"
                                    f"@cluster0.ubfnhtk.mongodb.net/"
                                    f"?retryWrites=true&w=majority",
                                    tls=True,
                                    tlsAllowInvalidCertificates=True)

USERS_LIMIT = 10


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
    if request.args:
        prolific_id = request.args.get('prolific_id')
        next_user_id = request.args.get('next_user')
        user = get_user_by_id(db_client, int(next_user_id))[0]
        comments = get_comments_by_user(db_client, int(next_user_id))
        return render_template('labelling.html', prolific_id=prolific_id, current_user=user, comments=comments)
    return render_template('labelling.html')


if __name__ == "__main__":
    app.run()
