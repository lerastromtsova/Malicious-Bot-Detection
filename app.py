import os

import pymongo  # type: ignore
from dotenv import dotenv_values  # type: ignore
from flask import Flask, render_template, request, session, redirect  # type: ignore
from flask_babel import Babel  # type: ignore

from database_adapter import get_user_data, get_comments_by_user
from models import bot_check_results

app = Flask(__name__)
babel = Babel(app)

config = dotenv_values(".env")
if not config:
    config = os.environ

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


@app.route("/is_bot")
def is_bot():
    if request.args:
        user_id = request.args.get('user')
        user = list(get_user_data(db_client, user_id))[0]
        bot_check_result = bot_check_results(user_id)
        return render_template(
            'bot-check-results.html',
            user=user,
            is_bot=bot_check_result
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


if __name__ == "__main__":
    app.run()
