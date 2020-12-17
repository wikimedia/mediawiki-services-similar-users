from flask import Flask


def create_app(config=None):
    """
    Instantiate a Flask application, and register extensions, according to
    the Application Factories pattern
    https://flask.palletsprojects.com/en/1.1.x/patterns/appfactories/

    :param config: a dict of Flask configuration options
    :return: a Flask application
    """
    app = Flask(__name__)

    if config:
        app.config.update(config)

    from .wsgi import api
    from .wsgi import metrics, cors, basic_auth, database, swagger

    with app.app_context():
        for extension in (metrics, cors, basic_auth, database, swagger):
            extension.init_app(app=app)

        app.register_blueprint(api)
        return app