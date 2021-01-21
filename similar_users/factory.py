from json import JSONEncoder
from flask import Flask


class BinaryJSONEncoder(JSONEncoder):
    """
    A custom JSONEncoder that handles encoding
    of binary objects.

    Example:
        >>> data = {"key": b"binary_value"}
        >>> print(json.dumps(data, cls=BinaryJSONEncoder))
    """
    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode("utf-8")
        return JSONEncoder.default(obj)


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
        # jsonify() responses will be encoded with BinaryJSONEncoder.
        # This is needed to allow serialisation of binary (varbinary stored) user names.
        app.json_encoder = BinaryJSONEncoder
        return app
