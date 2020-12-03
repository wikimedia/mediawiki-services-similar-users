import pytest
from datetime import datetime

from base64 import b64encode
from similar_users.models import database, Coedit, UserMetadata, Temporal
from similar_users.factory import create_app
from similar_users.wsgi import TIME_FORMAT

TEST_DATABASE_URI = "sqlite:///:memory:"
BASIC_AUTH_USERNAME = "test"
BASIC_AUTH_PASSWORD = "test"
TEST_USER = "testuser"
TEST_USER_MISSING = "not_found"

users = [
    UserMetadata(
        user_text=TEST_USER,
        is_anon=False,
        num_edits=4139,
        num_pages=3520,
        most_recent_edit=datetime.strptime("2020-09-21T23:42:39Z", TIME_FORMAT),
        oldest_edit=datetime.strptime("2020-06-28T17:24:14Z", TIME_FORMAT),
    ),
    UserMetadata(
        user_text="127.0.0.1",
        is_anon=True,
        num_edits=1,
        num_pages=1,
        most_recent_edit=datetime.strptime("2020-02-04T16:26:02Z", TIME_FORMAT),
        oldest_edit=datetime.strptime("2020-02-04T16:26:02Z", TIME_FORMAT),
    ),
]


coedits = [
    Coedit(user_text=TEST_USER, user_text_neighbour="127.0.0.1", overlap_count=1),
]


temporals = [
    Temporal(
        user_text=TEST_USER,
        d=7,
        h=21,
        num_edits=86,
    ),
]


@pytest.fixture(scope="session")
def credentials():
    user = bytes(str(BASIC_AUTH_USERNAME), encoding="utf-8")
    password = bytes(str(BASIC_AUTH_PASSWORD), encoding="utf-8")
    return b64encode(b"%s:%s" % (user, password)).decode("utf-8")


@pytest.fixture(scope="session")
def app(request):
    """Session-wide test `Flask` application."""
    config = {
        "CUSTOM_UA": "spd (cloud-vps) - pytest ",
        "EARLIEST_TS": "2020-02-04T16:26:02Z",
        "MOST_RECENT_REV_TS": "2020-09-21T23:42:39Z",
        "JSON_SORT_KEYS": False,
        "TEMPORAL_OFFSET": "(-1, 0, 1)",
        "NAMESPACES": [0],
        "EDIT_WINDOW": 1,
        "TESTING": True,
        "SQLALCHEMY_DATABASE_URI": TEST_DATABASE_URI,
        "SQLALCHEMY_TRACK_MODIFICATIONS": False,
        "SQLALCHEMY_ECHO": True,
        "BASIC_AUTH_USERNAME": BASIC_AUTH_USERNAME,
        "BASIC_AUTH_PASSWORD": BASIC_AUTH_PASSWORD,
        "MWAPI_RETRIES": 0
    }
    app = create_app(config)
    with app.app_context():
        database.create_all()

        for user in users:
            database.session.add(user)

        for coedit in coedits:
            database.session.add(coedit)

        for temporal in temporals:
            database.session.add(temporal)

        database.session.commit()
        yield app
        database.drop_all()


@pytest.fixture(scope="function")
def client(app):
    with app.test_client() as client:
        yield client
