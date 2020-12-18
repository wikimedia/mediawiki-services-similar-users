import json
import pytest
from conftest import TEST_USER, TEST_USER_MISSING


def get_url(client, credentials, url="/", headers=None):
    if not headers:
        headers = {"Authorization": f"Basic {credentials}"}
    return client.get(url, headers=headers)


def test_forbid_access_root(client, credentials):
    rv = get_url(client, credentials, url="/")
    assert rv.status_code == 403


@pytest.mark.parametrize(
    "url",
    [
        f"/similarusers?usertext={TEST_USER_MISSING}",
        f"/similarusers?usertext={TEST_USER}",
    ],
)
def test_get_similarusers(client, credentials, url):
    rv = get_url(client, credentials, url=url)
    assert rv.status_code == 200


def test_database_refresh(client, credentials):
    rv = get_url(client, credentials, url='/database/refresh')
    expected = {'in_progress': False}
    assert rv.status_code == 200
    assert json.loads(rv.data) == expected