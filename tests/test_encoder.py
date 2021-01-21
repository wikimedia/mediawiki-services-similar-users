from similar_users.factory import BinaryJSONEncoder

import json


def test_binary_jsonencoder():
    data = {'key1': b"binary_value", "key2": "value"}
    assert json.dumps(data, cls=BinaryJSONEncoder)
