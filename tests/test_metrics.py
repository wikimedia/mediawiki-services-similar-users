from similar_users.metrics import ExecutionTime


def test_execution_time():
    with ExecutionTime() as timer:
        pass
    assert timer.elapsed > 0