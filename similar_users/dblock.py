from flask import current_app
from contextlib import contextmanager
from functools import wraps

from .models import database

app = current_app


def release_mysql_lock(name):
    status = database.session.execute(f"SELECT RELEASE_LOCK('{name}')").scalar()
    if status == 1:
        app.logger.debug(f'Released application lock `{name}`')
    elif status == 0:
        app.logger.debug(f'No application lock found with name `{name}`')
    else:
        # Could not explicitly release the application lock. Rely on implicit termination
        # once session ends
        app.logger.debug(f'Failed to release application lock `{name}`')


@contextmanager
def mysql_lock(name, timeout=3, retry=0):
    """
    A simple spinlock for distributed tasks that require infrequent
    db access (e.g. batch periodic updates).
    It's orchestrated by acquiring a user lock on mysql.

    :param name: lock name
    :param timeout: waiting time (in seconds) between retries
    :param retry: number of attempts to acquire a lock
    :return:
    """
    resource = None
    tries = max(0, retry - 1)
    while not resource and tries >= 0:
        app.logger.debug(f'Attempting to acquire application lock `{name}`. Waiting for `{timeout}` seconds.'
                         f'`{tries}` re-tries left.')
        resource = database.session.execute(
        f"SELECT GET_LOCK('{name}', {timeout})").scalar()
        tries -= 1
    else:
        if resource:
            app.logger.debug(f'Acquired application lock `{name}`.')
            yield resource
            release_mysql_lock(name=name)
        else:
            raise RuntimeError(f"Could not acquire application lock `{name}`.")


def is_used_lock(name='lock_ingestion'):
    is_used = False
    rdbms = database.session.bind.dialect.name
    if rdbms == "mysql":
        is_used = bool(database.session.execute(
            f"SELECT IS_USED_LOCK('{name}'").scalar())
    else:
        app.logger.warning(f'Failed to test for application lock. '
                           f'The IS_USED_LOCK function is not available on {rdbms}.')
    return is_used


def application_lock(func, name="lock_ingestion", timeout=10, retry=0):
    """
    :param name: lock name
    :param timeout: waiting time (in seconds) between retries
    :param retry: number of attempts to acquire a lock
    :return:
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        if database.session.bind.dialect.name == "mysql":
            with mysql_lock(name, timeout, retry) as lock:
                if lock:
                    func(*args, **kwargs)
                else:
                    raise Exception(f'Could not acquire lock {name}')
        else:
            func(*args, **kwargs)

    return wrapper
