from time import time

from Database.postgres import Postgres_db
from Database.clickhouse import Clickhouse_db


def timer(func):
    def the_wrapper_around_the_original_function(*args, **kwargs):
        start_time = time()
        result = func(*args, **kwargs)
        print(
            '''
{stars}
function: {function}
time: {time}
{stars}
            '''.format(
                stars='*******************',
                function=func.__name__,
                time=time() - start_time
            )
        )
        return result

    return the_wrapper_around_the_original_function


def init_postgres(func):
    def the_wrapper_around_the_original_function(*args, **kwargs):
        postgres_db = Postgres_db()
        result = func(*args, **kwargs, postgres_db=postgres_db)
        return result

    return the_wrapper_around_the_original_function


def init_clickhouse(func):
    def the_wrapper_around_the_original_function(*args, **kwargs):
        clickhouse_db = Clickhouse_db()
        result = func(*args, **kwargs, clickhouse_db=clickhouse_db)
        return result

    return the_wrapper_around_the_original_function