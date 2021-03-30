import os
from time import time

from Database.postgres import Postgres_db
from Database.clickhouse import Clickhouse_db


def timer(func):
    def the_wrapper_around_the_original_function(*args, **kwargs):
        start_time = time()
        arguments = ",".join(f'{i}={kwargs[i]}' for i in kwargs)
        result = func(*args, **kwargs)
        stop_time = time()
        
        log = '''
{stars}
function: {function}
arguments: {arguments}
time: {time}
{stars}
            '''.format(
                stars='*******************',
                function=func.__name__,
                arguments=arguments,
                time=stop_time - start_time
        )

        mode = 'a'
        filename = 'timer.log'

        if not os.path.isfile(filename):
            mode = 'w+'

        with open(filename, mode) as f:
            f.write(log)

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