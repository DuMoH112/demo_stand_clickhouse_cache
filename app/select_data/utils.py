import os
import time

from Database.postgres import Postgres_db
from Database.clickhouse import Clickhouse_db


def log_timer(name_func, diff_time, args_):
    arguments = ", ".join(f'{i}' for i in args_)
    log = '''
{stars}
function: {function}
arguments: {arguments}
time: {time}
{stars}
        '''.format(
        stars='*******************',
        function=name_func,
        arguments=arguments,
        time=diff_time
    )

    mode = 'a'
    filename = 'timer.log'

    if not os.path.isfile(filename):
        mode = 'w+'

    with open(filename, mode) as f:
        f.write(log)


def timer(func):
    def the_wrapper_around_the_original_function(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        stop_time = time.time()

        log_timer(func.__name__, stop_time - start_time, args)

        return result

    return the_wrapper_around_the_original_function


def random_date(start, end, prop):
    format = '%Y-%m-%d'

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


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
