import os
import time
import random

from logging_excel import LoggingResultDBToExcel
from Database.postgres import Postgres_db, config
from Database.clickhouse import Clickhouse_db

doc_rows = []



def get_random_files(files):
    rnd_count = random.randint(1, len(files))
    rnd_files = set()
    while rnd_count - len(rnd_files) > 0:
        rnd_files.add(random.choice(files))

    return list(rnd_files) 


def select_table(obj, wb):
    if type(obj) == Postgres_db:
        return wb.postgres_table
    elif type(obj) == Clickhouse_db:
        return wb.clickhouse_table


def writer_excel(name_func, diff_time, args_, result):
    path = config['APP']['PATH_TO_EXCEL_FILE']

    with LoggingResultDBToExcel(path) as wb:
        if args_:
            table = select_table(args_[0], wb)

            row = doc_rows[-1]
            if name_func == 'select_count_all_data':
                table.cell(column=1, row=row, value=result[0])
            elif name_func == "select_sum_value_in_all_files":
                table.cell(column=2, row=row, value=diff_time)
            elif name_func == "select_min_and_max_dt":
                table.cell(column=3, row=row, value=diff_time)
            elif name_func == "select_sum_value_in_one_files":
                table.cell(column=4, row=row, value=diff_time)
            elif name_func == "select_without_filters":
                table.cell(column=5, row=row, value=diff_time)
            elif name_func == "select_with_filters":
                table.cell(column=5 + args_[-1], row=row, value=diff_time)


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
    filename = config['APP']['PATH_TO_LOG_FILE']

    if not os.path.isfile(filename):
        mode = 'w+'

    with open(filename, mode) as f:
        f.write(log)


def timer(isRetunrnTime=True):
    """
        If isRetunrnTime=True then returned:
        {
            'result': result,
            'total_time': time
        }
    """
    def decorator_timer(func):
        def the_wrapper_around_the_original_function(*args, **kwargs):
            if func.__name__ in [
                'select_all_query_postgres',
                'select_all_query_clickhouse'
            ]:
                doc_rows.append(kwargs['row'])

            start_time = time.time()
            result = func(*args, **kwargs)
            stop_time = time.time()

            log_timer(func.__name__, stop_time - start_time, args)
            writer_excel(func.__name__, stop_time - start_time, args, result)

            if isRetunrnTime:
                return {
                    'result': result,
                    'total_time': stop_time - start_time
                }

            return result

        return the_wrapper_around_the_original_function

    return decorator_timer


def random_date(start, end, prop):
    format = '%Y-%m-%d'

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))
