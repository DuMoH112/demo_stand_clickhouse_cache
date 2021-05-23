import random

from Errors import error_db_handler
from Database.postgres import init_postgres
from Database.clickhouse import init_clickhouse
from select_data.utils import (
    timer,
    random_date
)


@error_db_handler
@init_clickhouse
@init_postgres
@timer(isRetunrnTime=False)
def select_all_query_clickhouse(clickhouse_db, postgres_db):
    files = select_files(postgres_db)
    if not files:
        return False

    functions = [
        (select_sum_value_in_all_files, [clickhouse_db]),
        (select_min_and_max_dt, [clickhouse_db, random.choice(files)]),
        (select_sum_value_in_one_files, [clickhouse_db, random.choice(files)]),
        (select_with_filters, [clickhouse_db, random.choice(files), 0]),
        (select_with_filters, [clickhouse_db, random.choice(files), 1]),
        (select_with_filters, [clickhouse_db, random.choice(files), 2]),
        (select_with_filters, [clickhouse_db, random.choice(files), 3]),
        (select_with_filters, [clickhouse_db, random.choice(files), 4]),
        (select_with_filters, [clickhouse_db, random.choice(files), 5]),
        (select_with_filters, [clickhouse_db, random.choice(files), 6]),
        (select_with_filters, [clickhouse_db, random.choice(files), 7]),
        (select_with_filters, [clickhouse_db, random.choice(files), 8]),
        (select_with_filters, [clickhouse_db, random.choice(files), 9]),
        (select_with_filters, [clickhouse_db, random.choice(files), 10])
    ]

    result = []

    for function, arguments in functions:
        response = function(*arguments)
        if type(response) == dict:
            result.append({
                "total_time": response.get('total_time'),
                "result": response.get('result')
            })
        else:
            result.append(response)

    return result


@timer(isRetunrnTime=False)
def select_files(postgres_db):
    files = postgres_db.select_data("SELECT id FROM file")

    if files:
        files = [i[0] for i in files]

    return files


@timer(isRetunrnTime=True)
def select_min_and_max_dt(clickhouse_db, file_id):
    response = clickhouse_db.select_data(f"""
        SELECT 
            min(dt),
            max(dt)
        FROM raw_data 
        WHERE file_id={file_id}
    """)

    return response


@timer(isRetunrnTime=True)
def select_sum_value_in_all_files(clickhouse_db):
    response = clickhouse_db.select_data("""
        SELECT 
            sum(value)
        FROM raw_data 
        GROUP BY file_id
    """)

    return response


@timer(isRetunrnTime=True)
def select_sum_value_in_one_files(clickhouse_db, file_id):
    response = clickhouse_db.select_data(f"""
        SELECT 
            sum(value)
        FROM raw_data
        WHERE file_id={file_id}
    """)

    return response


@timer(isRetunrnTime=True)
def select_with_filters(clickhouse_db, file_id, count_filters):
    other_filters = []
    if count_filters:
        for numb_filter in range(1, count_filters + 1):
            other_filters.append(
                f'filter_column{numb_filter}_id={random.randint(1, 4)}')

        other_filters = " and " + " and ".join(other_filters)
    else:
        other_filters = ""

    response = clickhouse_db.select_data(f"""
        SELECT
            sum(value)
        FROM raw_data
        WHERE
            file_id={file_id} and
            dt='{random_date('2019-01-01', '2020-01-01', random.random())}'
            {other_filters}
        """)

    return response
