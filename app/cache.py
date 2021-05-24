import os
from time import time
from csv import reader

from config import config
from Errors import error_db_handler
from Database.postgres import init_postgres
from Database.clickhouse import init_clickhouse


def create_new_table_clickhouse(clickhouse_db, table):
    clickhouse_db.insert_data(f"DROP TABLE IF EXISTS {table};")
    queries = {
        "raw_data":
            f"""
            CREATE TABLE
                raw_data_new
            (
                file_id int,
                dt Date,
                filter_column1_id int,
                filter_column2_id int,
                filter_column3_id int,
                filter_column4_id int,
                filter_column5_id int,
                filter_column6_id int,
                filter_column7_id int,
                filter_column8_id int,
                filter_column9_id int,
                filter_column10_id int,
                value Int64
            )
            engine = MergeTree
            ORDER BY (
                file_id, dt,
                filter_column1_id, filter_column2_id,
                filter_column3_id, filter_column4_id,
                filter_column5_id, filter_column6_id,
                filter_column7_id, filter_column8_id,
                filter_column9_id, filter_column10_id
            )
            ;
            """
    }

    return clickhouse_db.insert_data(queries[table])


def rename_table_clickhouse(clickhouse_db, table):
    queries = [
        f"CREATE TABLE IF NOT EXISTS {table} (id int) engine = MergeTree ORDER BY (id);",
        f"RENAME TABLE {table} TO {table}_old;",
        f"RENAME TABLE {table}_new TO {table};"
    ]
    for query in queries:
        clickhouse_db.insert_data(query)
    return True


def drop_old_table_clickhouse(clickhouse_db, table):
    query = f"""
    DROP TABLE IF EXISTS {table}_old;
    """
    return clickhouse_db.insert_data(query)


def format_line(line):
    new_line = ",".join((f"'{val}'" if indx == 0 else val if val != "\\N" else "null") for indx, val in enumerate(line))

    return f"({new_line})"


def read_csv_to_clickhouse(clickhouse_db, query, filename):
    with open(f"{config['APP']['PATH_TEMP_FILES']}/{filename}", 'r') as f:
        data_reader = reader(f, delimiter=',')
        transaction = query
        print("start reader")
        count = 0
        insert_count = 0
        stack = 100000
        for line in data_reader:
            transaction += f"{format_line(line)},\n" if count < (stack - 1) else f"{format_line(line)};"
            count += 1
            if count >= stack:
                local_time_start = time()
                check = clickhouse_db.insert_data(query=transaction)
                if check != True:
                    print(transaction)
                    raise NameError('Failed insert')
                transaction = query
                insert_count += count
                count = 0
                print(f"Insert {insert_count} lines to clickhouse time: {time() - local_time_start}")
        else:
            insert_count += count
            local_time_start = time()
            clickhouse_db.insert_data(query=transaction)
            print(f"Insert {insert_count} lines to clickhouse time: {time() - local_time_start}")

    return True


@error_db_handler
@init_postgres
@init_clickhouse
def update_cache_table_clickhouse(postgres_db, clickhouse_db, table_name, file_id=None):
    print('--------------------------------------------------')
    if file_id:
        print(f"Start add new lines to cache_{table_name}_clickhouse")
    else:
        print(f"Start update cache_{table_name}_clickhouse")
    fields = {
        "raw_data": [
            'dt',
            'file_id',
            'filter_column1_id',
            'filter_column2_id',
            'filter_column3_id',
            'filter_column4_id',
            'filter_column5_id',
            'filter_column6_id',
            'filter_column7_id',
            'filter_column8_id',
            'filter_column9_id',
            'filter_column10_id',
            'value'
        ]
    }

    table = f"{table_name}_new"

    copy_to_time_start = time()
    with open(f"{config['APP']['PATH_TEMP_FILES']}/{table_name}", "w+") as f:
        if file_id is None:
            postgres_db.copy_to(f, table_name, fields[table_name], ",")
        else:
            query = "\
                SELECT {fields_} FROM {table_} WHERE file_id={file_id_};\
            ".format(
                fields_=','.join(fields[table_name]),
                table_=table,
                file_id_=file_id
            )

            for row in postgres_db.select_data(query):
                f.write("{}\n".format(
                    ",".join(str(i if i is not None else '\\N') for i in row)))

    print(f"Copy from posgres to csv time: {time() - copy_to_time_start}")

    query = "INSERT INTO {table}({keys}) VALUES \n".format(
        db_name=clickhouse_db.db_name,
        table=table if file_id is None else table,
        keys=",".join(i for i in fields[table_name])
    )
    if file_id is None:
        create_table_time_start = time()
        create_new_table_clickhouse(clickhouse_db, table_name)
        print(f"Create table '{table}'' in clickhouse time: {time() - create_table_time_start}")

    insert_time_start = time()
    read_csv_to_clickhouse(clickhouse_db, query, filename=table_name)
    print(f"Insert ALL to clickhouse time: {time() - insert_time_start}")

    del_file_time_start = time()
    os.remove(f"{config['APP']['PATH_TEMP_FILES']}/{table_name}")
    print(f"Del file {table_name} time: {time() - del_file_time_start}")

    check = None
    if file_id is None:
        rename_time_start = time()
        check = rename_table_clickhouse(clickhouse_db, table_name)
        print(f"Rename table {table} is {check}. Time: {time() - rename_time_start}")

        if check:
            drop_time_start = time()
            check = drop_old_table_clickhouse(clickhouse_db, table_name)
            print(f"DROP table {table}_old is {check}. Time: {time() - drop_time_start}")

    print('--------------------------------------------------')
    return True
