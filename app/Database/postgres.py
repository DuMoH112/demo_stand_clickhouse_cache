from time import time
from uuid import uuid4

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

from config import config
from Errors import ConnectError, ExecuteError

class Postgres_db:
    conn = None

    def __init__(self):
        conn = self.connect(config)
        if conn:
            self.conn = conn
        else:
            print("Нет подключения к БД")
            raise ConnectError('No connection to database Postgres')

    def __init_named_cursor(func):
        def the_wrapper_around_the_original_function(self, *args, **kwargs):
            cursor = None
            try:
                cursor = self.conn.cursor(str(uuid4()))
            except AttributeError:
                raise ConnectError('No connection to database Postgres')

            try:
                return func(self, *args, **kwargs, cursor=cursor)
            except AttributeError:
                raise ConnectError('No connection to database Postgres')
            except Exception as e:
                bad_query = None
                if type(args[0]) == sql.Composed:
                    bad_query = args[0].as_string(self.conn)
                elif type(args[0]) == str:
                    bad_query = args[0]
                cursor = self.conn.cursor(cursor_factory=DictCursor)

                self.print_error(e, bad_query)

                raise ExecuteError('Execute error to database Postgres')
                
        return the_wrapper_around_the_original_function

    def __init_dict_cursor(func):
        def the_wrapper_around_the_original_function(self, *args, **kwargs):
            cursor = None
            try:
                cursor = self.conn.cursor(cursor_factory=DictCursor)
            except AttributeError:
                raise ConnectError('No connection to database Postgres')

            try:
                return func(self, *args, **kwargs, cursor=cursor)
            except AttributeError:
                raise ConnectError('No connection to database Postgres')
            except Exception as e:
                bad_query = None
                if type(args[0]) == sql.Composed:
                    bad_query = args[0].as_string(self.conn)
                elif type(args[0]) == str:
                    bad_query = args[0]

                self.print_error(e, bad_query)

                raise ConnectError('No connection to database Postgres')

        return the_wrapper_around_the_original_function

    def connect(self, config):
        """Connect to database PostgreSQL"""
        try:
            conn = psycopg2.connect(
                dbname=str(config['POSTGRES']['POSTGRES_DATABASE_NAME']),
                user=str(config['POSTGRES']['POSTGRES_USERNAME']),
                password=str(config['POSTGRES']['POSTGRES_PASSWORD']),
                host=str(config['POSTGRES']['POSTGRES_HOST']),
                port=str(config['POSTGRES']['POSTGRES_PORT'])
            )
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError:
            return False

    def close(self):
        """Close connect with database"""
        if self.conn:
            self.conn.close()
        return True

    @__init_dict_cursor
    def select_data(self, execute, cursor):
        # Если присылаемым значение было error, то вызывается исключение
        if execute == "error":
            raise AttributeError
        cursor.execute(execute)

        return cursor.fetchall()

    @__init_named_cursor
    def copy_to(self, file, table, columns, sep, cursor):
        cursor.copy_to(file, table, columns=columns, sep=sep)

        return True

    @__init_dict_cursor
    def insert_data(self, execute, cursor):
        cursor.execute(execute)

        return True


    def print_error(self, e, bad_query=None):
        if bad_query:
            print(f"""
================POSTGRES_ERROR================
    type: {type(e)},
    arguments: {e.args},
    text: {e},
    time: {time()},
    bad_query: {bad_query}
==============================================
                """)
        else:
            print(f"""
================POSTGRES_ERROR================
    type: {type(e)},
    arguments: {e.args},
    text: {e},
    time: {time()}
==============================================
                """)