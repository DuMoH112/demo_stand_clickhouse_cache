from Errors import error_db_handler
from select_data.utils import timer
from Database.postgres import init_postgres


@error_db_handler
@init_postgres
@timer(True)
def generate_data(postgres_db, list_count_rows):
    postgres_db.select_data("""
        SELECT
            {}
    """.format(
        ",".join(
            f'filling_of_raw_data_tables({count})' for count in list_count_rows)
    ))

    return True
