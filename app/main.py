from time import sleep


from config import init_config, config
from Errors import error_db_handler
from Database.postgres import Postgres_db
from Database.clickhouse import Clickhouse_db


@error_db_handler
def main():
    # sleep(20)
    print('START PYTHON')
    init_config('/app/settings.ini')

    return True


if __name__ == "__main__":
    main()
