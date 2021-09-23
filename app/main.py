import os
import tempfile

from config import init_config, config
from Errors import error_db_handler

from cache import update_cache_table_clickhouse
from generate_data_postgres import generate_data
from select_data.select_postgres import select_all_query_postgres
from select_data.select_clickhouse import select_all_query_clickhouse
from script_generation_data import (
    data_generation_scripts_big_different,
    data_generation_scripts_low_different,
    data_generation_scripts_test
)


@error_db_handler
def main():
    print('START PYTHON')
    init_config('/app/settings.ini')

    # Create directory for temp files
    os.makedirs(config['APP']['PATH_TEMP_FILES'], exist_ok=True)
    tempfile.tempdir = config['APP']['PATH_TEMP_FILES']
    
    scripts = data_generation_scripts_low_different
    for indx, script in enumerate(scripts):
        print(f'[INFO] Start {indx + 1}/{len(scripts)} script')
        generate_data(list_count_rows=script)
        update_cache_table_clickhouse(table_name='raw_data')
        select_all_query_postgres(row=indx + 2)
        select_all_query_clickhouse(row=indx + 2)

    return True


if __name__ == "__main__":
    main()
