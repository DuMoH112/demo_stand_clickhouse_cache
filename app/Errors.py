import traceback
from time import time


class ConnectError(BaseException):
    pass


class ExecuteError(BaseException):
    pass


def error_db_handler(func):
    def the_wrapper_around_the_original_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ConnectError:
            print("[ERROR] Нет подключения к БД")
            return False
        except ExecuteError:
            print("[ERROR] Ошибка обращения к БД")
            return False
        except Exception as e:
            print("""
================ERROR================
    traceback: """)
            traceback.print_stack()
            print(f"""
    type: {type(e)},
    arguments: {e.args},
    text: {e},
    time: {time()}
=====================================
                """)
            return False

    return the_wrapper_around_the_original_function
