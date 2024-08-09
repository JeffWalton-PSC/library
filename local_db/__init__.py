import os


# local connection information
def connection(test=False):
    from sqlalchemy import create_engine

    if test==True:
        db_user = os.environ.get("TEST_POWERCAMPUS_USER")
        db_pass = os.environ.get("TEST_POWERCAMPUS_PASS")
        db_host = os.environ.get("TEST_POWERCAMPUS_HOST")
        db_database = os.environ.get("DB_DATABASE")
        # db_driver = os.environ.get("DB_DRIVER")
    elif test=='write':
        db_user = os.environ.get("SQL_USER")
        db_pass = os.environ.get("SQL_PASS")
        db_host = os.environ.get("DB_HOST")
        db_database = os.environ.get("DB_DATABASE")
        # db_driver = os.environ.get("DB_DRIVER")
    else:
        db_user = os.environ.get("DB_USER")
        db_pass = os.environ.get("DB_PASS")
        db_host = os.environ.get("DB_HOST")
        db_database = os.environ.get("DB_DATABASE")
        # db_driver = os.environ.get("DB_DRIVER")

    # engine = create_engine(
    #     f"mssql+pyodbc://{db_user}:{db_pass}"
    #     + f"@{db_host}/{db_database}?"
    #     + f"driver={db_driver}"
    # )
    connection_str = fr"mssql+pymssql://{db_user}:{db_pass}@{db_host}/{db_database}"
    engine = create_engine(connection_str)
    return engine.connect()


def print_connection_variables(test=False):
    if test==True:
        db_user = os.environ.get("TEST_POWERCAMPUS_USER")
        db_pass = os.environ.get("TEST_POWERCAMPUS_PASS")
        db_host = os.environ.get("TEST_POWERCAMPUS_HOST")
        db_database = os.environ.get("DB_DATABASE")
        db_driver = os.environ.get("DB_DRIVER")
    elif test=='write':
        db_user = os.environ.get("SQL_USER")
        db_pass = os.environ.get("SQL_PASS")
        db_host = os.environ.get("DB_HOST")
        db_database = os.environ.get("DB_DATABASE")
        db_driver = os.environ.get("DB_DRIVER")
    else:
        db_user = os.environ.get("DB_USER")
        db_pass = os.environ.get("DB_PASS")
        db_host = os.environ.get("DB_HOST")
        db_database = os.environ.get("DB_DATABASE")
        db_driver = os.environ.get("DB_DRIVER")

    print(db_user)
    print(db_pass)
    print(db_host)
    print(db_database)
    print(db_driver)
