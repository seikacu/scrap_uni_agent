import csv

import psycopg

from secure import PSql, log


def connect_db():
    connection = psycopg.connect(
        host=PSql.host,
        user=PSql.user,
        password=PSql.password,
        dbname=PSql.db_name
    )
    return connection


def check_exist_table(connection):
    with connection.cursor() as cursor:
        cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('uniagent',))
        return cursor.fetchone()[0]


def create_table(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE uniagent (
                    id serial NOT NULL,
                    url text NOT NULL,
                    organization varchar(1000),
                    country varchar(100) NOT NULL,
                    deals varchar(2000),
                    page varchar(5),
                    company text,
                    path_page text,
                    CONSTRAINT "uniagent_pk" PRIMARY KEY ("id")
                    ) WITH (
                    OIDS=FALSE
                );"""
            )

            print("[INFO] Table created successfully")

    except Exception as _ex:
        log.write_log("db_sql_create_table_ads ", _ex)
        print("db_sql_create_table_ads_ Error while working with PostgreSQL", _ex)
        pass


def insert_url_table(connection, url, country, page, company):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO uniagent (url, country, page, company) VALUES 
                    ('{url}', '{country}', '{page}', '{company}');"""
            )
    except Exception as _ex:
        log.write_log("db_sql_insert_to_table ", _ex)
        print("db_sql_insert_to_table_  Error while working with PostgreSQL", _ex)
        pass


def add_main_data(connection, id_db, organization, deals):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""UPDATE uniagent SET organization = '{organization}', deals = '{deals}' WHERE id = {id_db};""")

            print(f"[INFO] Information by id {id_db} was successfully add")
    except psycopg.Error as e:
        print(e)
        log.write_log("add_main_data ", e)
    except Exception as _ex:
        log.write_log("add_main_data ", _ex)
        print("db_sql_add_main_data Error while working with PostgreSQL", _ex)


def add_path_page(connection, id_db, path_page):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""UPDATE uniagent SET path_page = '{path_page}' WHERE id = {id_db};""")

            print(f"[INFO] Path_page {path_page} was successfully add")

    except Exception as _ex:
        log.write_log("db_sql_add_path_page ", _ex)
        print("db_sql__Path_page Error while working with PostgreSQL", _ex)
        pass


def get_links_from_table(connection):
    with connection.cursor() as cursor:
        cursor.execute(f"""SELECT id, url FROM uniagent WHERE path_page IS NULL;""")
        if cursor.fetchone is not None:
            return cursor.fetchall()


def get_id_from_table(connection):
    with connection.cursor() as cursor:
        cursor.execute(f"""SELECT id, path_page FROM uniagent WHERE path_page IS NOT NULL AND organization is NULL ORDER BY id;""")
        if cursor.fetchone is not None:
            return cursor.fetchall()
