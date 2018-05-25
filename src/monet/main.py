import json
import sys

import pymonetdb

sys.path.append('../../config/')
from config import config


def _create_insert_table():
    command = """
        CREATE TABLE crimestatsocial (
            "reg_id" INTEGER,
            "reg_name" CHAR(64),
            "year" INTEGER,
            "group_id" INTEGER,
            "group_name" CHAR(64),
            "category" CHAR(64),
            "gender" CHAR(64),
            "value" INTEGER
        );"""
    conn = None
    try:
        params = config('../../config/database.ini', 'monetdb')
        conn = pymonetdb.connect(
            database=params["database"],
            hostname=params["hostname"],
            username=params["username"],
            password=params["password"])
        cur = conn.cursor()
        cur.arraysize = 100
        cur.execute(command)
        with open('../../dataset/crimestatsocial_final.json') as f:
            for line in f:
                parameters = tuple(json.loads(line).values())
                cur.execute("""
                    INSERT INTO crimestatsocial
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", parameters)
        cur.close()
        conn.commit()
    except (Exception, pymonetdb.exceptions.StandardError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    _create_insert_table()
