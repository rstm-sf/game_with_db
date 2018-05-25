import json
import sys

import psycopg2

sys.path.append('../../config/')
from config import config


def _create_db():
    commands = (
        """CREATE TABLE "region" (
            id SERIAL PRIMARY KEY,
            name VARCHAR(64) NOT NULL);""",
        """CREATE TABLE "group" (
            id SERIAL PRIMARY KEY,
            name VARCHAR(256) NOT NULL);""",
        """CREATE TABLE "crimestatsocial" (
            id SERIAL PRIMARY KEY,
            reg_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            category VARCHAR(256) NOT NULL,
            gender CHAR(8) NOT NULL,
            value INTEGER NOT NULL,
            FOREIGN KEY (reg_id)
                REFERENCES "region" (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (group_id)
                REFERENCES "group" (id)
                ON UPDATE CASCADE ON DELETE CASCADE);""")
    conn = None
    try:
        params = config('../../config/database.ini')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database \'{}\' connection closed.'.format(
                params["database"]))


def _insert_table():
    reg = set()
    group = set()
    crimestatsocial = list()
    conn = None
    try:
        with open('../../dataset/crimestatsocial_final.json') as f:
            for line in f:
                j = json.loads(line)
                reg.add((int(j["reg_id"]), j["reg_name"]))
                group.add((j["group_id"], j["group_name"]))
                crimestatsocial.append(
                    (int(j["reg_id"]), j["year"], j["group_id"],
                    j["category"], j["gender"], j["value"], ))
        params = config('../../config/database.ini')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        for item in reg:
            cur.execute("""INSERT INTO "region" VALUES (%s, %s)""", item)
        for item in group:
            cur.execute("""INSERT INTO "group" VALUES (%s, %s)""", item)
        for item in crimestatsocial:
            cur.execute("""
                INSERT INTO
                    "crimestatsocial"
                    (reg_id, year, group_id, category, gender, value)
                VALUES (%s, %s, %s, %s, %s, %s)""", item)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database \'{}\' connection closed.'.format(
                params["database"]))


if __name__ == '__main__':
    _create_db()
    _insert_table()
