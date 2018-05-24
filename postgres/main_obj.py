import json
import sys

import psycopg2

sys.path.append('../config/')
from config import config


def _create_db():
    commands = (
        """
        CREATE TABLE "region" (
            id SERIAL PRIMARY KEY,
            name VARCHAR(64) NOT NULL);""",
        """
        CREATE TABLE "group" (
            id SERIAL PRIMARY KEY,
            name VARCHAR(256) NOT NULL);""",
        """
        CREATE TABLE "crimestatsocial" (
            id SERIAL PRIMARY KEY,
            data json NOT NULL,
            reg_id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            FOREIGN KEY (reg_id)
                REFERENCES "region" (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (group_id)
                REFERENCES "group" (id)
                ON UPDATE CASCADE ON DELETE CASCADE);""")
    conn = None
    try:
        params = config(section="obj_postgres")
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
    conn = None
    try:
        region = set()
        group = set()
        params = config(section="obj_postgres")
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        with open('../dataset/crimestatsocial_final.json') as f:
            for line in f:
                j = json.loads(line)

                reg_id = j.pop(u'reg_id')
                reg_name = j.pop(u'reg_name')
                if reg_id not in region:
                    region.add(reg_id)
                    cur.execute(
                        """INSERT INTO "region" VALUES (%s, %s)""",
                        (reg_id, reg_name, ))

                group_id = j.pop(u'group_id')
                group_name = j.pop(u'group_name')
                if group_id not in group:
                    group.add(group_id)
                    cur.execute(
                        """INSERT INTO "group" VALUES (%s, %s)""",
                        (group_id, group_name, ))

                cur.execute(
                    """
                    INSERT INTO "crimestatsocial" (data, reg_id, group_id)
                    VALUES (%s, %s, %s)""",
                    (json.dumps(j), reg_id, group_id, ))
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
