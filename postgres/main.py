import json
import psycopg2
import sys

sys.path.append('../')
from config import config


def create_db():
    commands = (
        """CREATE TABLE reg (
            id SERIAL PRIMARY KEY,
            name CHAR(64) NOT NULL);""",
        """CREATE TABLE "group" (
            id SERIAL PRIMARY KEY,
            name CHAR(64) NOT NULL);""",
        """CREATE TABLE category (name CHAR(64) PRIMARY KEY UNIQUE);""",
        """CREATE TABLE gender (name CHAR(64) PRIMARY KEY UNIQUE);""",
        """CREATE TABLE crimestatsocial (
            id SERIAL PRIMARY KEY,
            reg_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            category CHAR(64) NOT NULL,
            gender CHAR(64) NOT NULL,
            value INTEGER NOT NULL,
            FOREIGN KEY (reg_id)
                REFERENCES reg (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (group_id)
                REFERENCES "group" (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (category)
                REFERENCES category (name)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (gender)
                REFERENCES gender (name)
                ON UPDATE CASCADE ON DELETE CASCADE);""")
    conn = None
    try:
        params = config()
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
            print('Database connection closed.')


def insert_table():
    reg = set()
    group = set()
    category = set()
    gender = set()
    crimestatsocial = list()
    with open('../crimestatsocial_final.json') as f:
        for line in f:
            j = json.loads(line)
            reg.add((j["reg_id"], j["reg_name"]))
            group.add((j["group_id"], j["group_name"]))
            category.add((j["category"], ))
            gender.add((j["gender"], ))
            crimestatsocial.append(
                (j["reg_id"], j["year"], j["group_id"],
                j["category"], j["gender"], j["value"], ))
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        for item in reg:
            cur.execute("""INSERT INTO reg VALUES (%s, %s)""", item)
        for item in group:
            cur.execute("""INSERT INTO "group" VALUES (%s, %s)""", item)
        for item in category:
            cur.execute("""INSERT INTO category VALUES (%s)""", item)
        for item in gender:
            cur.execute("""INSERT INTO gender VALUES (%s)""", item)
        for item in crimestatsocial:
            cur.execute("""
                INSERT INTO
                    crimestatsocial
                    (reg_id, year, group_id, category, gender, value)
                VALUES (%s, %s, %s, %s, %s, %s)""", item)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    create_db()
    insert_table_reg()
