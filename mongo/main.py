import json
import sys

from pymongo import MongoClient

sys.path.append('../')
from config import config


def conn_db():
    params = config(section='mongodb')
    client = MongoClient(params["host"], int(params["port"]))
    return client[params["database"]]


def create_insert_db():
    db = conn_db()
    table = db.table
    for line in open('../crimestatsocial_final.json', 'r'):
        table.insert(json.loads(line))


def count_doc():
    return conn_db().table.count()


if __name__ == '__main__':
    create_insert_db()
    print(count_doc())
