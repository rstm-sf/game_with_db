import json
import sys

from pymongo import MongoClient

sys.path.append('../config/')
from config import config


def init():
    _create_insert_db()
    print(_find_one())


def conn_db():
    params = config(section='mongodb')
    client = MongoClient(params["host"], int(params["port"]))
    return client[params["database"]]


def _create_insert_db():
    db = conn_db()
    table = db.table
    with open('../dataset/crimestatsocial_final.json') as f:
        for line in f:
            data = json.loads(line)
            table.insert_one(_refactor_json(data))


def _refactor_json(data):
    return {
        u"region": {
            u"id": data[u"reg_id"],
            u"name": data[u"reg_name"],
        },
        u"year": data["year"],
        u"group": {
            u"id": data[u"group_id"],
            u"name": data[u"group_name"],
        },
        u"category": data[u"category"],
        u"gender": data[u"gender"],
        u"value": data[u"value"],
    }


def _count_doc():
    return conn_db().table.count()


def _find_one():
    return conn_db().table.find_one()


def _drop_table():
    conn_db().table.drop()
    print("Drop table!")


if __name__ == '__main__':
    init()
