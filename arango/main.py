import json
import sys

from arango import ArangoClient

sys.path.append('../')
from config import config


def _create_insert_graph():
    client = ArangoClient()

    params = config(section="arangodb")
    db = client.db(*tuple(params.values()))

    graph = db.create_graph('crimestatsocial')
    region = graph.create_vertex_collection('region')
    group = graph.create_vertex_collection('group')
    css = graph.create_vertex_collection('css')
    css_region = graph.create_edge_definition(
        edge_collection='css_region',
        from_vertex_collections=['css'],
        to_vertex_collections=['region']
    )
    css_group = graph.create_edge_definition(
        edge_collection='css_group',
        from_vertex_collections=['css'],
        to_vertex_collections=['group']
    )

    with open('../crimestatsocial_final.json') as f:
        i = 1
        for line in f:
            data = json.loads(line)
            if not region.has(data[u"reg_id"]):
                region.insert(
                    {'_key': data[u"reg_id"], 'name': data[u"reg_name"]})
            if not group.has(str(data[u"group_id"])):
                group.insert({
                    '_key': str(data[u"group_id"]),
                    'name': data[u"group_name"]
                })
            css.insert({
                '_key': str(i),
                'year': data[u"year"],
                'category': data[u"category"],
                'gender': data[u"gender"],
                'value': data[u"value"],
            })
            css_region.insert({
                '_key': '{}-{}'.format(i, data[u"reg_id"]),
                '_from': 'css/{}'.format(i),
                '_to': 'region/{}'.format(data[u"reg_id"])
            })
            css_group.insert({
                '_key': '{}-{}'.format(i, data[u"group_id"]),
                '_from': 'css/{}'.format(i),
                '_to': 'group/{}'.format(data[u"group_id"])
            })
            i += 1

    print(css.get('1'))


if __name__ == '__main__':
    _create_insert_graph()
