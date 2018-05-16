import json
import riak
import sys


if __name__ == '__main__':
    client = riak.RiakClient(pb_port=8087)
    bucket = client.bucket('test4')
    with open('../crimestatsocial_final.json') as f:
        i = 1
        for line in f:
            key = bucket.new(str(i), data=json.loads(line))
            key.store()
            i += 1
    print(bucket.get('1').data)