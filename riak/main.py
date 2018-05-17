import json
import sys

import riak


if __name__ == '__main__':
    client = riak.RiakClient(pb_port=8087)
    bucket = client.bucket('crimestatsocial')
    with open('../dataset/crimestatsocial_final.json') as f:
        i = 1
        for line in f:
            key = bucket.new(str(i), data=json.loads(line))
            key.store()
            i += 1
    print(bucket.get('1').data)
