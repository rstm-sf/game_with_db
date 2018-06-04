import json
import sys

import redis


if __name__ == '__main__':
    pool = redis.ConnectionPool(decode_responses=True)
    r = redis.StrictRedis(connection_pool=pool)
    with open('../../dataset/crimestatsocial_final.json') as f:
        with r.pipeline() as pipe:
            i = 0
            for line in f:
                pipe.hmset('css:{}'.format(i), json.loads(line))
                i += 1
            pipe.execute()
    print(r.hgetall('0'))
