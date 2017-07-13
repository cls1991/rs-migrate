# coding: utf8


import redis
import gevent

from util.common import time_now_str


class Migrate:
    def __init__(self, config_s, config_t):
        self._redis_s = redis.Redis(config_s['host'], config_s['port'], config_s['db'])
        self._redis_t = redis.Redis(config_t['host'], config_t['port'], config_t['db'])
        self._cursor = 0
        self._run = False

    def migrate(self):
        if self._run and self._cursor == 0:
            print("no key in database")
            return
        print("[%s]migrate: %s" % (time_now_str(format_str='%Y-%m-%d %H:%M:%S'), self._cursor))
        t = self._redis_s.scan(self._cursor, count=200)
        kts = []
        pipe_s = self._redis_s.pipeline()
        for k in t[1]:
            kt = self._redis_s.type(k)
            if kt == 'hash':
                pipe_s.hgetall(k)
            elif kt == 'list':
                pipe_s.lrange(k, 0, -1)
            elif kt == 'set':
                pipe_s.smembers(k)
            elif kt == 'string':
                pipe_s.get(k)
            kts.append(kt)
        v = pipe_s.execute()
        pipe_t = self._redis_t.pipeline()
        for i in xrange(0, len(kts)):
            if kts[i] == 'hash':
                pipe_t.hmset(t[1][i], v[i])
            elif kts[i] == 'list':
                pipe_t.rpush(t[1][i], v[i])
            elif kts[i] == 'set':
                pipe_t.sadd(t[1][i], v[i])
            elif kts[i] == 'string':
                pipe_t.set(t[1][i], v[i])
        pipe_t.execute()
        self._cursor = t[0]
        self._run = True


def run():
    m = Migrate(config_s={'host': '127.0.0.1', 'port': 6379, 'db': 2},
                config_t={'host': '127.0.0.1', 'port': 6379, 'db': 3})
    loop = gevent.get_hub().loop
    timer = loop.timer(0.0, 15 * 60)
    timer.start(m.migrate)
    loop.run()
