import itertools
import math

from fakeredis import _msgs as msgs
from fakeredis._commands import command, Key, Hash, Int, Float
from fakeredis._helpers import SimpleError, OK


class HashCommandsMixin:
    @command((Key(Hash), bytes), (bytes,))
    def hdel(self, key, *fields):
        h = key.value
        rem = 0
        for field in fields:
            if field in h:
                del h[field]
                key.updated()
                rem += 1
        return rem

    @command((Key(Hash), bytes))
    def hexists(self, key, field):
        return int(field in key.value)

    @command((Key(Hash), bytes))
    def hget(self, key, field):
        return key.value.get(field)

    @command((Key(Hash),))
    def hgetall(self, key):
        return list(itertools.chain(*key.value.items()))

    @command((Key(Hash), bytes, Int))
    def hincrby(self, key, field, amount):
        c = Int.decode(key.value.get(field, b'0')) + amount
        key.value[field] = self._encodeint(c)
        key.updated()
        return c

    @command((Key(Hash), bytes, bytes))
    def hincrbyfloat(self, key, field, amount):
        c = Float.decode(key.value.get(field, b'0')) + Float.decode(amount)
        if not math.isfinite(c):
            raise SimpleError(msgs.NONFINITE_MSG)
        encoded = self._encodefloat(c, True)
        key.value[field] = encoded
        key.updated()
        return encoded

    @command((Key(Hash),))
    def hkeys(self, key):
        return list(key.value.keys())

    @command((Key(Hash),))
    def hlen(self, key):
        return len(key.value)

    @command((Key(Hash), bytes), (bytes,))
    def hmget(self, key, *fields):
        return [key.value.get(field) for field in fields]

    @command((Key(Hash), bytes, bytes), (bytes, bytes))
    def hmset(self, key, *args):
        self.hset(key, *args)
        return OK

    @command((Key(Hash), Int,), (bytes, bytes))
    def hscan(self, key, cursor, *args):
        cursor = int(cursor)
        # When starting a new scan, saves snapshot of the keys
        if cursor == 0:
            self._scan_snapshot['hscan'] = list(key.value)
        next_cursor, keys = self._scan(self._scan_snapshot['hscan'], cursor, *args)
        # When scan is finished remove the snapshot
        if next_cursor == 0:
            del self._scan_snapshot['hscan']
        items = []
        for k in keys:
            if k in key.value:
                items.append(k)
                items.append(key.value[k])
        return [str(next_cursor).encode(), items]

    @command((Key(Hash), bytes, bytes), (bytes, bytes))
    def hset(self, key, *args):
        h = key.value
        keys_count = len(h.keys())
        h.update(dict(zip(*[iter(args)] * 2)))  # https://stackoverflow.com/a/12739974/1056460
        created = len(h.keys()) - keys_count

        key.updated()
        return created

    @command((Key(Hash), bytes, bytes))
    def hsetnx(self, key, field, value):
        if field in key.value:
            return 0
        return self.hset(key, field, value)

    @command((Key(Hash), bytes))
    def hstrlen(self, key, field):
        return len(key.value.get(field, b''))

    @command((Key(Hash),))
    def hvals(self, key):
        return list(key.value.values())
