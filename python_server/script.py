import json
import hashlib
from rediscluster import RedisCluster


masters = [
    {"host": "172.19.0.3", "port": "6379"},
    {"host": "172.19.0.4", "port": "6379"},
    {"host": "172.19.0.6", "port": "6379"},
]


class DB:
    def __init__(self, startup_nodes=None):
        if startup_nodes is None:
            startup_nodes = masters
        self.redis_cluster = RedisCluster(
            startup_nodes=startup_nodes, decode_responses=True
        )
        self.score = 0
        self.find_key = 0
        self.end_key = 1

    @staticmethod
    def hash(key):
        if key.isnumeric():
            return key.rjust(20, "0")
        else:
            return key

    def set_value(self, table, partition_key, sort_key, value):
        sorted_set_key = f"{table}__sortedset__{{{partition_key}}}"
        hash_key = f"{table}__hashtable__{{{partition_key}}}"
        json_dumped_value = json.dumps(value)
        hashed_partition_key = self.hash(partition_key)
        hashed_sort_key = self.hash(sort_key)
        sort_value = f"{hashed_partition_key}|{hashed_sort_key}|{len(sort_key)}"
        status_1 = self.redis_cluster.zadd(sorted_set_key, {sort_value: self.score})
        status_2 = self.redis_cluster.hset(hash_key, sort_value, json_dumped_value)
        if status_1 and status_2:
            return True
        else:
            return False

    def get_value(self, table, partition_key, sort_key):
        sorted_set_key = f"{table}__sortedset__{{{partition_key}}}"
        hash_key = f"{table}__hashtable__{{{partition_key}}}"
        hashed_partition_key = self.hash(partition_key)
        hashed_sort_key = self.hash(sort_key)
        sort_value = f"{hashed_partition_key}|{hashed_sort_key}|{len(sort_key)}"
        try:
            return json.loads(self.redis_cluster.hget(hash_key, sort_value))
        except (ValueError, TypeError):
            return None

    def get_range(self, table, partition_key, sort_key_start, sort_key_end):
        sorted_set_key = f"{table}__sortedset__{{{partition_key}}}"
        hash_key = f"{table}__hashtable__{{{partition_key}}}"
        hashed_partition_key = self.hash(partition_key)
        hashed_sort_key_start = self.hash(sort_key_start)
        hashed_sort_key_end = self.hash(sort_key_end)
        start_value = f"[{hashed_partition_key}|{hashed_sort_key_start}"
        end_value = f"({hashed_partition_key}|{hashed_sort_key_end}"
        try:
            stored_values = self.redis_cluster.zrangebylex(
                sorted_set_key, start_value, end_value
            )
        except IndexError:
            return None
        if not stored_values:
            return []
        hash_values = self.redis_cluster.hmget(hash_key, stored_values)
        values = []
        for index, stored_value in enumerate(stored_values):
            splitted_values = stored_value.split("|")
            value = json.loads(hash_values[index])
            negative_sort_key_length = 0 - int(splitted_values[-1])
            sort_key = splitted_values[1][negative_sort_key_length:]
            values.append((value, sort_key))

        return values

    def keys(self):
        return self.redis_cluster.keys()

    def dbsize(self):
        return self.redis_cluster.dbsize()

    def reset(self):
        self.redis_cluster.flushall()
