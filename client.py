import random
import threading
import time
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg

        self.client_id = nrand()
        self.request_id = 0

        self.nshards = self.cfg.nservers

        self.shard_leaders = [i for i in range(self.nshards)]

    def key_to_shard(self, key):
        try:
            return int(key) % self.nshards
        except (ValueError, TypeError):
            return hash(key) % self.nshards

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def get(self, key: str) -> str:
        self.request_id += 1

        args = GetArgs(key, self.client_id, self.request_id)

        shard = self.key_to_shard(key)

        s_i = self.shard_leaders[shard]
        
        while True:
            server_to_try = self.servers[s_i]
            try:
                reply = server_to_try.call("KVServer.Get", args)
                if reply.error is None or reply.error == "":
                    self.shard_leaders[shard] = s_i
                    return reply.value
                elif reply.error == "Wrong Group":
                    pass

            except TimeoutError:
                pass

            s_i = (s_i + 1) % len(self.servers)
            time.sleep(0.1)

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str) -> str:
        self.request_id += 1

        args = PutAppendArgs(key, value, self.client_id, self.request_id)

        shard = self.key_to_shard(key)

        s_i = self.shard_leaders[shard]
        
        while True:
            server_to_try = self.servers[s_i]
            try:
                reply = server_to_try.call("KVServer." + op, args)
                if reply.error is None or reply.error == "":
                    self.shard_leaders[shard] = s_i
                    return reply.value
                elif reply.error == "Wrong Group":
                    pass

            except TimeoutError:
                pass
            
            s_i = (s_i + 1) % len(self.servers)
            time.sleep(0.1)

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
