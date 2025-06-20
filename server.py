import logging
import threading
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value, client_id, request_id):
        self.key = key
        self.value = value
        self.client_id = client_id
        self.request_id = request_id

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value, error):
        self.value = value
        self.error = error

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key, client_id, request_id):
        self.key = key
        self.client_id = client_id
        self.request_id = request_id

class GetReply:
    # Add definitions here if needed
    def __init__(self, value, error):
        self.value = value
        self.error = error

def primary_of(shard, nshards):
    return shard % nshards

class KVServer:
    def __init__(self, cfg,  srvid):
        self.mu = threading.Lock()
        self.cfg = cfg

        self.id = srvid
        self.db = {}
        self.last_requests = {}

        self.nshards = self.cfg.nservers

    def key_to_shard(self, key):
        try:
            return int(key) % self.nshards
        except (ValueError, TypeError):
            return hash(key) % self.nshards

    def responsible_for(self, shard):
        return (self.id - shard) % self.nshards < self.cfg.nreplicas

    def _duplicate(self, client_id, request_id):
        if client_id in self.last_requests and request_id <= self.last_requests[client_id][0]:
            return True, self.last_requests[client_id][1]
        
        return False, None
    
    def _replicate_put(self, shard, key, value, client_id, request_id, reply):
        # send the write to the R-1 follower replicas
        for r in range(1, self.cfg.nreplicas):
            server_id = (shard + r) % self.nshards
            replica = self.cfg.kvservers[server_id]
            with replica.mu:
                replica.db[key] = value
                replica.last_requests[client_id] = (request_id, reply)

    def _replicate_append(self, shard, key, delta, client_id, request_id, reply):
        for r in range(1, self.cfg.nreplicas):
            server_id = (shard + r) % self.nshards
            replica = self.cfg.kvservers[server_id]
            with replica.mu:
                replica.db[key] = replica.db.get(key, "") + delta
                replica.last_requests[client_id] = (request_id, reply)

    def Get(self, args: GetArgs):
        shard = self.key_to_shard(args.key)

        if not self.responsible_for(shard):
            return GetReply("", "Wrong Group")

        duplicate, cached = self._duplicate(args.client_id, args.request_id)
        if duplicate:
            return cached

        with self.mu:
            key = args.key
            value = self.db.get(key, "")
            reply = GetReply(value, "")

            self.last_requests[args.client_id] = (args.request_id, reply)

            return reply

    def Put(self, args: PutAppendArgs):
        shard = self.key_to_shard(args.key)
        if not self.responsible_for(shard):
            return PutAppendReply(None, "Wrong Group")

        if self.id != primary_of(shard, self.nshards):
            return self.cfg.kvservers[primary_of(shard, self.nshards)].Put(args)

        duplicate, cached = self._duplicate(args.client_id, args.request_id)
        if duplicate:
            return cached

        with self.mu:
            self.db[args.key] = args.value
            reply = PutAppendReply(None, "")
            self.last_requests[args.client_id] = (args.request_id, reply)

        self._replicate_put(shard, args.key, args.value, args.client_id, args.request_id, reply)

        return reply

    def Append(self, args: PutAppendArgs):
        shard = self.key_to_shard(args.key)
        if not self.responsible_for(shard):
            return PutAppendReply(None, "Wrong Group")

        if self.id != primary_of(shard, self.nshards):
            return self.cfg.kvservers[primary_of(shard, self.nshards)].Append(args)

        duplicate, cached = self._duplicate(args.client_id, args.request_id)
        if duplicate:
            return cached

        with self.mu:
            old = self.db.get(args.key, "")
            self.db[args.key] = old + args.value
            reply = PutAppendReply(old, "")
            self.last_requests[args.client_id] = (args.request_id, reply)

        self._replicate_append(shard, args.key, args.value, args.client_id, args.request_id, reply)
        return reply
