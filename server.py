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
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key, client_id, request_id):
        self.key = key
        self.client_id = client_id
        self.request_id = request_id

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg

        # Your definitions here.
        self.db = {}
        self.last_requests = {}

    def is_duplicate(self, client_id, request_id):
        if client_id in self.last_requests:
            last_request_id, cached_reply = self.last_requests[client_id]
            if request_id <= last_request_id:
                return True, cached_reply
        
        return False, None

    def Get(self, args: GetArgs):
        with self.mu:
            duplicate, cached_reply = self.is_duplicate(args.client_id, args.request_id)
            if duplicate:
                return cached_reply

            key = args.key
            value = self.db.get(key, "")
            reply = GetReply(value)

            self.last_requests[args.client_id] = (args.request_id, reply)

            return reply

    def Put(self, args: PutAppendArgs):
        with self.mu:
            duplicate, cached_reply = self.is_duplicate(args.client_id, args.request_id)
            if duplicate:
                return cached_reply

            key = args.key
            value = args.value
            self.db[key] = value
            reply = PutAppendReply(None)

            self.last_requests[args.client_id] = (args.request_id, reply)

            return reply

    def Append(self, args: PutAppendArgs):
        with self.mu:
            duplicate, cached_reply = self.is_duplicate(args.client_id, args.request_id)
            if duplicate:
                return cached_reply

            key = args.key
            value_to_append = args.value
            old_value = self.db.get(key, "")

            self.db[key] = old_value + value_to_append

            reply = PutAppendReply(old_value)

            self.last_requests[args.client_id] = (args.request_id, reply)
            
            return reply
