import json
import pickle

def pack_string(str):
    return json.dumps(str)

def pack_obj(obj):
    return pickle.dumps(obj)

def unpack_string(str):
    return json.loads(str)

def unpack_obj(obj):
    return json.loads(obj)
