import json
import pickle

def pack_obj(obj):
    return pickle.dumps(obj)

def unpack_obj(obj):
    return pickle.loads(obj)

def pack2json(obj):
    return json.dumps(obj)

def unpack_from_json(obj):
    return json.loads(obj)