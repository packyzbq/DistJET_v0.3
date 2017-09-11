
import pickle

def pack_obj(obj):
    return pickle.dumps(obj)

def unpack_obj(obj):
    return pickle.loads(obj)
