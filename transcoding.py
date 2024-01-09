import json


def bytes2str(src: bytes, encoding='utf-8'):
    return src.decode(encoding)


def str2bytes(src: str, encoding='utf-8'):
    return src.encode(encoding)


def str2json(src: str):
    return json.loads(src)


def json2str(src: object):
    return json.dumps(src)


def bytes2json(src: bytes, encoding='utf-8'):
    return str2json(bytes2str(src, encoding))


def json2bytes(src: object, encoding='utf-8'):
    return str2bytes(json2str(src), encoding)
