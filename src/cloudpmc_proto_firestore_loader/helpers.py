import base64
import pprint
from typing import Any, Dict

pp = pprint.PrettyPrinter(indent=4, depth=2, width=100)


def bytes_to_str(v):
    return "".join([f"\\x{{{x:02x}}}" if not (32 < x < 128) else chr(x) for x in v])


ELLIPSIS_STR = " ..."


def deep_truncate(o, max_size=56):
    """
    deep_truncate() helper function iterates over all elements
    of dictionary and truncate values to requested size
    """
    if isinstance(o, dict):
        for k, v in o.items():
            if isinstance(v, dict):
                o.update({k: deep_truncate(v)})
            elif isinstance(v, bytes):
                new_v = bytes_to_str(v)
                if len(new_v) > max_size:
                    new_v = new_v[:max_size] + ELLIPSIS_STR
                o.update({k: new_v})
            elif isinstance(v, str) and len(v) > max_size:
                o.update({k: v[:max_size] + ELLIPSIS_STR})

    return o


def decode_b64_fields(o: Dict[str, Any]) -> Dict[str, Any]:
    """ """
    if isinstance(o, dict):
        for k in list(o):
            if isinstance(k, str) and k.endswith(".b64") and isinstance(o[k], str):
                v = o.pop(k)
                new_k = k.rstrip(".b64")
                new_v = base64.b64decode(v)
                o.update({new_k: new_v})
            elif isinstance(o[k], dict):
                o.update({k: decode_b64_fields(o[k])})

    return o
