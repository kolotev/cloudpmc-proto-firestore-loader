import base64
import pprint
from ast import literal_eval
from typing import Any, Dict, Union

pprinter = pprint.PrettyPrinter(indent=4, depth=2, width=100)


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
    """
    decode_b64_fields() decodes fields with name suffix ".b64" into bytes
    and renames the field to the same name with no suffix.
    """
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


def simplest_type(s: str) -> Union[str, int, float]:
    """
    Infer the value as python object. It is also support
    certain strings as booleans.
    """
    try:
        if s.lower() in ["y", "yes", "true", "on"]:
            s = "True"
        if s.lower() in ["n", "no", "false", "off"]:
            s = "False"
        return literal_eval(s)
    except Exception:
        return s


def docstring_with_params(*args, **kwargs):
    """
    The decorator function to supply arguments to docstring in any
    object, before it would be used as o.__doc__ anywhere.
    """

    def decorated(o):
        o.__doc__ = o.__doc__.format(*args, **kwargs)
        return o

    return decorated
