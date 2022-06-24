import base64
import pprint
from ast import literal_eval
from functools import wraps
from typing import Any, Dict, List, Union
from . import zstd
from .logger import logger

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


def decode_b64_fields(o: Dict[str, Any]) -> None:
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


def decode_b64_compress_fields(o: Dict[str, Any], fields: List[str]) -> None:
    for f in fields:
        v = o.pop(f)
        if v is not None:
            v = base64.b64decode(v)
            v_zstd = zstd.compress(v)
            f_zstd = f + "_zstd"
def zdecompress_b64_encode_fields(d: Dict[str, Any], fields: List[str]) -> None:
    for f in fields:
        if f.endswith("_zstd"):
            v = d.pop(f)
            if v is not None:
                v = zstd.decompress(v)
                d[f.strip("_zstd")] = base64.b64encode(v).decode("ascii")


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


def cli_try_except(error_code):
    """
    The decorator function to decorate cli commands with try except block.
    """

    def decorator(func):
        @wraps(func)
        def decorated(click_ctx, *args, **kwargs):
            try:
                return func(click_ctx, *args, **kwargs)
            except Exception as e:
                if click_ctx.parent._debug:
                    logger.exception(e)
                logger.error(f"{e} type(e)={type(e)}")
                click_ctx.exit(error_code)

        return decorated

    return decorator


def log_debug_doc_dict(click_ctx, doc_dict: Dict[str, Any]) -> None:
    if click_ctx.parent._debug:
        doc_for_display = deep_truncate(copy.deepcopy(doc_dict), 64)
        logger.debug("\n{}", pprinter.pformat(doc_for_display))
