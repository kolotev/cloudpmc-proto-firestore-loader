import base64
import copy
import json
import pprint
import re
from ast import literal_eval
from functools import wraps
from itertools import chain, islice
from pathlib import Path
from typing import Any, Dict, Iterator, List, Union

from . import zstd
from .logger import logger

pprinter = pprint.PrettyPrinter(indent=4, depth=2, width=100)


def bytes_to_str(v):
    return "".join([f"\\x{{{x:02x}}}" if not (32 < x < 128) else chr(x) for x in v])


ELLIPSIS_STR = " ..."


def deep_truncate(d: Dict[str, Any], max_size=56) -> Dict[str, Any]:
    """
    deep_truncate() helper function iterates over all elements
    of dictionary and truncate values to requested size
    """
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, dict):
                d.update({k: deep_truncate(v)})
            elif isinstance(v, bytes):
                new_v = bytes_to_str(v)
                if len(new_v) > max_size:
                    new_v = new_v[:max_size] + ELLIPSIS_STR
                d.update({k: new_v})
            elif isinstance(v, str) and len(v) > max_size:
                d.update({k: v[:max_size] + ELLIPSIS_STR})

    return d


def decode_b64_fields(d: Dict[str, Any]) -> None:
    """
    decode_b64_fields() decodes fields with name suffix ".b64" into bytes
    and renames the field to the same name with no suffix.
    """
    if isinstance(d, dict):
        for k in d.keys():
            if isinstance(k, str) and k.endswith(".b64") and isinstance(d[k], str):
                v = d.pop(k)
                new_k = k.rstrip(".b64")
                new_v = base64.b64decode(v)
                d.update({new_k: new_v})
            elif isinstance(d[k], dict):
                decode_b64_fields(d[k])


B64_RE = re.compile("^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$")


def b64_decode_zcompress_fields(d: Dict[str, Any], fields: List[str]) -> None:
    for f in fields:
        v = d.pop(f)
        if v is not None:
            if B64_RE.match(v):
                v = base64.b64decode(v)
            else:
                v = v.encode()
            v_zstd = zstd.compress(v)
            f_zstd = f + "_zstd"
            d.update({f_zstd: v_zstd})


def zdecompress_b64_encode_fields(d: Dict[str, Any], fields: List[str]) -> None:
    for f in fields:
        if f.endswith("_zstd"):
            v = d.pop(f, None)
            if v is not None:
                v = zstd.decompress(v)
                d[f.strip("_zstd")] = base64.b64encode(v).decode("ascii")


def b64_decode_zdecompress_fields(d: Dict[str, Any], fields: List[str]) -> None:
    for f in fields:
        if f.endswith("_zstd"):
            v = d.pop(f, None)
            if v is not None:
                v = base64.b64decode(v) if B64_RE.match(v) else v.encode()
                v = zstd.decompress(v)
                d[f.strip("_zstd")] = v.decode("utf-8")


def simplest_type(s: str) -> Union[str, int, float]:
    """
    Infer the value as python object. It is also support
    certain strings as booleans.
    """
    try:
        if s.lower() in ["y", "yes", "true", "on"]:
            s = "True"
        elif s.lower() in ["n", "no", "false", "off"]:
            s = "False"
        elif s.lower() in ["none", "null"]:
            s = "None"
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
                if click_ctx.parent.arg_debug:
                    logger.exception(e)
                logger.error(f"{e.__class__.__name__}: {e}")
                click_ctx.exit(error_code)

        return decorated

    return decorator


def log_debug_doc_dict(click_ctx, doc_dict: Dict[str, Any]) -> None:
    if click_ctx.parent.arg_debug:
        doc_for_display = deep_truncate(copy.deepcopy(doc_dict), 64)
        logger.debug("\n{}", pprinter.pformat(doc_for_display))


def save_json_doc_dict(click_ctx, doc_dict: Dict[str, Any], doc_id: str, dst: Path) -> None:
    json_path = dst / f"{doc_id}.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(doc_dict, f, ensure_ascii=False, indent=4, sort_keys=True)
    logger.info(f"document with doc_id={doc_id} was written into {json_path} file.")


def chunks(iterable: Iterator[Any], size: int) -> Iterator[chain]:
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))
