from ast import literal_eval
from typing import Any, Optional

from click.types import ParamType


class PyEvalType(ParamType):
    name = "infer"

    def convert(
        self,
        value: Any,
        param: Optional["Parameter"],
        ctx: Optional["Context"],
    ) -> Any:

        val = PyEvalType._simplest_type(value)
        return val

    def __repr__(self) -> str:
        return "INFER"

    @staticmethod
    def _simplest_type(s):
        try:
            if s.lower() in ["y", "yes", "true", "on"]:
                s = "True"
            if s.lower() in ["n", "no", "false", "off"]:
                s = "False"
            return literal_eval(s)
        except:
            return s
