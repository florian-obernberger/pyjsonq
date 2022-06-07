__all__ = []

from typing import Any, Optional, Tuple

def removeprefix(prefix: str, string: str) -> str:
    if string.startswith(prefix):
        return string[1:]
    else:
        return string

def removesuffix(suffix: str, string: str) -> str:
    if string.startswith(suffix):
        return string[:-1]
    else:
        return string

def parseIndex(p: str) -> Optional[int]:
    try:
        return int(p)
    except ValueError:
        return None


def makeAlias(inp: str, separator: str) -> Tuple[str, str]:
    ALIAS: str = " as "
    inp = inp.replace(" As ", ALIAS)
    inp = inp.replace(" AS ", ALIAS)

    if ALIAS in inp:
        strings = inp.split(ALIAS)
        return strings[0].strip(), strings[1].strip()

    if separator in inp:
        strings = inp.split(separator)
        return inp, strings[-1]

    return inp, inp


def isIndex(inp: str) -> bool:
    return inp.startswith("[") and inp.endswith("]")


def getIndex(inp: str) -> Optional[int]:
    if not isIndex(inp):
        return None

    ind_str: str = removeprefix("[", removesuffix("]", inp))
    return parseIndex(ind_str)


def getNestedValue(inp: Any, node: str, separator: str) -> Any:
    parts: list[str] = node.split(separator)
    for n in parts:
        if isIndex(n):
            if isinstance(inp, list):
                _inp: list[Any] = inp
                if index := getIndex(n) is None:
                    return None
                return _inp[index]

        else:
            valid: bool = False
            if isinstance(inp, dict):
                inp = inp[n]
                valid = True

            if not valid:
                return None

    return inp


def deleteNestedValue(inp: Any, node: str, separator: str) -> Any:
    parts: list[str] = node.split(separator)
    for n in parts:
        if isIndex(n):
            if isinstance(inp, list):
                l_inp: list[Any] = inp
                if index := getIndex(n) is None:
                    return None
                del l_inp[index]
                return l_inp

        else:
            valid: bool = False
            if isinstance(inp, dict):
                d_inp: dict[str, Any] = inp
                del d_inp[n]
                inp = d_inp
                valid = True

            if not valid:
                return None

    return inp
