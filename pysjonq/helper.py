from typing import Any


def parseIndex(p: str) -> int | None:
    try:
        return int(p)
    except ValueError:
        return None


def makeAlias(inp: str, separator: str) -> tuple[str, str]:
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


def getIndex(inp: str) -> int | None:
    if not isIndex(inp):
        return None

    ind_str: str = inp.removeprefix("[").removesuffix("]")
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
