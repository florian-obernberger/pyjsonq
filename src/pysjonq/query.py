__all__ = ["QueryOperator", "QueryFunc"]

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, TypeAlias


class QueryOperator(Enum):
    """Enum of all query operators.
    """

    eq = "="
    eqEng = "eq"
    notEq = "!="
    notEqEng = "neq"
    notEqAnother = "<>"
    gt = ">"
    gtEng = "gt"
    lt = "<"
    ltEng = "lt"
    gtE = ">="
    gtEEng = "gte"
    ltE = "<="
    ltEEng = "lte"
    strictContains = "strictContains"
    contains = "contains"
    notStrictContains = "notContains"
    notContains = "notStrictContains"
    endsWith = "endsWith"
    startsWith = "startsWith"
    isIn = "in"
    notIn = "notIn"
    holds = "holds"
    notHolds = "notHolds"
    lenEq = "leneq"
    lenNotEq = "lenneq"
    lenGt = "lengt"
    lenGte = "lengte"
    lenLt = "lenlt"
    lenLte = "lenlte"


QueryFunc: TypeAlias = Callable[[Any, Any], bool]
QueryDict: TypeAlias = dict[str | QueryOperator, QueryFunc]


@dataclass
class Query:
    key: str
    operator: str
    value: Any


def defaultQueries() -> QueryDict:
    return {
        QueryOperator.eq.value: eq,
        QueryOperator.eqEng.value: eq,
        QueryOperator.notEq.value: neq,
        QueryOperator.notEqEng.value: neq,
        QueryOperator.notEqAnother.value: neq,
        QueryOperator.gt.value: gt,
        QueryOperator.gtEng.value: gt,
        QueryOperator.lt.value: lt,
        QueryOperator.ltEng.value: lt,
        QueryOperator.gtE.value: gte,
        QueryOperator.gtEEng.value: gte,
        QueryOperator.ltE.value: lte,
        QueryOperator.ltEEng.value: lte,
        QueryOperator.strictContains.value: strStrictContains,
        QueryOperator.contains.value: strContains,
        QueryOperator.notStrictContains.value: notStrStrictContains,
        QueryOperator.notContains.value: notStrContains,
        QueryOperator.startsWith.value: strStartsWith,
        QueryOperator.endsWith.value: strEndsWith,
        QueryOperator.isIn.value: isIn,
        QueryOperator.notIn.value: notIn,
        QueryOperator.holds.value: holds,
        QueryOperator.notHolds.value: notHolds,
        QueryOperator.lenEq.value: lenEq,
        QueryOperator.lenNotEq.value: lenNotEq,
        QueryOperator.lenGt.value: lenGt,
        QueryOperator.lenGte.value: lenGte,
        QueryOperator.lenLt.value: lenLt,
        QueryOperator.lenLte.value: lenLte,
    }


def eq(x: Any, y: Any) -> bool:
    return x == y


def neq(x: Any, y: Any) -> bool:
    b: bool = eq(x, y)
    return not b


def gt(x: Any, y: Any) -> bool:
    try:
        xv: float = float(x)
        xy: float = float(y)

        return xv > xy
    except ValueError:
        return False


def lt(x: Any, y: Any) -> bool:
    try:
        xv: float = float(x)
        xy: float = float(y)

        return xv < xy
    except ValueError:
        return False


def gte(x: Any, y: Any) -> bool:
    try:
        xv: float = float(x)
        xy: float = float(y)

        return xv >= xy
    except ValueError:
        return False


def lte(x: Any, y: Any) -> bool:
    try:
        xv: float = float(x)
        xy: float = float(y)

        return xv <= xy
    except ValueError:
        return False


def strStrictContains(x: Any, y: Any) -> bool:
    if not isinstance(x, str) or not isinstance(y, str):
        return False

    return x in y


def strContains(x: Any, y: Any) -> bool:
    if not isinstance(x, str) or not isinstance(y, str):
        return False

    return x.lower() in y.lower()


def notStrStrictContains(x: Any, y: Any) -> bool:
    return not strStrictContains(x, y)


def notStrContains(x: Any, y: Any) -> bool:
    return not strContains(x, y)


def strStartsWith(x: Any, y: Any) -> bool:
    if not isinstance(x, str) or not isinstance(y, str):
        return False

    return x.startswith(y)


def strEndsWith(x: Any, y: Any) -> bool:
    if not isinstance(x, str) or not isinstance(y, str):
        return False

    return x.endswith(y)


def isIn(x: Any, y: Any) -> bool:
    if isinstance(y, list):
        y_list: list[Any] = y
        return x in y_list

    return False


def notIn(x: Any, y: Any) -> bool:
    return not isIn(x, y)


def holds(x: Any, y: Any) -> bool:
    return isIn(y, x)


def notHolds(x: Any, y: Any) -> bool:
    return not holds(x, y)


def lenEq(x: Any, y: Any) -> bool:
    if not isinstance(y, int):
        return False
    try:
        return len(x) == y
    except TypeError:
        return False


def lenNotEq(x: Any, y: Any) -> bool:
    if not isinstance(y, int):
        return False
    try:
        return len(x) != y
    except TypeError:
        return False


def lenGt(x: Any, y: Any) -> bool:
    if not isinstance(y, int):
        return False
    try:
        return len(x) > y
    except TypeError:
        return False


def lenGte(x: Any, y: Any) -> bool:
    if not isinstance(y, int):
        return False
    try:
        return len(x) >= y
    except TypeError:
        return False


def lenLt(x: Any, y: Any) -> bool:
    if not isinstance(y, int):
        return False
    try:
        return len(x) < y
    except TypeError:
        return False


def lenLte(x: Any, y: Any) -> bool:
    if not isinstance(y, int):
        return False
    try:
        return len(x) <= y
    except TypeError:
        return False
