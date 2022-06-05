from __future__ import annotations
from copy import deepcopy
from operator import itemgetter

__all__ = ["JsonQuery"]

import json
from typing import Any, Callable, TypeVar

from helper import getNestedValue, deleteNestedValue, makeAlias
from query import Query, QueryDict, defaultQueries, QueryFunc, QueryOperator

DEFAULT_SEPARATOR = "."

__T = TypeVar("__T")


class JsonQuery:

    def __init__(self, json_string: str, separator: str = DEFAULT_SEPARATOR) -> None:
        self.__separator: str = separator
        self.__root_json_content: Any = json.loads(json_string)
        self.__json_content: Any = self.__root_json_content

        self.__query_map: QueryDict = defaultQueries()
        self.__query_index: int = 0

        self.__offset_records: int = 0
        self.__limit_records: int = 0

        self.__queries: list[list[Query]] = []
        self.__dropped_properties: list[str] = []
        self.__attributes: list[str] = []
        self.__distinct_property: str = ""

    @classmethod
    def File(cls,
             file_path: str,
             encoding: str = "utf-8",
             separator: str = DEFAULT_SEPARATOR) -> JsonQuery:
        with open(file_path, encoding=encoding) as file:
            return cls(file.read(), separator=separator)

    def From(self, node: str) -> JsonQuery:
        value: Any = getNestedValue(self.__json_content, node, self.__separator)
        if value is None:
            raise ValueError()

        self.__json_content = value
        return self

    def Select(self, *properties: str) -> JsonQuery:
        self.__attributes.extend(properties)
        return self

    def Get(self) -> Any:
        self.__prepare()
        if self.__offset_records != 0:
            self.__offset()

        if self.__limit_records != 0:
            self.__limit()

        if len(self.__dropped_properties) != 0:
            self.__drop()

        return self.__json_content

    def Where(self, key: str, cond: str | QueryOperator, val: Any) -> JsonQuery:
        query: Query = Query(
            key=key,
            operator=cond if isinstance(cond, str) else cond.value,
            value=val,
        )
        if self.__query_index == 0 and len(self.__queries) == 0:
            self.__queries.append([query])
        else:
            self.__queries[self.__query_index].append(query)

        return self

    def OrWhere(self, key: str, cond: str | QueryOperator, val: Any) -> JsonQuery:
        self.__query_index += 1
        qquery: list[Query] = [
            Query(
                key=key,
                operator=cond if isinstance(cond, str) else cond.value,
                value=val,
            )
        ]
        self.__queries.append(qquery)
        return self

    def WhereEqual(self, key: str, val: Any) -> JsonQuery:
        return self.Where(key, QueryOperator.eq, val)

    def WhereNotEqual(self, key: str, val: Any) -> JsonQuery:
        return self.Where(key, QueryOperator.notEq, val)

    def WhereNone(self, key: str) -> JsonQuery:
        return self.Where(key, QueryOperator.eq, None)

    def WhereNotNone(self, key: str) -> JsonQuery:
        return self.Where(key, QueryOperator.notEq, None)

    def WhereIn(self, key: str, val: list[Any]) -> JsonQuery:
        return self.Where(key, QueryOperator.isIn, val)

    def WhereNotIn(self, key: str, val: list[Any]) -> JsonQuery:
        return self.Where(key, QueryOperator.notIn, val)

    def WhereHolds(self, key: str, val: Any) -> JsonQuery:
        return self.Where(key, QueryOperator.holds, val)

    def WhereNotHolds(self, key: str, val: Any) -> JsonQuery:
        return self.Where(key, QueryOperator.notHolds, val)

    def WhereStartsWith(self, key: str, val: str) -> JsonQuery:
        return self.Where(key, QueryOperator.startsWith, val)

    def WhereEndsWith(self, key: str, val: str) -> JsonQuery:
        return self.Where(key, QueryOperator.endsWith, val)

    def WhereContains(self, key: str, val: str) -> JsonQuery:
        return self.Where(key, QueryOperator.contains, val)

    def WhereStrictContains(self, key: str, val: str) -> JsonQuery:
        return self.Where(key, QueryOperator.strictContains, val)

    def WhereNotContains(self, key: str, val: str) -> JsonQuery:
        return self.Where(key, QueryOperator.notContains, val)

    def WhereNotStrictContains(self, key: str, val: str) -> JsonQuery:
        return self.Where(key, QueryOperator.notStrictContains, val)

    def WhereLenEqual(self, key: str, val: int) -> JsonQuery:
        return self.Where(key, QueryOperator.lenEq, val)

    def WhereLenNotEqual(self, key: str, val: int) -> JsonQuery:
        return self.Where(key, QueryOperator.lenNotEq, val)

    def Find(self, path: str) -> Any:
        return self.From(path).Get()

    def Offset(self, offset: int) -> JsonQuery:
        self.__offset_records = offset
        return self

    def Limit(self, limit: int) -> JsonQuery:
        self.__limit_records = limit
        return self

    def Sum(self, *properties: str) -> float:
        floats: list[float] = self.__getAggregationValues(*properties)
        return sum(floats)

    def Count(self) -> int | None:
        self.__prepare()

        if isinstance(self.__json_content, list) or isinstance(self.__json_content, dict):
            json_content: list[Any] | dict[str, Any] = self.__json_content
            return len(json_content)
        else:
            return

    def Min(self, *properties: str) -> float:
        floats: list[float] = self.__getAggregationValues(*properties)
        return min(floats)

    def Max(self, *properties: str) -> float:
        floats: list[float] = self.__getAggregationValues(*properties)
        return max(floats)

    def Avg(self, *properties: str) -> float:
        floats: list[float] = self.__getAggregationValues(*properties)
        return sum(floats) / len(floats)

    def First(self) -> Any:
        self.__prepare()
        if isinstance(self.__json_content, list):
            json_content: list[Any] = self.__json_content
            return json_content[0]
        else:
            return None

    def Last(self) -> Any:
        self.__prepare()
        if isinstance(self.__json_content, list):
            json_content: list[Any] = self.__json_content
            return json_content[-1]
        else:
            return None

    def Nth(self, index: int) -> Any:
        self.__prepare()
        if isinstance(self.__json_content, list):
            json_content: list[Any] = self.__json_content
            return json_content[index]
        else:
            return None

    def GroupBy(self, attr: str) -> JsonQuery:
        self.__prepare()
        dt: dict[str, list[Any]] = {}
        if isinstance(self.__json_content, list):
            json_list: list[Any] = self.__json_content
            for a in json_list:
                if isinstance(a, dict):
                    value = getNestedValue(a, attr, self.__separator)
                    if value is None:
                        # TODO: error
                        return self
                    if dt.get(str(value)) is None:
                        dt[str(value)] = [a]
                    else:
                        dt[str(value)].append(a)

        self.__json_content = dt
        return self

    def Distinct(self, attr: str) -> JsonQuery:
        self.__distinct_property = attr
        return self

    def Sort(self, key: Callable[[Any], Any] | None = None, reverse: bool = False) -> JsonQuery:
        self.__prepare()
        if isinstance(self.__json_content, list):
            json_list: list[Any] = self.__json_content
            json_list.sort(reverse=reverse, key=key)
            self.__json_content = json_list

        return self

    def SortBy(self, attr: str, reverse: bool = False) -> JsonQuery:
        if isinstance(self.__json_content, list):
            json_list: list[dict[str, Any]] = self.__json_content
            self.__json_content = sorted(json_list, key=itemgetter(attr), reverse=reverse)

        return self

    def Reset(self) -> JsonQuery:
        self.__json_content = self.__root_json_content
        self.__queries.clear()
        self.__attributes.clear()
        self.__dropped_properties.clear()
        self.__query_index = 0
        self.__limit_records = 0
        self.__offset_records = 0
        self.__distinct_property = ""
        return self

    def Only(self, *properties: str) -> JsonQuery:
        self.__attributes.extend(properties)
        return self.__prepare()

    def Pluck(self, attr: str) -> list[Any]:
        self.__prepare()
        if self.__distinct_property != "":
            self.__distinct()

        if self.__limit_records != 0:
            self.__limit()

        result: list[Any] = []
        if isinstance(self.__json_content, list):
            json_list: list[Any] = self.__json_content
            for a in json_list:
                if isinstance(a, dict):
                    d: dict[str, Any] = a
                    if d.get(attr) is not None:
                        result.append(d[attr])

        return result

    def Out(self, func: Callable[[dict[str, Any] | list[Any]], __T]) -> __T:
        return func(self.__json_content)

    def Macro(self, operator: str, func: QueryFunc) -> JsonQuery:
        self.__query_map[operator] = func
        return self

    def Copy(self) -> JsonQuery:
        new_query: JsonQuery = deepcopy(self)
        return new_query.Reset()

    def More(self) -> JsonQuery:
        self.__root_json_content = self.Get()
        self.__queries.clear()
        self.__attributes.clear()
        self.__dropped_properties.clear()
        self.queryIndex = 0
        self.limitRecords = 0
        self.distinctProperty = ""
        return self

    def Drop(self, *properties: str) -> JsonQuery:
        self.__dropped_properties.extend(properties)
        return self

    # **Privat functions**

    def __getAggregationValues(self, *properties: str) -> list[float]:
        self.__prepare()
        if self.__distinct_property != "":
            self.__distinct()

        if self.__limit_records != 0:
            self.__limit()

        floats: list[float] = []

        if isinstance(self.__json_content, list):
            json_list: list[Any] = self.__json_content
            floats = self.__getFloatValFromArray(json_list, *properties)

        if isinstance(self.__json_content, dict):
            json_dict: dict[str, Any] = self.__json_content
            if len(properties) == 0:
                return []

            value: Any | None = json_dict.get(properties[0])
            if value is None:
                return []
            elif isinstance(value, float) or isinstance(value, int):
                floats.append(float(value))

        return floats

    def __getFloatValFromArray(self, json_list: list[Any], *properties: str) -> list[float]:
        floats: list[float] = []
        for a in json_list:
            if isinstance(a, float) or isinstance(a, int):
                if len(properties) > 0:
                    return []

                floats.append(float(a))

            if isinstance(a, dict):
                js_dict: dict[str, Any] = a
                if len(properties) == 0:
                    # TODO: error
                    return []

                dv: Any | None = js_dict.get(properties[0])
                if dv is not None and (isinstance(dv, float) or isinstance(dv, int)):
                    floats.append(float(dv))
                else:
                    # TODO: error
                    return []

        return floats

    def __limit(self):
        if isinstance(self.__json_content, list):
            json_content: list[Any] = self.__json_content
            if self.__limit_records <= 0:
                return self

            if len(json_content) > self.__limit_records:
                self.__json_content = json_content[:self.__limit_records]

    def __offset(self):
        if isinstance(self.__json_content, list):
            json_content: list[Any] = self.__json_content
            if self.__offset_records < 0:
                return self

            if len(json_content) >= self.__limit_records:
                self.__json_content = json_content[self.__offset_records:]
            else:
                self.__json_content.clear()

    def __drop(self):
        for node in self.__dropped_properties:
            self.__json_content = deleteNestedValue(self.__json_content, node, self.__separator)

    def __only(self):
        result: list[dict[str, Any]] = []
        if isinstance(self.__json_content, list):
            json_content: list[Any] = self.__json_content
            for am in json_content:
                tmap: dict[str, Any] = {}
                for attr in self.__attributes:
                    node, alias = makeAlias(attr, self.__separator)
                    value = getNestedValue(am, node, self.__separator)
                    if value is None:
                        continue

                    tmap[alias] = value

                if len(tmap) > 0:
                    result.append(tmap)

        self.__json_content = result

    def __findInList(self, value_list: list[Any]) -> list[Any]:
        result: list[Any] = []
        for v in value_list:
            if isinstance(v, dict):
                m: dict[str, Any] = v
                result.extend(self.__findInDict(m))

        return result

    def __findInDict(self, value_map: dict[str, Any]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        or_passed: bool = False
        for q_list in self.__queries:
            and_passed: bool = True
            for q in q_list:
                cf: QueryFunc | None = self.__query_map.get(q.operator)
                if cf is None:
                    return result

                value = getNestedValue(value_map, q.key, self.__separator)
                if value is None:
                    and_passed = False
                    continue
                else:
                    qb: bool = cf(value, q.value)
                    and_passed = and_passed and qb

            or_passed = or_passed or and_passed

        if or_passed:
            result.append(value_map)

        return result

    def __processQuery(self) -> JsonQuery:
        if isinstance(self.__json_content, list):
            json_content: list[Any] = self.__json_content
            self.__json_content = self.__findInList(json_content)

        return self

    def __distinct(self) -> JsonQuery:
        m: dict[str, bool] = {}
        dt: list[Any] = []

        if isinstance(self.__json_content, list):
            json_content: list[Any] = self.__json_content
            for a in json_content:
                if isinstance(a, dict):
                    value = getNestedValue(a, self.__distinct_property, self.__separator)
                    if value is not None and m.get(str(value)) is None:
                        dt.append(a)
                        m[str(value)] = True

        self.__json_content = dt
        return self

    def __prepare(self) -> JsonQuery:
        if len(self.__queries) > 0:
            self.__processQuery()
        if self.__distinct_property != "":
            self.__distinct()
        if len(self.__attributes) > 0:
            self.__only()

        self.__query_index = 0
        return self
