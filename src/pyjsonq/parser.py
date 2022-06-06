from __future__ import annotations
from copy import deepcopy
from operator import itemgetter

__all__ = ["JsonQuery", "JQuery"]

import json
import toml
from typing import Any, Callable, TypeVar

from .errors import NoNumberException, NoPropertyProvidedException, OperatorDoesntExistException, PathDoesntExistException, QueryIsEmptyException, QueryIsNotListException
from .helper import getNestedValue, deleteNestedValue, makeAlias
from .query import Query, QueryDict, defaultQueries, QueryFunc, QueryOperator

DEFAULT_SEPARATOR = "."

__T = TypeVar("__T")


class JsonQuery:
    """Provides an easy to use API to query over your json data.

    Parameters
    ----------
    separator : str, optional
        The separator used for separating nodes / paths for queries,
        by default "."

    Usage
    -----
    First, create a new query.

    ```python
    jq: JsonQuery = JsonQuery()
    ```

    Next load the data using either `File / file`,
    `String / string`, `TOMLFile / toml_file`,
    `TOMLString / toml_string` or `Raw / raw`.

    ```python
    jq.String("{ ... }")
    ```

    And finally you can start querying over your data!

    ```python
    names = jq.At("users").Select("name")
    ```

    Alias & Syntax
    --------------
    - Instead of calling `JsonQuery` you can also call `JQuery`
    - The class provides both methods in CamelCase and snake_case
    """

    def __init__(self, separator: str = DEFAULT_SEPARATOR) -> None:
        self.__separator: str = separator
        self.___root_json_content: Any | None = None
        self.___json_content: Any | None = None

        self.__query_map: QueryDict = defaultQueries()
        self.__query_index: int = 0

        self.__offset_records: int = 0
        self.__limit_records: int = 0

        self.__queries: list[list[Query]] = []
        self.__dropped_properties: list[str] = []
        self.__attributes: list[str] = []
        self.__distinct_property: str = ""

    @property
    def __root_json_content(self) -> Any:
        if self.___root_json_content is None:
            raise QueryIsEmptyException()

        return self.___root_json_content

    @__root_json_content.setter
    def __root_json_content(self, value: Any):
        self.___root_json_content = value

    @property
    def __json_content(self) -> Any:
        if self.___json_content is None:
            return self.__json_content

        return self.___json_content

    @__json_content.setter
    def __json_content(self, value: Any):
        self.___json_content = value

    def File(self, file_path: str, encoding: str = "utf-8") -> JsonQuery:
        """Load a json file into the query.

        Parameters
        ----------
        file_path : str
            Path to the json file
        encoding : str, optional
            The encoding of the file, by default "utf-8"
        """

        with open(file_path, encoding=encoding) as file:
            self.__root_json_content = json.loads(file.read())

        return self

    def String(self, json_string: str) -> JsonQuery:
        """Load a json string into the query.

        Parameters
        ----------
        json_string : str
            The json formatted string
        """

        self.___root_json_content = json.loads(json_string)
        return self

    def TOMLFile(self, file_path: str, encoding: str = "utf-8") -> JsonQuery:
        """Load a toml file into the query.

        Parameters
        ----------
        file_path : str
            Path to the toml file
        encoding : str, optional
            The encoding of the file, by default "utf-8"
        """

        with open(file_path, encoding="utf-8") as file:
            self.__root_json_content = toml.loads(file.read())

        return self

    def TOMLString(self, toml_string: str) -> JsonQuery:
        """Load a toml string into the query.

        Parameters
        ----------
        json_string : str
            The toml formatted string
        """

        self.__root_json_content = toml.loads(toml_string)
        return self

    def Raw(self, data: list[Any] | dict[str, Any]) -> JsonQuery:
        """Set the query content to `data`.

        Parameters
        ----------
        data : list[Any] | dict[str, Any]
            The data you want the query to be
        """

        self.__root_json_content = data
        return self

    def At(self, node: str) -> JsonQuery:
        """Seeks the json content for the provided node.

        Parameters
        ----------
        node : str
            The node / path in the json content, e.g: "users.[0]" or
            "users.[1].name"
        """

        value: Any = getNestedValue(self.__json_content, node, self.__separator)
        if value is None:
            raise PathDoesntExistException("At / at", node)

        self.__json_content = value
        return self

    def Select(self, *properties: str) -> JsonQuery:
        """Select only the specified properties from the current query.

        Parameters
        ----------
        properties : *str
            The properties to select
        """

        self.__attributes.extend(properties)
        return self

    def Get(self) -> Any:
        """Returns the result of the query.

        Returns
        -------
        Any
            The result of the query
        """

        self.__prepare()
        if self.__offset_records != 0:
            self.__offset()

        if self.__limit_records != 0:
            self.__limit()

        if len(self.__dropped_properties) != 0:
            self.__drop()

        return self.__json_content

    def Where(self, key: str, cond: str | QueryOperator, val: Any) -> JsonQuery:
        """Builds a where clause and filters the query accordingly.

        Parameters
        ----------
        key : str
            The property name of the value
        cond : str | QueryOperator
            The operator to be used for matching
        val : Any
            Value to be matched with

        Operators
        ---------
        - `=`: For equality matching
        - `eq`: Same as `=`
        - `!=`: For not equality matching
        - `neq`: Same as `!=`
        - `<>`: Same as `!=`
        - `>`: Check if value of given **key** in data is Greater than **val**
        - `gt`: Same as `>`
        - `<`: Check if the value of given **key** in data is Less than **val**
        - `lt`: Same as `<`
        - `>=`: Check if the value of given **key** in data is Greater than or Equal of **val**
        - `gte`: Same as `>=`
        - `<=`: Check if the value of given **key** in data is Less than or Equal of **val**
        - `lte`: Same as `<=`
        - `isIn`: Check if the value of given **key** in data is exists in given **val**. **val** should be a **list** of `int / float / str`.
        - `notIn`: Check if the value of given **key** in data is not exists in given **val**. **val** should be a **list** of `int / float / str`.
        - `holds`: Check if the value of given **key** contains given **val**.
        - `notHolds`: Check if the value of given **key** does not contains given **val**.
        - `startsWith`: Check if the value of given **key** in data starts with (has a prefix of) the given **val**. This would only work for **String** type data and exact match.
        - `endsWith`: Check if the value of given **key** in data ends with (has a suffix of) the given **val**. This would only work for **String** type data and exact match.
        - `contains`: Check if the value of given **key** in data has a substring of given **val**. This would only work for **String** type data and loose match.
        - `strictContains`: Check if the value of given **key** in data has a substring of given **val**. This would only work for **String** type data and exact match.
        - `notContains`: Check if the value of given **key** in data does not have a substring of given **val**. This would only work for **String** type data and loose match.
        - `notStrictContains`: Check if the value of given **key** in data does not have a substring of given **val**. This would only work for **String** type data and exact match.
        - `leneq`: For equality matching length of `str / list / dict`
        - `lenneq`: For not equality matching length of `str / list / dict`
        - `lengt`: For matching length greater than value of `str / list / dict`
        - `lenlt`: For matching length less than value of `str / list / dict`
        - `lengte`: For matching length greater than equal value of `str / list / dict`
        - `lenlte`: For matching length greater than equal value of `str / list / dict`
        """

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
        """Same as Where but will OR-ed the result with other conditions.

        Parameters
        ----------
        key : str
            The property name of the value
        cond : str | QueryOperator
            The operator to be used for matching
        val : Any
            Value to be matched with
        """

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
        """Same as `Where(key, "=", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : Any
            Should be `int / float / str`
        """

        return self.Where(key, QueryOperator.eq, val)

    def WhereNotEqual(self, key: str, val: Any) -> JsonQuery:
        """Same as `Where(key, "!=", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : Any
            Should be `int / float / str`
        """

        return self.Where(key, QueryOperator.notEq, val)

    def WhereNone(self, key: str) -> JsonQuery:
        """Same as `Where(key, "=", None)`.

        Parameters
        ----------
        key : str
            The property name of the value
        """

        return self.Where(key, QueryOperator.eq, None)

    def WhereNotNone(self, key: str) -> JsonQuery:
        """Same as `Where(key, "!=", None)`.

        Parameters
        ----------
        key : str
            The property name of the value
        """

        return self.Where(key, QueryOperator.notEq, None)

    def WhereIn(self, key: str, val: list[Any]) -> JsonQuery:
        """Same as `Where(key, "isIn", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : list[Any]
            Should be list of `int / float / str`
        """

        return self.Where(key, QueryOperator.isIn, val)

    def WhereNotIn(self, key: str, val: list[Any]) -> JsonQuery:
        """Same as `Where(key, "notIn", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : list[Any]
            Should be list of `int / float / str`
        """

        return self.Where(key, QueryOperator.notIn, val)

    def WhereHolds(self, key: str, val: Any) -> JsonQuery:
        """Same as `Where(key, "holds", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : Any
            Should be `int / float / str`
        """

        return self.Where(key, QueryOperator.holds, val)

    def WhereNotHolds(self, key: str, val: Any) -> JsonQuery:
        """Same as `Where(key, "notHolds", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : Any
            Should be `int / float / str`
        """

        return self.Where(key, QueryOperator.notHolds, val)

    def WhereStartsWith(self, key: str, val: str) -> JsonQuery:
        """Same as `Where(key, "startsWith", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : str
        """

        return self.Where(key, QueryOperator.startsWith, val)

    def WhereEndsWith(self, key: str, val: str) -> JsonQuery:
        """Same as `Where(key, "endsWith", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : str
        """

        return self.Where(key, QueryOperator.endsWith, val)

    def WhereContains(self, key: str, val: str) -> JsonQuery:
        """Same as `Where(key, "contains", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : str
        """

        return self.Where(key, QueryOperator.contains, val)

    def WhereNotContains(self, key: str, val: str) -> JsonQuery:
        """Same as `Where(key, "notContains", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : str
        """

        return self.Where(key, QueryOperator.notContains, val)

    def WhereStrictContains(self, key: str, val: str) -> JsonQuery:
        """Same as `Where(key, "strictContains", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : str
        """

        return self.Where(key, QueryOperator.strictContains, val)

    def WhereNotStrictContains(self, key: str, val: str) -> JsonQuery:
        """Same as `Where(key, "notStrictContains", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : str
        """

        return self.Where(key, QueryOperator.notStrictContains, val)

    def WhereLenEqual(self, key: str, val: int) -> JsonQuery:
        """Same as `Where(key, "leneq", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : int
        """

        return self.Where(key, QueryOperator.lenEq, val)

    def WhereLenNotEqual(self, key: str, val: int) -> JsonQuery:
        """Same as `Where(key, "lenneq", val)`.

        Parameters
        ----------
        key : str
            The property name of the value
        val : int
        """

        return self.Where(key, QueryOperator.lenNotEq, val)

    def Find(self, path: str) -> Any:
        """Gets the result of the given path. No need to call `Get()`.

        Parameters
        ----------
        path : str
            The 

        Returns
        -------
        Any
            The value at the end of `path`
        """

        return self.At(path).Get()

    def Offset(self, offset: int) -> JsonQuery:
        """Skips the first `offset` elements in the current query.

        Parameters
        ----------
        offset : int
        """

        self.__offset_records = offset
        return self

    def Limit(self, limit: int) -> JsonQuery:
        """Skips the last `limit` elements in the current query.

        Parameters
        ----------
        limit : int
        """

        self.__limit_records = limit
        return self

    def Sum(self, *properties: str) -> float:
        """Returns the sum of the current query or if properties is
        provided the sum of the current query at `properties`.

        Parameters
        ----------
        properties : str, optional
            The property name of the data
        """

        floats: list[float] = self.__getAggregationValues(*properties)
        return sum(floats)

    def Count(self) -> int | None:
        """Returns the number of items in the current query. Returns
        None if the query is neither a list or a dict.

        Returns
        -------
        int | None
        """

        self.__prepare()

        if isinstance(self.__json_content, list) or isinstance(self.__json_content, dict):
            json_content: list[Any] | dict[str, Any] = self.__json_content
            return len(json_content)
        else:
            return

    def Min(self, *properties: str) -> float:
        """Returns the smallest value in the current query or if
        properties is provided the smallest value in the current query
        at `properties`.

        Parameters
        ----------
        properties : str, optional
            The property name of the data
        """

        floats: list[float] = self.__getAggregationValues(*properties)
        return min(floats)

    def Max(self, *properties: str) -> float:
        """Returns the biggest value in the current query or if
        properties is provided the biggest value in the current query
        at `properties`.

        Parameters
        ----------
        properties : str, optional
            The property name of the data
        """

        floats: list[float] = self.__getAggregationValues(*properties)
        return max(floats)

    def Avg(self, *properties: str) -> float:
        """Returns the average of the current query or if
        properties is provided the average of the current query
        at `properties`.

        Parameters
        ----------
        properties : str, optional
            The property name of the data
        """

        floats: list[float] = self.__getAggregationValues(*properties)
        return sum(floats) / len(floats)

    def First(self) -> Any:
        """Returns the first element in the current query.

        Returns
        -------
        Any
            The first element
        """

        self.__prepare()
        if isinstance(self.__json_content, list):
            json_content: list[Any] = self.__json_content
            return json_content[0]
        else:
            raise QueryIsNotListException()

    def Last(self) -> Any:
        """Returns the last element in the current query.

        Returns
        -------
        Any
            The last element
        """

        self.__prepare()
        if isinstance(self.__json_content, list):
            json_content: list[Any] = self.__json_content
            return json_content[-1]
        else:
            raise QueryIsNotListException()

    def Nth(self, index: int) -> Any:
        """Returns the nth element in the current query.

        Parameters
        ----------
        index : int
            The index of the element. Works the same as standard Python
            indexes

        Returns
        -------
        Any
            The nth element
        """

        self.__prepare()
        if isinstance(self.__json_content, list):
            json_content: list[Any] = self.__json_content
            return json_content[index]
        else:
            raise QueryIsNotListException()

    def GroupBy(self, attr: str) -> JsonQuery:
        """Builds a chunk of exact matched data in a group list using
        provided attribute / column / property.

        Parameters
        ----------
        attr : str
            The property by which you want to group the collection
        """

        self.__prepare()
        dt: dict[str, list[Any]] = {}
        if isinstance(self.__json_content, list):
            json_list: list[Any] = self.__json_content
            for a in json_list:
                if isinstance(a, dict):
                    value = getNestedValue(a, attr, self.__separator)
                    if value is None:
                        raise PathDoesntExistException("GroupBy / group_by", attr)
                    if dt.get(str(value)) is None:
                        dt[str(value)] = [a]
                    else:
                        dt[str(value)].append(a)

        self.__json_content = dt
        return self

    def Distinct(self, attr: str) -> JsonQuery:
        """Builds distinct value using provided
        attribute / column / property.

        Parameters
        ----------
        attr : str
            The property by which you want to distinct the collection
        """

        self.__distinct_property = attr
        return self

    def Sort(self, key: Callable[[Any], Any] | None = None, reverse: bool = False) -> JsonQuery:
        """Sorts the current query using default Python sort. For
        info on the parameters look at default Python sort.

        Parameters
        ----------
        key : Callable[[Any], Any], optional
            The function used for sorting the values, by default None.
        reverse : bool, optional
            Reverses the results, by default False
        """

        self.__prepare()
        if isinstance(self.__json_content, list):
            json_list: list[Any] = self.__json_content
            json_list.sort(reverse=reverse, key=key)
            self.__json_content = json_list
        else:
            raise QueryIsNotListException()

        return self

    def SortBy(self, attr: str, reverse: bool = False) -> JsonQuery:
        """Sorts the current query -- if it is a list of dicts -- by
        the given dict attribute.

        Parameters
        ----------
        attr : str
            The attribute of the dict
        reverse : bool, optional
            Reverses the results, by default False
        """

        if isinstance(self.__json_content, list):
            json_list: list[dict[str, Any]] = self.__json_content
            self.__json_content = sorted(json_list, key=itemgetter(attr), reverse=reverse)
        else:
            raise QueryIsNotListException()

        return self

    def Reset(self) -> JsonQuery:
        """Reset the queries with the original data so that you can
        query again.
        """

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
        """_summary_

        Parameters
        ----------
        properties: *str
            The properties which you want to get in final results
        """

        self.__attributes.extend(properties)
        return self.__prepare()

    def Pluck(self, attr: str) -> list[Any]:
        """Builds a list of values from a property of a list of dicts.

        Parameters
        ----------
        attr : str
            The property by which you want to get an array

        Returns
        -------
        list[Any]
        """

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
        else:
            raise QueryIsNotListException()

        return result

    def Out(self, func: Callable[[dict[str, Any] | list[Any]], __T]) -> __T:
        """Returns the current query as the type / function result
        provided.

        Parameters
        ----------
        func : Callable[[dict[str, Any]  |  list[Any]], T]
            The function / type that will be called

        Returns
        -------
        T
            The Result
        """

        return func(self.__json_content)

    def Macro(self, operator: str, func: QueryFunc) -> JsonQuery:
        """Adds a query func to the JsonQuery instance.

        Parameters
        ----------
        operator : str
            The operator to use in the `Where()` function
        func : QueryFunc (Callable[[Any, Any], bool])
            The function that will be called
        """

        self.__query_map[operator] = func
        return self

    def Copy(self) -> JsonQuery:
        """Returns a new fresh instance of JsonQuery with the original
        copy of data so that you can do concurrent operation on the same
        data without being decoded again.
        """

        new_query: JsonQuery = deepcopy(self)
        return new_query.Reset()

    def More(self) -> JsonQuery:
        """Provides the functionality to query over the resultant data.
        """

        self.__root_json_content = self.Get()
        self.__queries.clear()
        self.__attributes.clear()
        self.__dropped_properties.clear()
        self.queryIndex = 0
        self.limitRecords = 0
        self.distinctProperty = ""
        return self

    def Drop(self, *properties: str) -> JsonQuery:
        """Drops / Removes the given properties from the curren query.
        The properties are either keys to the value or indexes to a list.

        Parameters
        ----------
        properties : *str
            The properties / indexes you want to drop

        Returns
        -------
        JsonQuery
            _description_
        """

        self.__dropped_properties.extend(properties)
        return self

    # **Aliases**

    file = File
    string = String
    toml_file = TOMLFile
    toml_string = TOMLString
    raw = Raw
    at = At
    select = Select
    get = Get
    where = Where
    or_where = OrWhere
    where_equal = WhereEqual
    where_notEqual = WhereNotEqual
    where_none = WhereNone
    where_notNone = WhereNotNone
    where_in = WhereIn
    where_notIn = WhereNotIn
    where_holds = WhereHolds
    where_not_holds = WhereNotHolds
    where_starts_with = WhereStartsWith
    where_ends_with = WhereEndsWith
    where_contains = WhereContains
    where_strict_contains = WhereStrictContains
    where_not_contains = WhereNotContains
    where_not_strictContains = WhereNotStrictContains
    where_len_equal = WhereLenEqual
    where_len_not_equal = WhereLenNotEqual
    find = Find
    offset = Offset
    limit = Limit
    sum = Sum
    count = Count
    min = Min
    max = Max
    avg = Avg
    first = First
    last = Last
    nth = Nth
    group_by = GroupBy
    distinct = Distinct
    sort = Sort
    sort_by = SortBy
    reset = Reset
    only = Only
    pluck = Pluck
    out = Out
    macro = Macro
    copy = Copy
    more = More
    drop = Drop

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
                    raise NoPropertyProvidedException()

                dv: Any | None = js_dict.get(properties[0])
                if dv is not None and (isinstance(dv, float) or isinstance(dv, int)):
                    floats.append(float(dv))
                else:
                    raise NoNumberException()

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
                    raise OperatorDoesntExistException(q.operator)

                value = getNestedValue(value_map, q.key, self.__separator)
                if value is None:
                    and_passed = False
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


JQuery = JsonQuery
