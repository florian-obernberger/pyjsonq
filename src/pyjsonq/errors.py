__all__ = ["QueryIsEmptyException", "PathDoesntExistException"]


class QueryIsEmptyException(Exception):
    """Raised when the query is empty aka when there wasn't any data 
    loaded yet.
    """

    def __init__(self) -> None:
        super().__init__("Query is empty. Have you forgotten to load the data?")


class PathDoesntExistException(Exception):
    """Raised when a query selection of some sort returned None.
    """

    def __init__(self, fname: str, node: str):
        super().__init__(f"The method {fname} with path / node {node} doesn't exist!")


class QueryIsNotListException(Exception):
    """Raised when the query is supposed to be a list but isn't.
    """

    def __init__(self) -> None:
        super().__init__("The query does currently not contain list!")


class NoPropertyProvidedException(Exception):
    """Raised when there was supposed to be a property but none was
    specified."""

    def __init__(self) -> None:
        super().__init__("Please provide a property!")


class PropertyProvidedException(Exception):
    """Raised when there wasn't supposed to be a property but there was
    one specified."""

    def __init__(self) -> None:
        super().__init__("Don't provide a property here!")


class NoNumberException(Exception):
    """Raised when a value was expected to be int or float.
    """

    def __init__(self) -> None:
        super().__init__("Value was neither int nor float!")


class OperatorDoesntExistException(Exception):
    """Raised when the operator of a Where call doesn't exist.
    """

    def __init__(self, op: str) -> None:
        super().__init__(
            f"The operator {op} doesn't exist. Either correct ir or add a corresponding QueryFunc via `Macro`!"
        )
