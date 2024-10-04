def docstring_parameter(dictionary_variables):
    def wrapper(obj):
        obj.__doc__ = obj.__doc__.format(**dictionary_variables)
        return obj

    return wrapper


# from functools import wraps
# from typing import Any, Callable, TypeVar

# T = TypeVar("T", bound=Callable[..., Any])


# def docstring_parameter(dictionary_variables: dict[str, str]) -> T:
#     @wraps(dictionary_variables)
#     def wrapper(obj: T) -> T:
#         obj.__doc__ = obj.__doc__.format(**dictionary_variables)
#         return obj

#     return wrapper  # type: ignore 3:12
