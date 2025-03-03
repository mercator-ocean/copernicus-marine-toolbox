import logging
from typing import (
    Literal,
    Optional,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import BaseModel

logger = logging.getLogger("copernicusmarine")


def check_type_is_base_model(type_to_check: Type) -> bool:
    try:
        return issubclass(type_to_check, BaseModel)
    except TypeError:
        return False


class WrongFieldsError(Exception):
    """
    Exception raised when the fields requested are not queryable.

    Please verify the fields requested are a field name in the response type model
    for the command you are using. See the response type page of `the documentation <https://toolbox-docs.marine.copernicus.eu/>`_ for more information.
    """  # noqa: E501

    def __init__(
        self,
        wrong_fields: set[str],
        available_fields: set[str],
        option_name: str,
        *args: object,
    ) -> None:
        super().__init__(*args)
        self.wrong_fields = wrong_fields
        self.available_fields = available_fields
        self.option_name = option_name

    def __str__(self) -> str:
        return (
            f"All ``{self.option_name}`` fields are invalid"
            f": {', '.join(sorted(self.wrong_fields))}.\n"
            f"Available fields: {', '.join(sorted(self.available_fields))}"
        )


def build_query(
    fields_to_include_or_exclude: set[str],
    type_to_check: Type,
    query: Optional[dict] = None,
) -> dict:
    """
    Recursively builds a query to include or exclude fields from a Pydantic model
    Allows to handle nested models, lists and dictionaries.

    Example of resulting query:
    input:
    build_query({"product_id", "dataset_id"}, CopernicusMarineCatalogue)

    output:
    {
        "products": {
            "__all__": {
                "product_id": True
                "datasets": {
                    "__all__": {
                        "dataset_id": True
                    }
                }
            },
        }
    }
    """  # noqa: E501
    if query is None:
        query = {}

    for (
        field_name,
        field_type,
    ) in get_type_hints(type_to_check).items():
        if field_name in fields_to_include_or_exclude:
            query[field_name] = True
            continue
        all_base_models = _get_base_models_in_type(field_type)
        for base_model, in_an_iterable in (all_base_models or {}).items():
            if field_name not in query:
                query[field_name] = {"__all__": {}} if in_an_iterable else {}
            result = build_query(
                fields_to_include_or_exclude,
                base_model,
                (
                    query[field_name]["__all__"]
                    if in_an_iterable
                    else query[field_name]
                ),
            )
            if not result:
                del query[field_name]
    return query


def _get_base_models_in_type(
    type_to_check: Type,
    in_an_iterable: bool = False,
) -> Optional[dict[Type, Literal["__all__", None]]]:
    models = {}
    if check_type_is_base_model(type_to_check):
        return {type_to_check: "__all__" if in_an_iterable else None}
    elif get_origin(type_to_check) is list:
        result = _get_base_models_in_type(
            get_args(type_to_check)[0], in_an_iterable=True
        )
        if result:
            models.update(result)
    elif get_origin(type_to_check) is dict:
        result = _get_base_models_in_type(
            get_args(type_to_check)[1], in_an_iterable=True
        )
        if result:
            models.update(result)
    elif get_origin(type_to_check) is Union:
        for union_type in get_args(type_to_check):
            result = _get_base_models_in_type(
                union_type, in_an_iterable=in_an_iterable
            )
            if result:
                models.update(result)
    return models


def _return_available_fields(
    type_to_check: Type, available_fields: Optional[set[str]] = None
) -> set[str]:
    """
    Recursively get all the fields that are available in the model
    """
    if available_fields is None:
        available_fields = set()

    for (
        field_name,
        field_type,
    ) in get_type_hints(type_to_check).items():
        available_fields.add(field_name)
        all_base_models = _get_base_models_in_type(field_type)
        for base_model, _ in (all_base_models or {}).items():
            result = _return_available_fields(base_model, available_fields)
            available_fields.union(result)
    return available_fields


def get_queryable_requested_fields(
    requested_fields: set[str], type_to_check: Type, option_name: str
) -> set[str]:
    """
    Computes all the fields that are available in the model and checks if the requested fields are queryable

    if none of the fields are queryable, it raises an error
    if some of the fields are not queryable, it returns the queryable fields and logs a warning
    Returns:
    - a set with the queryable fields
    """  # noqa: E501
    available_fields = _return_available_fields(type_to_check)
    queryable_fields = requested_fields.intersection(available_fields)
    wrong_inputs = requested_fields - queryable_fields
    if queryable_fields == set():
        raise WrongFieldsError(wrong_inputs, available_fields, option_name)
    elif wrong_inputs:
        logger.warning(
            f"Some ``{option_name}`` fields are invalid:"
            f" {', '.join(wrong_inputs)}.\n "
            f"Available fields: {', '.join(sorted(available_fields))}."
        )
    return queryable_fields
