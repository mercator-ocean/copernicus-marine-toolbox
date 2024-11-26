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


def check_type_is_base_model(type_to_check: Type) -> bool:
    try:
        return issubclass(type_to_check, BaseModel)
    except TypeError:
        return False


class QueryBuilder:
    fields_to_include_or_exclude: set[str]

    def __init__(self, fields_to_include_or_exclude: set[str]):
        self.fields_to_include_or_exclude = fields_to_include_or_exclude
        assert (
            self.fields_to_include_or_exclude
        ), "fields_to_include_or_exclude is empty"

    def build_query(
        self,
        type_to_check: Type,
        query: Optional[dict] = None,
    ):
        """
        Recursively builds a query to include or exclude fields from a Pydantic model
        Allows to handle nested models, lists and dictionaries.

        Example of resulting query:
        input:
        QueryBuilder({"product_id", "dataset_id"}).build_query(CopernicusMarineCatalogue)

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
            if field_name in self.fields_to_include_or_exclude:
                query[field_name] = True
                continue
            all_base_models = self._get_base_models_in_type(field_type)
            for base_model, in_an_iterable in (all_base_models or {}).items():
                if field_name not in query:
                    query[field_name] = (
                        {"__all__": {}} if in_an_iterable else {}
                    )
                result = self.build_query(
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
        self,
        type_to_check: Type,
        in_an_iterable: bool = False,
    ) -> Optional[dict[Type, Literal["__all__", None]]]:
        models = {}
        if check_type_is_base_model(type_to_check):
            return {type_to_check: "__all__" if in_an_iterable else None}
        elif get_origin(type_to_check) is list:
            result = self._get_base_models_in_type(
                get_args(type_to_check)[0], in_an_iterable=True
            )
            if result:
                models.update(result)
        elif get_origin(type_to_check) is dict:
            result = self._get_base_models_in_type(
                get_args(type_to_check)[1], in_an_iterable=True
            )
            if result:
                models.update(result)
        elif get_origin(type_to_check) is Union:
            for union_type in get_args(type_to_check):
                result = self._get_base_models_in_type(
                    union_type, in_an_iterable=in_an_iterable
                )
                if result:
                    models.update(result)
        return models
