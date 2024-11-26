from typing import Optional, Type, Union, get_args, get_origin, get_type_hints

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
            elif get_origin(field_type) is Union:
                for union_type in get_args(field_type):
                    if get_origin(union_type) is None:
                        continue
                    if get_origin(union_type) is list:
                        if field_name not in query:
                            query[field_name] = {"__all__": {}}
                        result = self.build_query(
                            get_args(union_type)[0],
                            query[field_name]["__all__"],
                        )
                        if not result:
                            del query[field_name]
                    else:
                        if field_name not in query:
                            query[field_name] = {}
                        result = self.build_query(
                            union_type, query[field_name]
                        )
                        if not result:
                            del query[field_name]
            elif get_origin(field_type) is list:
                if field_name not in query:
                    query[field_name] = {"__all__": {}}
                if get_origin(get_args(field_type)[0]) is Union:
                    for union_type in get_args(get_args(field_type)[0]):
                        if field_name not in query:
                            query[field_name] = {"__all__": {}}
                        result = self.build_query(
                            union_type,
                            query[field_name]["__all__"],
                        )
                        if not result:
                            del query[field_name]
                else:
                    result = self.build_query(
                        get_args(field_type)[0],
                        query[field_name]["__all__"],
                    )
                    if not result:
                        del query[field_name]
            elif get_origin(field_type) is dict:
                if field_name not in query:
                    query[field_name] = {"__all__": {}}
                result = self.build_query(
                    get_args(field_type)[1],
                    query[field_name]["__all__"],
                )
                if not result:
                    del query[field_name]
            elif check_type_is_base_model(field_type):
                if field_name not in query:
                    query[field_name] = {}
                result = self.build_query(field_type, query[field_name])
                if not result:
                    del query[field_name]
        return query


if __name__ == "__main__":
    # query = QueryBuilder({"status", "message"}).build_query(
    #     CopernicusMarineCatalogue
    # )
    # print(query)

    class TestSomething(BaseModel):
        a: str
        b: int
        c: float
        d: dict[str, str]
        e: list[int]
        f: Union[str, int]
        g: dict[str, Union[str, int]]
        h: list[Union[str, int]]
        i: dict[str, list[Union[str, int]]]
        j: list[str]
        k: Optional[str]

    for file_name, file_type in get_type_hints(TestSomething).items():
        print("-------------------------")
        print(file_name, file_type)
        print(get_origin(file_type))
        print("get args", get_args(file_type))
        if get_origin(file_type) is dict:
            print("diccionari", get_type_hints(get_args(file_type)[0]))
        elif get_origin(file_type) is list:
            print("llista")
            for file in get_args(file_type):
                print("llista", get_type_hints(file))
        elif get_origin(file_type) is Union:
            print("union/optional")
            for file in get_args(file_type):
                print("union/optional", get_type_hints(file))
        else:
            print(get_type_hints(file_type))
        print("-------------------------")
