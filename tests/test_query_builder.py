from typing import Literal

from copernicusmarine.catalogue_parser.models import CopernicusMarineCatalogue
from copernicusmarine.core_functions.fields_query_builder import (
    _return_available_fields,
    build_query,
)
from copernicusmarine.core_functions.models import ResponseGet, ResponseSubset


class TestQueryBuilder:
    # For the catalogue
    def test_query_builder_catalogue(self, snapshot):
        query = build_query(
            ({"product_id", "dataset_id"}), CopernicusMarineCatalogue
        )
        assert query == snapshot

    def test_query_builder_catalogue_deep_inside(self, snapshot):
        query = build_query(
            ({"service_name", "coordinate_unit", "maximum_value"}),
            CopernicusMarineCatalogue,
        )
        assert query == snapshot

    # For the get
    def test_query_builder_responseget(self, snapshot):
        query = build_query(({"files", "files_deleted"}), ResponseGet)
        assert query == snapshot

    def test_query_builder_responseget_inside_basemodel(self, snapshot):
        query = build_query(({"file_status"}), ResponseGet)
        assert query == snapshot

    # For the subset
    def test_basic_query_builder_responsesubset(self, snapshot):
        query = build_query(
            ({"file_path", "output_directory"}), ResponseSubset
        )
        assert query == snapshot

    def test_subset_optional_float(self, snapshot):
        query = build_query(
            ({"file_size", "data_transfer_size"}), ResponseSubset
        )
        assert query == snapshot

    def test_subset_optional_status(self, snapshot):
        query = build_query(({"status", "file_status"}), ResponseSubset)
        assert query == snapshot

    def test_subset_optional_coordinate_id(self, snapshot):
        query = build_query(({"coordinate_id"}), ResponseSubset)
        assert query == snapshot

    def test_subset_optional_coordinate_id_and_file(self, snapshot):
        query = build_query(({"coordinate_id", "file_status"}), ResponseSubset)
        assert query == snapshot

    def test_return_available_fields(self, snapshot):
        self.when_I_run_with_model("describe", snapshot)
        self.when_I_run_with_model("get", snapshot)
        self.when_I_run_with_model("subset", snapshot)

    def when_I_run_with_model(
        self, command: Literal["get", "describe", "subset"], snapshot
    ):
        if command == "describe":
            model = CopernicusMarineCatalogue
        else:
            if command == "get":
                model = ResponseGet
            else:
                model = ResponseSubset

        available_fields = _return_available_fields(model)

        assert sorted(list(available_fields)) == snapshot
