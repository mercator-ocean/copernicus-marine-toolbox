from copernicusmarine.catalogue_parser.models import CopernicusMarineCatalogue
from copernicusmarine.core_functions.fields_query_builder import (
    build_query,
    get_queryable_requested_fields,
)
from copernicusmarine.core_functions.models import ResponseGet, ResponseSubset

CATALOGUE_SET = {
    "products",  # list of product attr
    "title",
    "bbox",
    "chunk_geometric_factor",
    "chunk_reference_coordinate",
    "chunk_type",
    "chunking_length",
    "coordinate_id",
    "coordinate_unit",
    "coordinates",
    "dataset_id",
    "dataset_name",
    "datasets",
    "description",
    "digital_object_identifier",
    "keywords",
    "label",
    "maximum_value",
    "minimum_value",
    "name",
    "parts",
    "processing_level",
    "product_id",
    "production_center",
    "released_date",
    "retired_date",
    "service_format",
    "service_name",
    "service_short_name",
    "services",
    "short_name",
    "sources",
    "standard_name",
    "step",
    "thumbnail_url",
    "units",
    "uri",
    "values",
    "variables",
    "versions",
}
GET_SET = {
    "files",  # following the fields of files:
    "s3_url",
    "https_url",
    "file_size",
    "last_modified_datetime",
    "etag",
    "file_format",
    "output_directory",
    "filename",
    "file_path",
    "file_status",
    # We follow RepsonseGet attrs
    "files_deleted",
    "files_not_found",
    "total_size",
    "status",
    "message",
}
SUBSET_SET = {
    "file_path",
    "output_directory",
    "filename",
    "file_size",
    "data_transfer_size",
    "variables",
    "coordinates_extent",  # fields in coordinates extent
    "minimum",
    "maximum",
    "unit",
    "coordinate_id",
    # lastly the last part of the RepsonseSubset
    "status",
    "message",
    "file_status",
}


class TestQueryBuilder:
    # For the catalogue
    def test_query_builder_catalogue(self):
        query = build_query(
            ({"product_id", "dataset_id"}), CopernicusMarineCatalogue
        )
        assert query == {
            "products": {
                "__all__": {
                    "product_id": True,
                    "datasets": {"__all__": {"dataset_id": True}},
                }
            }
        }

    def test_query_builder_catalogue_deep_inside(self):
        query = build_query(
            ({"service_name", "coordinate_unit", "maximum_value"}),
            CopernicusMarineCatalogue,
        )
        assert query == {
            "products": {
                "__all__": {
                    "datasets": {
                        "__all__": {
                            "versions": {
                                "__all__": {
                                    "parts": {
                                        "__all__": {
                                            "services": {
                                                "__all__": {
                                                    "service_name": True,
                                                    "variables": {
                                                        "__all__": {
                                                            "coordinates": {
                                                                "__all__": {
                                                                    "coordinate_unit": True,  # noqa: E501
                                                                    "maximum_value": True,  # noqa: E501
                                                                }
                                                            }
                                                        }
                                                    },
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    # For the get
    def test_query_builder_responseget(self):
        query = build_query(({"files", "files_deleted"}), ResponseGet)
        assert query == {"files": True, "files_deleted": True}

    def test_query_builder_responseget_inside_basemodel(self):
        query = build_query(({"file_status"}), ResponseGet)
        assert query == {"files": {"__all__": {"file_status": True}}}

    # For the subset
    def test_basic_query_builder_responsesubset(self):
        query = build_query(
            ({"file_path", "output_directory"}), ResponseSubset
        )
        assert query == {"file_path": True, "output_directory": True}

    def test_subset_optional_float(self):
        query = build_query(
            ({"file_size", "data_transfer_size"}), ResponseSubset
        )
        assert query == {"file_size": True, "data_transfer_size": True}

    def test_subset_optional_status(self):
        query = build_query(({"status", "file_status"}), ResponseSubset)
        assert query == {"status": True, "file_status": True}

    def test_subset_optional_coordinate_id(self):
        query = build_query(({"coordinate_id"}), ResponseSubset)
        assert query == {
            "coordinates_extent": {"__all__": {"coordinate_id": True}}
        }

    def test_subset_optional_coordinate_id_and_file(self):
        query = build_query(({"coordinate_id", "file_status"}), ResponseSubset)
        assert query == {
            "coordinates_extent": {"__all__": {"coordinate_id": True}},
            "file_status": True,
        }

    def test_return_available_fields(self):
        self.when_I_run_with_model(CATALOGUE_SET)
        self.when_I_run_with_model(GET_SET)
        self.when_I_run_with_model(SUBSET_SET)

    def when_I_run_with_model(self, function):
        if function == CATALOGUE_SET:
            fields = "--return-fields"
            included = {"minimum_value", "one-wrong"}
            model = CopernicusMarineCatalogue
        else:
            fields = "--response-fields"
            included = {"file_path", "output_directory", "one-wrong"}
            if function == GET_SET:
                model = ResponseGet
            else:
                model = ResponseSubset
        queryable_fields = get_queryable_requested_fields(
            included, model, fields
        )

        check_intersection = queryable_fields.intersection(function)
        assert check_intersection == queryable_fields
