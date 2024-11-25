from copernicusmarine.catalogue_parser.fields_query_builder import QueryBuilder
from copernicusmarine.catalogue_parser.models import CopernicusMarineCatalogue
from copernicusmarine.core_functions.models import ResponseGet, ResponseSubset


class TestQueryBuilder:
    # For the catalogue
    def test_query_builder_catalogue(self):
        query = QueryBuilder({"product_id", "dataset_id"}).build_query(
            CopernicusMarineCatalogue
        )
        assert query == {
            "products": {
                "__all__": {
                    "product_id": True,
                    "datasets": {"__all__": {"dataset_id": True}},
                }
            }
        }

    # For the get
    def test_query_builder_responseget(self):
        query = QueryBuilder({"files", "files_deleted"}).build_query(
            ResponseGet
        )
        assert query == {"files": True, "files_deleted": True}

    # For the subset
    def test_basic_query_builder_responsesubset(self):
        query = QueryBuilder({"file_path", "output_directory"}).build_query(
            ResponseSubset
        )
        assert query == {"file_path": True, "output_directory": True}

    def test_subset_optional_float(self):
        query = QueryBuilder({"file_size", "data_transfer_size"}).build_query(
            ResponseSubset
        )
        print(query)
        assert query == {"file_size": True, "data_transfer_size": True}

    def test_subset_optional_status(self):
        query = QueryBuilder({"status", "file_status"}).build_query(
            ResponseSubset
        )
        print(query)
        assert query == {"status": True, "file_status": True}

    # Add here the one with the union later when merged!
