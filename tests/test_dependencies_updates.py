from toml import load


class TestDependeciesUpdates:
    def test_update_dependencies(self, snapshot):

        with open("pyproject.toml") as f:
            config = load(f)

        # If these change, update the dependencies in the documentation!
        assert config["tool"]["poetry"]["dependencies"] == snapshot
        # just for the sake of the test, we will also check the dev-dependencies
        assert config["tool"]["poetry"]["dev-dependencies"] == snapshot
