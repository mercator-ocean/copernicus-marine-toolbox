# Contribute

After any implementation:

- add test/ documentation on new functionality if relevant
(the tests should be located in the "tests" folder and follow the pytest conventions)

- add necessary module to "pyproject.toml" in [tool.poetry.dependencies] section

- run pre-commit before committing:

    ``` sh
    pre-commit run --all-files
    ```

- run tests

## Run tests on Linux

Create a test conda environment:

```sh
make create-test-environment
```

Then activate this test environment:

```sh
conda activate copernicusmarine_test
```

Export credentials to local variables (if you don't use `moi`, simply put your own credentials):

```sh
export COPERNICUSMARINE_SERVICE_USERNAME=$(moi read-secret --name COPERNICUSMARINE_SERVICE_USERNAME)
export COPERNICUSMARINE_SERVICE_PASSWORD=$(moi read-secret --name COPERNICUSMARINE_SERVICE_PASSWORD)
```

Finally run the tests:

```sh
make run-tests
```

> **_NOTE:_**  `test_login_is_prompt_when_configuration_file_doest_not_exist`  will fail.

## Run tests on Mac/OS

Create a test conda environment:

```sh
conda env create --name copernicusmarine-test --file environment_test.yaml
```

Then activate this test environment:

```sh
conda activate copernicusmarine-test
```

Install CLI:

```sh
pip install --editable .
```

Export credentials to local variables (if you don't use `moi`, simply put your own credentials):

```sh
export COPERNICUSMARINE_SERVICE_USERNAME=$(moi read-secret --name COPERNICUSMARINE_SERVICE_USERNAME)
export COPERNICUSMARINE_SERVICE_PASSWORD=$(moi read-secret --name COPERNICUSMARINE_SERVICE_PASSWORD)
```

Finally run the tests:

```sh
pytest tests --log-level -vv tests --durations=0 --log-level=info
```

> **_NOTE:_**  `test_login_is_prompt_when_configuration_file_doest_not_exist`  will fail.

## Publish new release version

If you have the [`moi`](https://gitlab.mercator-ocean.fr/internal/shell-utils) command installed:

```sh
VERSION=<VERSION> DOCKER_HUB_USERNAME=`moi read-secret --name DOCKER_HUB_USERNAME` DOCKER_HUB_PUSH_TOKEN=`moi read-secret --name DOCKER_HUB_PUSH_TOKEN` PYPI_TOKEN=`moi read-secret --name PYPI_TOKEN` make release
```

Otherwise:

```sh
git switch main
poetry version $VERSION
poetry publish --build --username __token__ --password $PYPI_TOKEN
```

Then tag the appropriate persons and add the changelog the Jira issue, before merging the branch.

## Build the Docker image

If you have the [`moi`](https://gitlab.mercator-ocean.fr/internal/shell-utils) command installed:

```sh
VERSION=<VERSION> DOCKER_HUB_USERNAME=`moi read-secret --name DOCKER_HUB_USERNAME` DOCKER_HUB_PUSH_TOKEN=`moi read-secret --name DOCKER_HUB_PUSH_TOKEN` make build-and-publish-dockerhub-image
```

## Update the conda-forge feedstock repository

First, here is the link to the conda-forge feedstock repository: [https://github.com/conda-forge/copernicusmarine-feedstock](https://github.com/conda-forge/copernicusmarine-feedstock).

All the conda-forge informations about this repository are available [here in the README](https://github.com/orgs/conda-forge/teams/copernicusmarine). To update it (new version, new maintainer...), please follow the indicated procedure.

Please also take a look at [this conda-forge documentation](https://conda-forge.org/docs/maintainer/updating_pkgs/#example-workflow-for-updating-a-package) for more information about the update procedure.
