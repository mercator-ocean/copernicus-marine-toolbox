# Contribute

After any implementation:

- add test/ documentation on new functionality if relevant
(the tests should be located in the "tests" folder and follow the pytest conventions)

- add necessary module to "pyproject.toml" in [tool.poetry.dependencies] section

- run pre-commit before committing:

    ```
    pre-commit run --all-files
    ```

- run tests

## Development

The development workflow can be found [here](https://mercatoroceanfr.sharepoint.com/sites/CopernicusMarineClient/_layouts/15/Doc.aspx?sourcedoc={2ec87d9f-10c5-4451-9835-e9ea5b5be72e}&action=edit&wd=target%28Process.one%7Cb6552011-5a7d-404c-b256-6609f34bd291%2FDevelopment%20workflow%7Cd090c2e1-e118-48ff-b26c-1a399a58c457%2F%29&wdorigin=703).

In your Merge Request description or in comment, ping @internal/copernicus-marine-service/copernicus-marine-toolbox/codeowners to request review and approval from other developers.
If you ping only one person (or pick only one official reviewer), it is possible that person is not available to review you work, let's just increase your chance to get your work merged as soon as possible.

## Run tests on Linux

Create a test conda environment:
```
make conda-create-test-env
```
Then activate this test environment:
```
conda activate {name_of_the_test_environment}
```
Export credentials to local variables (if you don't use `moi`, simply put your own credentials):
```
export COPERNICUSMARINE_SERVICE_USERNAME=$(moi read-secret --name COPERNICUSMARINE_SERVICE_USERNAME)
export COPERNICUSMARINE_SERVICE_PASSWORD=$(moi read-secret --name COPERNICUSMARINE_SERVICE_PASSWORD)
```
Finally run the tests:
```
make run-tests
```
> **_NOTE:_**  `test_login_is_prompt_when_configuration_file_doest_not_exist`  will fail.

## Run tests on Mac/OS

Create a test conda environment:
```
conda env create --name copernicusmarine-test --file environment_test.yaml
```
Then activate this test environment:
```
conda activate copernicusmarine-test
```
Install CLI:
```
pip install --editable .
```
Export credentials to local variables (if you don't use `moi`, simply put your own credentials):
```
export COPERNICUSMARINE_SERVICE_USERNAME=$(moi read-secret --name COPERNICUSMARINE_SERVICE_USERNAME)
export COPERNICUSMARINE_SERVICE_PASSWORD=$(moi read-secret --name COPERNICUSMARINE_SERVICE_PASSWORD)
```
Finally run the tests:
```
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
