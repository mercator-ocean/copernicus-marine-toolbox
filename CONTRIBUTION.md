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

## Release Process

The Copernicus Marine Toolbox might maintain several versions at the same time. To this end, we create a branch that will be used for bug fixes and maintenance of the older versions: `release/*`. This branch is created when the development of the new major version starts.

Please be aware when you do a fork and create a pull request which of the branch is relevant: `main` or any of `release/*`.

## Tests

Note that some tests use snapshots (see [syrupy](https://github.com/syrupy-project/syrupy)) hence if your code has an influence on something tracked by snapshots you will need to update the snapshots using something like:

``` bash
# replace with the test you need
pytest --snapshot-update tests/test_help_command_interface.py
```

### Run tests on Linux

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

### Run tests on Mac/OS

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

## Documentation

We use sphinx and read the docs to respectively build and distribute the documentation of the toolbox.

### Sphinx

We use:

- autodoc: to create the documentation from the docstrings of the Python interface or the comments in the models
- numpydoc: to convert numpydoc documentation to restructuresText
- sphinx-click: to generate the documentation from the click CLI
- furo: as a base template

The configuration of sphinx can be found in the `doc/conf.py` file and the versions are in the `conda_environment_sphinx.yaml` file. The `_build` folder is gitignored.

To build the documentation do:

```bash
cd doc/
make html
```

### ReadTheDocs

Please see the admin page of the toolbox: [here](https://readthedocs.org/projects/copernicusmarine/).

To access admin rights, you need to be added to the readthedocs project (after creating an account). For the moment Mathis and Simon are the admin of this page.

Example: [toolbox documentation](https://copernicusmarine.readthedocs.io)

Readthedocs have a webhook on the copernicusmarine repo and is triggered when: a commit is pushed, a tag is created and other events.

We defined some automatisation processes that listen to these events and trigger some actions.

- If a tag of a release is pushed (eg v1.3.3) then the doc is built and the v1.3.3 will be available and the default documentation ie any user going to the root of the documentation [https://copernicusmarine.readthedocs.io](https://copernicusmarine.readthedocs.io) will be directed to the newest version: `copernicusmarine.readthedocs.io/en/v1.3.3`.
- If a tag of a pre-release is pushed (eg v2.0.0a1) then the doc is built and the v2.0.0a1 is available but won't be the default one.

## Miscellaneous

### About poetry.lock

If you do any action that leads to a `poetry lock` then you might enconter an infinite loop: `Resolving dependencies... (1232.3s)`.

It seems to be a known issue due to the use of `botocore` and `urllib3`.

Here the [issue on `poetry` repository](https://github.com/orgs/python-poetry/discussions/7937) and the [issue on the `botocore` repository](https://github.com/boto/botocore/issues/2926).

It seems that one work around is setting `urllib3<2`. So when you want to do `poetry add` or `poetry lock` follow this instructions (it suppose you use `poetry>=2.0.0`):

- In the "pyproject.toml" add `urllib3 = "<2.0"` at the end of the list of dependencies.
- Run your command: eg `poetry add pendulum`.
- Delete the line `urllib3 = "<2.0"` in the "pyproject.toml".
- Run `poetry lock`.
