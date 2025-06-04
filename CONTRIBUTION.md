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

The Copernicus Marine Toolbox might maintain several versions at the same time. To this end, we create a branch that will be used for bug fixes and maintenance of the older versions: `release/*`. This branch is created when the development of the new major version starts or when patches are needed but the 'main' branch contains already commits that are not relevant for the older version.

Please be aware when you do a fork and create a pull request which of the branch is relevant: `main` or any of `release/*`.

### Development

All developments needs to be done on feature branch. Then, the changes are pushed to the feature branch and a pull request is created to merge the feature branch into the `main` branch.

There is one exception: if you are working on a bug fix for the current version, you can create a pull request to the `release/*` branch if and only if the bug fix is not relevant anymore for the `main` branch. See section below about the releasing a patch.

### Guide to release

You will need:

- [poetry](https://python-poetry.org/docs/)
- A terminal
- [git](https://git-scm.com/)
- [github account](https://github.com/mercator-ocean/copernicus-marine-toolbox) to access the repository

#### Step 0

But sure that `main` is at a clean state with all the commits you want. Also be sure the documentation and the changelog are up to date and the tests are passing.

#### Step 1

Create a new branch from `main` or `release/*` branch.

``` sh
git fetch origin
git checkout main
git pull origin main --rebase
git checkout -b copernicusmarine-release
```

> [!WARNING]
> Do not name your branch `release/*` as it is reserved for the Copernicus Marine Toolbox.

#### Step 2

Update the version in the `pyproject.toml` file. The version should follow the [semantic versioning](https://semver.org/) rules.

Use the make command to update the version:

``` sh
make release-bump-minor
```

You should see the version updated in the `pyproject.toml` file.

Add here some information that might be missing like the changelog, the documentation, etc.

#### Step 3

Commit the changes and push the branch to the remote repository.

``` sh
make add-commit-for-release
git push origin main
```

#### Step 4

Create a pull request to merge the `copernicusmarine-release` branch into the `main` branch.

Name it `Copernicus Marine Toolbox vX.Y.Z` where `X.Y.Z` is the version you want to be released.

#### Step 5

After reviewing the pull request, merge it into the `main` branch.

The actions will be triggered and the new version will be built and published to PyPI.

#### Step 6

After some time, it will be possible to release to new version in conda-forge. To do so, you will need to update the [`conda-forge` feedstock](https://github.com/conda-forge/copernicusmarine-feedstock) repository with the new version.

### Patch releases

If you need to patch a bug in a current version, but `main` is not concerned by the fix or some commits from `main` are not relevant for the current version, you can create a pull request to a `release/*` branch.
This case the steps are really similar except that you will need to create a branch from the `release/*` branch instead of `main`.

#### Patch: Step 1

Create a new branch from the `release/vX.Y` branch from a release tag.

``` sh
git fetch origin
git checkout -b release/vX.Y vX.Y.Z-1
git push origin release/vX.Y
```

Then same as before, create your branch but your base is `release/vX.Y`.

``` sh
git checkout release/vX.Y
git checkout -b copernicusmarine-release
```

#### Patch: Step 2

Bump the version for a patch release. The version should follow the [semantic versioning](https://semver.org/) rules.

``` sh
make release-bump-patch
```

#### Patch: Step 3

> [!IMPORTANT]
> Here, in most cases, you want to cherry-pick the commits from `main` that are relevant for the patch release. You can do this by using the `git cherry-pick` command. However, in other situations, you might just fix the bug for this specific version here. The `release/vX.Y` branch now differs from `main`.

And then we commit.

``` sh
make add-commit-for-release
git push origin main
```

#### Patch: Step 4

Create a pull request to merge the `copernicusmarine-release` branch into the `release/vX.Y` branch.

Name it `Copernicus Marine Toolbox vX.Y.Z+1` where `X.Y.Z+1` is the version you want to be released.

Step 5 and step 6 are the same as before.

### Pre-releases

If you want to create a pre-release the flow is very similarly as the patch releases above. Instead of creating a `release/vX.Y` branch from a tag, we create a `pre-release/VX.Y.Z` branch from main and then create releases from this branch (e.g. `vX.Y.Za0` or  `vX.Y.Zb1`).

> [!NOTE]
> Pre-Releases are not released to conda forge

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

Please see [the admin page of the toolbox.](https://readthedocs.org/projects/copernicusmarine/).

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
