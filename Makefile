PROJECT_NAME = copernicusmarine

ENVIRONMENT_NAME = ${PROJECT_NAME}
ENVIRONMENT_FILE_NAME = conda_environment.yaml
TEST_ENVIRONMENT_NAME = ${PROJECT_NAME}_test
TEST_ENVIRONMENT_FILE_NAME = conda_environment_test.yaml
TEST_TOX_ENVIRONMENT_NAME = ${PROJECT_NAME}_test_tox
TEST_TOX_ENVIRONMENT_FILE_NAME = conda_environment_test_tox.yaml

.ONESHELL:
.SHELLFLAGS = -ec
SHELL := /bin/bash

MICROMAMBA_ACTIVATE=eval "$$(micromamba shell hook --shell=bash)" && micromamba activate && micromamba activate
CONDA_ACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh && conda activate && conda activate
ACTIVATE_ENVIRONMENT=${MICROMAMBA_ACTIVATE} ${SELECTED_ENVIRONMENT_NAME} || ${CONDA_ACTIVATE} ${SELECTED_ENVIRONMENT_NAME}

define conda-command
	micromamba $1 || mamba $1 || conda $1
endef

create-update-environment:
	export CONDARC=.condarc
	($(call conda-command, env update --file ${SELECTED_ENVIRONMENT_FILE_NAME} --name ${SELECTED_ENVIRONMENT_NAME}) \
		|| $(call conda-command, update --file ${SELECTED_ENVIRONMENT_FILE_NAME} --name ${SELECTED_ENVIRONMENT_NAME}) \
		|| $(call conda-command, env create --file ${SELECTED_ENVIRONMENT_FILE_NAME} --name ${SELECTED_ENVIRONMENT_NAME}))

create-environment: SELECTED_ENVIRONMENT_NAME = ${ENVIRONMENT_NAME}
create-environment: SELECTED_ENVIRONMENT_FILE_NAME = ${ENVIRONMENT_FILE_NAME}
create-environment: create-update-environment
		$(call conda-command, run --name ${ENVIRONMENT_NAME} poetry install)

create-test-environment: SELECTED_ENVIRONMENT_NAME = ${TEST_ENVIRONMENT_NAME}
create-test-environment: SELECTED_ENVIRONMENT_FILE_NAME = ${TEST_ENVIRONMENT_FILE_NAME}
create-test-environment: create-update-environment

create-test-environment-tox: SELECTED_ENVIRONMENT_NAME = ${TEST_TOX_ENVIRONMENT_NAME}
create-test-environment-tox: SELECTED_ENVIRONMENT_FILE_NAME = ${TEST_TOX_ENVIRONMENT_FILE_NAME}
create-test-environment-tox: create-update-environment

check-format: SELECTED_ENVIRONMENT_NAME = ${ENVIRONMENT_NAME}
check-format:
	${ACTIVATE_ENVIRONMENT}
	pre-commit run --all-files --show-diff-on-failure

run-tests: SELECTED_ENVIRONMENT_NAME = ${TEST_ENVIRONMENT_NAME}
run-tests:
	${ACTIVATE_ENVIRONMENT}
	pip install --editable .
	pytest tests --verbose -vv --durations=0 --log-cli-level=info --basetemp="tests/downloads" --junitxml=report.xml --log-format "%(asctime)s %(levelname)s %(message)s" --log-date-format "%Y-%m-%d %H:%M:%S"

run-tests-dependencie-versions: SELECTED_ENVIRONMENT_NAME = ${TEST_TOX_ENVIRONMENT_NAME}
run-tests-dependencie-versions:
	${ACTIVATE_ENVIRONMENT}
	tox run

run-using-pyinstaller-windows-latest:
	python -m PyInstaller --copy-metadata xarray --name copernicusmarine.exe --add-data "c:\hostedtoolcache\windows\python\3.12.6\x64\lib\site-packages\distributed\distributed.yaml;.\distributed" copernicusmarine/command_line_interface/copernicus_marine.py --onefile

run-using-pyinstaller-macos-latest:
	python -m PyInstaller --name copernicusmarine_macos-latest.cli copernicusmarine/command_line_interface/copernicus_marine.py --onefile --target-architecture=arm64

run-using-pyinstaller-macos-13:
	python -m PyInstaller --name copernicusmarine_macos-13.cli copernicusmarine/command_line_interface/copernicus_marine.py --onefile --target-architecture=x86_64

run-using-pyinstaller-ubuntu-latest:
	python3 -m PyInstaller --name copernicusmarine_ubuntu-latest.cli --add-data="/opt/hostedtoolcache/Python/3.12.6/x64/lib/python3.12/site-packages/distributed/distributed.yaml:./distributed"  copernicusmarine/command_line_interface/copernicus_marine.py --onefile --path /opt/hostedtoolcache/Python/3.12.6/x64/lib/python3.12/site-packages --copy-metadata xarray
	chmod +rwx ./dist/copernicusmarine_ubuntu-latest.cli

release: SELECTED_ENVIRONMENT_NAME = ${ENVIRONMENT_NAME}
release:
	${ACTIVATE_ENVIRONMENT}
	BUMP_TYPE=${BUMP_TYPE} ./release.sh

release-patch: BUMP_TYPE = patch
release-patch: release

release-minor: BUMP_TYPE = minor
release-minor: release

release-major: BUMP_TYPE = major
release-major: release

pre-release:
	${ACTIVATE_ENVIRONMENT}
	BUMP_TYPE=${BUMP_TYPE} ./pre-release.sh

pre-release-patch: BUMP_TYPE = prepatch
pre-release-patch: pre-release

pre-release-minor: BUMP_TYPE = preminor
pre-release-minor: pre-release

pre-release-major: BUMP_TYPE = premajor
pre-release-major: pre-release

pre-release-bump-release: BUMP_TYPE = prerelease
pre-release-bump-release: pre-release


build-and-publish-dockerhub-image:
	docker login --username $${DOCKER_HUB_USERNAME} --password $${DOCKER_HUB_PUSH_TOKEN}
	docker build --ulimit nofile=65536:65536 --tag copernicusmarine/copernicusmarine:$${VERSION} --tag copernicusmarine/copernicusmarine:latest -f Dockerfile.dockerhub --build-arg VERSION="$${VERSION}" .
	docker push copernicusmarine/copernicusmarine:$${VERSION}
	docker push copernicusmarine/copernicusmarine:latest

build-and-prepare-for-binary:
	python -m pip install --upgrade pip
	pip install pyinstaller
	pip install -e .
	pip install poetry
	pip install distributed
	echo "VERSION=$$(poetry version --short)" >> ${GITHUB_OUTPUT}

update-snapshots-tests:
	pytest --snapshot-update tests/test_command_line_interface.py::TestCommandLineInterface::test_describe_including_datasets
