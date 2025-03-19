PROJECT_NAME = copernicusmarine

ENVIRONMENT_NAME = ${PROJECT_NAME}
ENVIRONMENT_FILE_NAME = conda_environment.yaml
TEST_ENVIRONMENT_NAME = ${PROJECT_NAME}_test
TEST_ENVIRONMENT_FILE_NAME = conda_environment_test.yaml

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

check-format: SELECTED_ENVIRONMENT_NAME = ${ENVIRONMENT_NAME}
check-format:
	${ACTIVATE_ENVIRONMENT}
	pre-commit run --all-files --show-diff-on-failure

run-tests: SELECTED_ENVIRONMENT_NAME = ${TEST_ENVIRONMENT_NAME}
run-tests:
	${ACTIVATE_ENVIRONMENT}
	pip install --editable .
	pytest tests --verbose -vv --durations=0 --log-cli-level=info --basetemp="tests/downloads" --junitxml=report.xml --log-format "%(asctime)s %(levelname)s %(message)s" --log-date-format "%Y-%m-%d %H:%M:%S"

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


##  Binaries creation:
# Prepare the environment
build-and-prepare-for-binary:
	python -m pip install --upgrade pip
	pip install pyinstaller
	pip install -e .
	pip install poetry
	pip install distributed
	echo "VERSION=$$(poetry version --short)" >> ${GITHUB_OUTPUT}
# Build with macos windows and linux
run-using-pyinstaller-windows-latest:
	pip install -e .
	python -m PyInstaller --hiddenimport deprecated --copy-metadata copernicusmarine --icon=toolbox_icon.png --copy-metadata xarray --name copernicusmarine.exe --collect-data dask --add-data "C:\Users\runneradmin\micromamba\envs\copernicusmarine-binary\Lib\site-packages\distributed\distributed.yaml;.\distributed" copernicusmarine/command_line_interface/copernicus_marine.py --onefile --copy-metadata zarr

run-using-pyinstaller-macos:
	pip install -e .
	python -m PyInstaller --hiddenimport deprecated --noconfirm --clean --onefile --copy-metadata xarray --name copernicusmarine_macos-${ARCH}.cli  --copy-metada pandas --collect-data dask --collect-data distributed --collect-data tzdata --copy-metadata copernicusmarine copernicusmarine/command_line_interface/copernicus_marine.py --target-architecture=${ARCH} --copy-metadata zarr

run-using-pyinstaller-macos-13: ARCH = x86_64
run-using-pyinstaller-macos-13: run-using-pyinstaller-macos

run-using-pyinstaller-macos-latest: ARCH = arm64
run-using-pyinstaller-macos-latest: run-using-pyinstaller-macos

run-using-pyinstaller-linux:
	pip install -e .
	ldd --version
	which openssl
	openssl version -a
	export LD_LIBRARY_PATH=/home/runner/micromamba/envs/copernicusmarine-binary/lib
	echo $$LD_LIBRARY_PATH
	python3 -m PyInstaller --hiddenimport deprecated --collect-all tzdata --copy-metadata copernicusmarine --name copernicusmarine_${DISTRIBUTION}.cli --collect-data distributed --collect-data dask  copernicusmarine/command_line_interface/copernicus_marine.py --onefile --path /opt/hostedtoolcache/Python/3.12.6/x64/lib/python3.12/site-packages --copy-metadata xarray --copy-metadata zarr
	chmod +rwx /home/runner/work/copernicus-marine-toolbox/copernicus-marine-toolbox/dist/copernicusmarine_${DISTRIBUTION}.cli

run-using-pyinstaller-ubuntu-22.04: DISTRIBUTION = linux-glibc-2.35
run-using-pyinstaller-ubuntu-22.04: run-using-pyinstaller-linux

run-using-pyinstaller-ubuntu-24.04: DISTRIBUTION = linux-glibc-2.39
run-using-pyinstaller-ubuntu-24.04: run-using-pyinstaller-linux

# Tests for the binaries
run-tests-binaries:
	pytest tests_binaries/test_basic_commands_binaries.py -vv --log-cli-level=info --basetemp="tests_binaries/downloads" --junitxml=report.xml --log-format "%(asctime)s %(levelname)s %(message)s" --log-date-format "%Y-%m-%d %H:%M:%S"

change-name-binary:
	mv dist/copernicusmari* ./copernicusmarine.cli

update-tests-snapshots:
	pytest --snapshot-update tests/test_help_command_interface.py
	pytest --snapshot-update tests/test_dependencies_updates.py
	pytest --snapshot-update tests/test_describe_released_date.py
	pytest --snapshot-update tests/test_help_command_interface.py
	pytest --snapshot-update tests/test_query_builder.py::TestQueryBuilder::test_return_available_fields
