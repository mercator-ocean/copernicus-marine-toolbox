PROJECT_NAME = copernicusmarine

ENVIRONMENT_NAME = ${PROJECT_NAME}
ENVIRONMENT_FILE_NAME = conda_environment.yaml
TEST_ENVIRONMENT_NAME = ${PROJECT_NAME}_test
TEST_ENVIRONMENT_FILE_NAME = conda_environment_test.yaml

RELEASE_COMMIT_MESSAGE = Copernicus Marine Toolbox Release
PRERELEASE_COMMIT_MESSAGE = Copernicus Marine Toolbox Pre-Release

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

run-tests-unix:
	poetry run pytest tests -k "not fast_with_timeout and not ncdump" -n auto --verbose -vv --durations=0 --log-cli-level=info --basetemp="tests/downloads" --junitxml=report.xml --log-format "%(asctime)s %(levelname)s %(message)s" --log-date-format "%Y-%m-%d %H:%M:%S"

run-tests-windows:
	poetry run pytest tests -k "not fast_with_timeout and not test_describe_timeout and not ncdump and not cfcompliant and not prompt" -n auto --verbose -vv --durations=0 --log-cli-level=info --basetemp="tests/downloads" --junitxml=report.xml --log-format "%(asctime)s %(levelname)s %(message)s" --log-date-format "%Y-%m-%d %H:%M:%S"

run-cov-tests:
	poetry run pytest tests -k "not fast_with_timeout" --cov --cov-report xml -n auto --dist=loadgroup --verbose -vv --durations=0 --log-cli-level=info --basetemp="tests/downloads" --junitxml=report.xml --log-format "%(asctime)s %(levelname)s %(message)s" --log-date-format "%Y-%m-%d %H:%M:%S"

run-quick-tests:
	poetry run pytest tests -k "fast_with_timeout" -n auto --dist=loadgroup --verbose -vv --durations=0 --log-cli-level=info --basetemp="tests/downloads" --junitxml=report.xml --log-format "%(asctime)s %(levelname)s %(message)s" --log-date-format "%Y-%m-%d %H:%M:%S"


release-bump-patch:
	poetry version patch

release-bump-minor:
	poetry version minor

release-bump-major:
	poetry version major

pre-release-bump-patch:
	poetry version prepatch

pre-release-bump-minor:
	poetry version preminor

pre-release-bump-major:
	poetry version premajor

add-commit-for-release:
	@echo "Adding commit for release"
	git add .
	@VERSION=$$(poetry version --short); \
	git commit -m "${RELEASE_COMMIT_MESSAGE} v$$VERSION"

add-commit-for-pre-release:
	@echo "Adding commit for pre-release"
	git add .
	@VERSION=$$(poetry version --short); \
	git commit -m "${PRERELEASE_COMMIT_MESSAGE} v$$VERSION"


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

run-using-pyinstaller-windows-latest:
	pip install -e .
	python -m PyInstaller --hiddenimport deprecated --hidden-import numpy --hidden-import numpy._core._exceptions --collect-submodules=numpy --copy-metadata copernicusmarine --icon=toolbox_icon.png --copy-metadata xarray --name copernicusmarine.exe --collect-data dask --add-data "C:\Users\runneradmin\micromamba\envs\copernicusmarine-binary\Lib\site-packages\distributed\distributed.yaml;.\distributed" copernicusmarine/command_line_interface/copernicus_marine.py --onefile --copy-metadata zarr

run-using-pyinstaller-macos:
	pip install -e .
	python -m PyInstaller --hiddenimport deprecated --hidden-import numpy --noconfirm --clean --onefile --copy-metadata xarray --name copernicusmarine_macos-${ARCH}.cli  --copy-metada pandas --collect-data dask --collect-data distributed --collect-data tzdata --copy-metadata copernicusmarine copernicusmarine/command_line_interface/copernicus_marine.py --target-architecture=${ARCH} --copy-metadata zarr

run-using-pyinstaller-macos-15-intel: ARCH = x86_64
run-using-pyinstaller-macos-15-intel: run-using-pyinstaller-macos

run-using-pyinstaller-macos-latest: ARCH = arm64
run-using-pyinstaller-macos-latest: run-using-pyinstaller-macos

run-using-pyinstaller-linux:
	pip install -e .
	ldd --version
	which openssl
	openssl version -a
	export LD_LIBRARY_PATH=/home/runner/micromamba/envs/copernicusmarine-binary/lib
	echo $$LD_LIBRARY_PATH
	python3 -m PyInstaller --hiddenimport deprecated --hidden-import numpy --collect-all tzdata --copy-metadata copernicusmarine --name copernicusmarine_${DISTRIBUTION}.cli --collect-data distributed --collect-data dask  copernicusmarine/command_line_interface/copernicus_marine.py --onefile --path /opt/hostedtoolcache/Python/3.12.6/x64/lib/python3.12/site-packages --copy-metadata xarray --copy-metadata zarr
	chmod +rwx /home/runner/work/copernicus-marine-toolbox/copernicus-marine-toolbox/dist/copernicusmarine_${DISTRIBUTION}.cli

run-using-pyinstaller-ubuntu-22.04: DISTRIBUTION = linux-glibc-2.35
run-using-pyinstaller-ubuntu-22.04: run-using-pyinstaller-linux

run-using-pyinstaller-ubuntu-24.04: DISTRIBUTION = linux-glibc-2.39
run-using-pyinstaller-ubuntu-24.04: run-using-pyinstaller-linux

run-tests-binaries:
	pytest tests_extra/test_basic_commands_binaries.py -vv --log-cli-level=info --basetemp="tests_extra/downloads" --junitxml=report.xml --log-format "%(asctime)s %(levelname)s %(message)s" --log-date-format "%Y-%m-%d %H:%M:%S"

change-name-binary:
	mv dist/copernicusmari* ./copernicusmarine.cli

update-tests-snapshots:
	pytest --snapshot-update tests/test_help_command_interface.py
	pytest --snapshot-update tests/test_dependencies_updates.py
	pytest --snapshot-update tests/test_describe_released_date.py
	pytest --snapshot-update tests/test_query_builder.py::TestQueryBuilder::test_return_available_fields
	pytest --snapshot-update tests/test_request_files.py::TestRequestFiles::test_subset_request_with_request_file
	pytest --snapshot-update tests/test_cf_compliance.py
	pytest --snapshot-update tests/test_subset_split_on.py
