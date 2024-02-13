PROJECT_NAME = copernicusmarine

ENVIRONMENT_NAME = ${PROJECT_NAME}
ENVIRONMENT_FILE_NAME = conda_environment.yaml
TEST_ENVIRONMENT_NAME = ${PROJECT_NAME}_test
TEST_ENVIRONMENT_FILE_NAME = conda_environment_test.yaml
TEST_TOX_ENVIRONMENT_FILE_NAME = conda_environment_test_tox.yaml

.ONESHELL:
.SHELLFLAGS = -ec
SHELL := /bin/bash

MICROMAMBA_ACTIVATE=eval "$$(micromamba shell hook --shell=bash)" && micromamba activate && micromamba activate
CONDA_ACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh && conda activate && conda activate
ACTIVATE_ENVIRONMENT=${MICROMAMBA_ACTIVATE} ${SELECTED_ENVIRONMENT_NAME} || ${CONDA_ACTIVATE} ${SELECTED_ENVIRONMENT_NAME}
BUILD_ENVIRONMENT_CHECKSUM=$$(sha256sum ${ENVIRONMENT_FILE_NAME} .pre-commit-config.yaml .condarc pip.conf Dockerfile.ci | sha256sum -z | cut -d ' ' -f 1)
BUILD_ENVIRONMENT_CHECKSUM_TESTS=$$(sha256sum ${TEST_ENVIRONMENT_FILE_NAME} .condarc pip.conf Dockerfile.ci.tests | sha256sum -z | cut -d ' ' -f 1)
BUILD_ENVIRONMENT_CHECKSUM_TESTS_TOX=$$(sha256sum ${TEST_TOX_ENVIRONMENT_FILE_NAME} .condarc pip.conf Dockerfile.ci.tests.tox | sha256sum -z | cut -d ' ' -f 1)

REGISTRY="docker.mercator-ocean.fr"
REGISTRY_URI="https://${REGISTRY}"
REGISTRY_REPOSITORY="${REGISTRY}/moi-docker"
REGISTRY_USERNAME="ci-robot"

define conda-command
	micromamba $1 || mamba $1 || conda $1
endef

# Make sure you use it for nothing but networking stuff (think about race conditons)
# # Example: $(call retry,3,some_script.sh)
retry = $(2) $(foreach t,$(shell seq 1 ${1}),|| (echo -e "\033[33m Failed ($$?): '$(2)'\n Retrying $t ... \033[0m"; $(2)))"]]"))

create-update-environment:
	export CONDARC=.condarc
	export PIP_CONFIG_FILE=pip.conf
	($(call conda-command, env update --file ${SELECTED_ENVIRONMENT_FILE_NAME} --name ${SELECTED_ENVIRONMENT_NAME}) \
		|| $(call conda-command, update --file ${SELECTED_ENVIRONMENT_FILE_NAME} --name ${SELECTED_ENVIRONMENT_NAME}) \
		|| $(call conda-command, env create --file ${SELECTED_ENVIRONMENT_FILE_NAME} --name ${SELECTED_ENVIRONMENT_NAME}))

create-environment: SELECTED_ENVIRONMENT_NAME = ${ENVIRONMENT_NAME}
create-environment: SELECTED_ENVIRONMENT_FILE_NAME = ${ENVIRONMENT_FILE_NAME}
create-environment: create-update-environment
		$(call conda-command, run --name ${ENVIRONMENT_NAME} poetry install)

check-format: SELECTED_ENVIRONMENT_NAME = ${ENVIRONMENT_NAME}
check-format:
	${ACTIVATE_ENVIRONMENT}
	pre-commit run --all-files --show-diff-on-failure

get_ci_image:
	@echo ${REGISTRY_REPOSITORY}/${PROJECT_NAME}-ci:${BUILD_ENVIRONMENT_CHECKSUM}

get_ci_image_tests:
	@echo ${REGISTRY_REPOSITORY}/${PROJECT_NAME}-ci-tests:${BUILD_ENVIRONMENT_CHECKSUM_TESTS}

get_ci_image_tests_tox:
	@echo ${REGISTRY_REPOSITORY}/${PROJECT_NAME}-ci-tests-tox:${BUILD_ENVIRONMENT_CHECKSUM_TESTS_TOX}

build-and-publish-image:
	docker login ${REGISTRY_URI} --username ${REGISTRY_USERNAME} --password $${REGISTRY_PASSWORD}
	@if docker manifest inspect ${REGISTRY_REPOSITORY}/${CONTAINER_IMAGE_NAME} > /dev/null ; then
		echo "The image already exist on nexus"
	else
		echo "The image does not exists on nexus"
		docker build --ulimit nofile=65536:65536 --tag ${REGISTRY_REPOSITORY}/${CONTAINER_IMAGE_NAME} -f ${CONTAINER_IMAGE_DOCKERFILE} --build-arg REGISTRY="${REGISTRY}/" .
		docker push ${REGISTRY_REPOSITORY}/${CONTAINER_IMAGE_NAME}
	fi

build-and-publish-ci-image: CONTAINER_IMAGE_NAME = ${PROJECT_NAME}-ci:${BUILD_ENVIRONMENT_CHECKSUM}
build-and-publish-ci-image: CONTAINER_IMAGE_DOCKERFILE = Dockerfile.ci
build-and-publish-ci-image: build-and-publish-image

build-and-publish-ci-image-tests: CONTAINER_IMAGE_NAME = ${PROJECT_NAME}-ci-tests:${BUILD_ENVIRONMENT_CHECKSUM_TESTS}
build-and-publish-ci-image-tests: CONTAINER_IMAGE_DOCKERFILE = Dockerfile.ci.tests
build-and-publish-ci-image-tests: build-and-publish-image

build-and-publish-ci-image-tests-tox: CONTAINER_IMAGE_NAME = ${PROJECT_NAME}-ci-tests-tox:${BUILD_ENVIRONMENT_CHECKSUM_TESTS_TOX}
build-and-publish-ci-image-tests-tox: CONTAINER_IMAGE_DOCKERFILE = Dockerfile.ci.tests.tox
build-and-publish-ci-image-tests-tox: build-and-publish-image

create-test-environment: SELECTED_ENVIRONMENT_NAME = ${TEST_ENVIRONMENT_NAME}
create-test-environment: SELECTED_ENVIRONMENT_FILE_NAME = ${TEST_ENVIRONMENT_FILE_NAME}
create-test-environment: create-update-environment

create-test-environment-tox: SELECTED_ENVIRONMENT_NAME = ${TEST_ENVIRONMENT_NAME}-tox
create-test-environment-tox: SELECTED_ENVIRONMENT_FILE_NAME = ${TEST_TOX_ENVIRONMENT_FILE_NAME}
create-test-environment-tox: create-update-environment

run-tests: SELECTED_ENVIRONMENT_NAME = ${TEST_ENVIRONMENT_NAME}
run-tests:
	${ACTIVATE_ENVIRONMENT}
	pip install --editable .
	pytest tests --verbose -vv --durations=0 --log-cli-level=info --basetemp="tests/downloads" --junitxml=report.xml --log-format "%(asctime)s %(levelname)s %(message)s" --log-date-format "%Y-%m-%d %H:%M:%S"

release: SELECTED_ENVIRONMENT_NAME = ${ENVIRONMENT_NAME}
release:
	${ACTIVATE_ENVIRONMENT}
	PYPI_TOKEN=$${PYPI_TOKEN} VERSION=$${VERSION} ./release.sh

run-tests-dependencie-versions: SELECTED_ENVIRONMENT_NAME = ${TEST_ENVIRONMENT_NAME}
run-tests-dependencie-versions:
	${ACTIVATE_ENVIRONMENT}
	tox run
