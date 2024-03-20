#!/bin/bash

set -eufo pipefail

git switch main
git pull
if [ -z `git status --porcelain` ] && [ ! -z "${VERSION}" ] && [ ! -z "${PYPI_TOKEN}" ] ; then
  RELEASE_BRANCH_NAME="New-copernicusmarine-package-release-${VERSION}"
  echo "Starting release..."
  git checkout -b $RELEASE_BRANCH_NAME
  poetry version ${VERSION}
  git commit -am "New copernicusmarine package release ${VERSION}"
  poetry publish --build --username __token__ --password "${PYPI_TOKEN}"
  git switch main
  git merge $RELEASE_BRANCH_NAME
  rm -rf dist/
  echo "Release ${VERSION} done."
else
  git status
  echo "Release ${VERSION} aborted. Clean your repository and specify VERSION and PYPI_TOKEN."
fi

while [[ $(poetry search copernicusmarine | grep copernicusmarine | awk '{print $2}') != "($VERSION)" ]]
do
  echo "Waiting for version $VERSION to be available on Pypi..."
  sleep 10
done

make build-and-publish-dockerhub-image
