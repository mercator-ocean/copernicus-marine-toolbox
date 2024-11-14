#!/bin/bash

set -eufo pipefail

PRE_RELEASE_BRANCH=$(git branch --show-current)

if [ ! -z "${BUMP_TYPE}" ] && [[ "${BUMP_TYPE}" == pre* ]]; then
  poetry version ${BUMP_TYPE}
  VERSION=$(poetry version --short)
  if [ "${PRE_RELEASE_BRANCH}" != "pre-releases/${VERSION}" ]; then
    echo "Branch name should be pre-releases/${VERSION} instead of ${PRE_RELEASE_BRANCH}"
    exit 1
  fi
  RELEASE_BRANCH_NAME="New-copernicusmarine-package-pre-release"
  git checkout -b $RELEASE_BRANCH_NAME
  RELEASE_TITLE="Copernicus Marine Pre-Release ${VERSION}"
  git commit -am "$RELEASE_TITLE"
  git push --set-upstream origin $RELEASE_BRANCH_NAME
  PR_LINK=$(gh pr create --title "$RELEASE_TITLE" --body "" --base "${PRE_RELEASE_BRANCH}")
  gh pr view $PR_LINK --web
else
  git status
  echo "Release aborted. Clean your repository and use make pre-release-[BUMP_TYPE]."
fi
