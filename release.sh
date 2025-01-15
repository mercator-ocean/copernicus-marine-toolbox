#!/bin/bash

set -eufo pipefail

git switch main
git pull

RELEASE_BRANCH_NAME="New-copernicusmarine-package-release"
if [ -n "$(git branch --list $RELEASE_BRANCH_NAME)" ]; then
  git branch -D $RELEASE_BRANCH_NAME
fi

if [ -z `git status --porcelain` ] && [ ! -z "${BUMP_TYPE}" ] ; then
  git checkout -b $RELEASE_BRANCH_NAME
  poetry version ${BUMP_TYPE}
  VERSION=$(poetry version --short)
  RELEASE_TITLE="Copernicus Marine Release ${VERSION}"
  git commit -am "$RELEASE_TITLE"
  git push --set-upstream origin $RELEASE_BRANCH_NAME
  PR_LINK=$(gh pr create --title "$RELEASE_TITLE" --body "")
  gh pr view $PR_LINK --web
else
  git status
  echo "Release aborted. Clean your repository and use make release-[BUMP_TYPE]."
fi
