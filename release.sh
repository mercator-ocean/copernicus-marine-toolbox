#!/bin/bash

set -eufo pipefail

RELEASE_WATCHER_JIRA_ID="70121:ac53a7e4-e096-45f2-9408-118133f1c9ca"
RELEASE_ISSUE_PREFIX="New copernicusmarine package release "
PYPI_URL="https://pypi.org/project/copernicusmarine/${VERSION}/"
LAST_RELEASE_COMMIT=$(git log --grep "${RELEASE_ISSUE_PREFIX}" -n 1 --pretty=format:'%h')

if [[ -z "$LAST_RELEASE_COMMIT" ]]; then
    RELATED_CMCD_ISSUE_KEYS=$(git log | grep -E "CMCD-[0-9]+" -wo | xargs | sed 's/ /,/g')
else
    RELATED_CMCD_ISSUE_KEYS=$(git log "${LAST_RELEASE_COMMIT}..HEAD" | grep -E "CMCD-[0-9]+" -wo | xargs | sed 's/ /,/g')
fi

RELATED_CMCS_ISSUE_KEYS=$(moi related-issues --issue-keys "${RELATED_CMCD_ISSUE_KEYS}" | grep -E "^CMCS-[0-9]+" -wo || echo "")
RELEASE_DESCRIPTION="Pypi link: ${PYPI_URL}"
MOI_TOOL=$(command -v moi)

git switch main
git pull
if [ ! -z "${MOI_TOOL}" ] && [ -z `git status --porcelain` ] && [ ! -z "${VERSION}" ] && [ ! -z "${PYPI_TOKEN}" ] ; then
  echo "Starting release..."
  moi new -k CMCD --summary "New copernicusmarine package release ${VERSION}" --epic-key CMCD-96 --description "${RELEASE_DESCRIPTION}" --watchers "${RELEASE_WATCHER_JIRA_ID}" --related-issue-keys "${RELATED_CMCD_ISSUE_KEYS}" --sprint-project-key DO --start
  poetry version ${VERSION}
  git commit -am "`moi commit-message`"
  moi review
  poetry publish --build --username __token__ --password "${PYPI_TOKEN}"
  moi merge --force-ci-pipeline-not-yet-successful
  rm -rf dist/
  for cmcs in ${RELATED_CMCS_ISSUE_KEYS}
  do
    status=`moi details --issue-key "${cmcs}"| grep -o -P '(?<=status: ).*'`
    case "${status}" in
      "Cancelled"|"Done")
        echo "${cmcs} is ${status}"
        ;;
      *)
        echo "Updating ${cmcs}..."
        moi comment-issue --issue-key "${cmcs}" --comment "[AUTOMATIC] Some related issues are part of release ${VERSION}. Moving to In Progress"
        moi change-issue-status --issue-key "${cmcs}" --new-status "In Progress"
        ;;
    esac
  done
  echo "Release ${VERSION} done."
else
  git status
  echo "Release ${VERSION} aborted. Install the 'moi' tool, clean your repository and specify VERSION and PYPI_TOKEN."
fi
