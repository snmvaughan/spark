#!/usr/bin/env bash

set -eux -o pipefail

export PATH=$PATH:$JAVA_HOME/bin

# Figure out where the project root is
PROJECT_ROOT=$(git -C "$(dirname $0)" rev-parse --show-toplevel)

MVN="$PROJECT_ROOT/build/mvn"

function exit_with_usage {
  set +x
  echo ""
  echo "bump-version.sh - tool for committing version increment"
  echo ""
  echo "usage:"
  cl_options="[--project-root <path>] [--mvn <mvn-command>]"
  echo "bump-version.sh $cl_options"
  echo ""
  exit 1
}

# Parse arguments
while (( "$#" )); do
  case $1 in
    --project-root)
      PROJECT_ROOT=$(readlink -f "$2")
      shift
      ;;
    --mvn)
      MVN=$(readlink -f "$2")
      shift
      ;;
    --help)
      exit_with_usage
      ;;
    --*)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
    -*)
      break
      ;;
    *)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
  esac
  shift
done


if [ -z "$JAVA_HOME" ]; then
  # Fall back on JAVA_HOME from rpm, if found
  if command -v  rpm; then
    RPM_JAVA_HOME="$(rpm -E %java_home 2>/dev/null)"
    if [ "$RPM_JAVA_HOME" != "%java_home" ]; then
      JAVA_HOME="$RPM_JAVA_HOME"
      echo "No JAVA_HOME set, proceeding with '$JAVA_HOME' learned from rpm"
    fi
  fi
fi

if [ -z "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set, cannot proceed."
  exit -1
fi

if ! command -v "$MVN" ; then
  echo -e "Could not locate Maven command: '$MVN'."
  echo -e "Specify the Maven command with the --mvn flag"
  exit -1
fi

pushd "$PROJECT_ROOT"

# Check whether the directory is clean. E.g. pending merges, uncommited changes, etc...
# https://unix.stackexchange.com/a/394674
git update-index --really-refresh || { popd; set +x; echo ""; echo "FAILED! The directory is unclean, please check the git status."; exit 1; }
git diff-index --quiet HEAD || { popd; set +x; echo ""; echo "FAILED! The directory is unclean, please check the git status."; exit 1; }

OLD_VERSION=$($MVN help:evaluate -Dexpression=project.version -q -DforceStdout)
OLD_RELEASE_VERSION=${OLD_VERSION%-SNAPSHOT}

# Increment the last digit of the version
# E.g. 3.2.0.66-apple-SNAPSHOT -> 3.2.0.67-apple-SNAPSHOT
# https://stackoverflow.com/a/21493080
# TODO: make sure perl is available if running this script in rio
NEW_VERSION=$(echo $OLD_VERSION | perl -pe 's/^((\d+\.)*)(\d+)(.*)$/$1.($3+1).$4/e')
NEW_RELEASE_VERSION=${NEW_VERSION%-SNAPSHOT}

$MVN versions:set -DnewVersion="$NEW_VERSION" -DgenerateBackupPoms=false | grep -v "no value"

sed -i '' 's/'"$OLD_RELEASE_VERSION"'/'"$NEW_RELEASE_VERSION"'/' rio.y*ml
sed -i '' 's/'"${OLD_RELEASE_VERSION%-apple}"'/'"${NEW_RELEASE_VERSION%-apple}"'/' python/pyspark/version.py

find . -name pom.xml -type f -print0 | xargs -0 grep -Hn "$OLD_VERSION" && { popd; set +x; echo ""; echo "FAILED! mvn missed bumping versions in the above files. It is likely a mvn bug."; echo "Please correct the above files manually. Once ready, you can make a commit with"; echo "git commit -a -m \"Bump to $NEW_VERSION after $OLD_RELEASE_VERSION release\""; echo ""; exit 1; } || echo "No old version left behind, making a local commit."

git commit -a -m "Bump to $NEW_VERSION after $OLD_RELEASE_VERSION release"

popd

set +x
echo ""
echo "SUCCESS! Please double check whether the generated commit has correct versions before pushing to the remote."
