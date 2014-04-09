#!/bin/sh

set -e

die() {
	echo "$@" 1>&2
	exit 1
}

# make sure we are called from the project's root dir
if [ ! -e build.sbt ]; then
	die "Please run the script from the project's root directory."
fi

sbt doc || die "Documents could not be built."

COMMIT_HASH=$(git log -n1 --format="format:%H" HEAD)
OLD_BRANCH=$(git branch | grep '^*' | sed -e 's/^* //')

git checkout gh-pages || die "could not switch to gh-pages"
git rm -rf api/
cp -R target/scala-2.10/api ./api
git add api/
git commit -m "publish_api.sh generated docs for commit ${COMMIT_HASH}"

git checkout "$OLD_BRANCH" || die "Cannot switch back to ${OLD_BRANCH}."
git push -u origin gh-pages

