#!/bin/bash

set -e
if [[ "$1" == "" ]]
then
  echo "Missing version e.g 5 1 2"
  exit 1
fi

echo "Setting version to $1.$2.$3"


REPLACED_TEXT="__version_info__ = { 'major': $1, 'minor': $2, 'micro': $3, 'releaselevel': 'final'}"
sed -i "s/^__version_info__.*$/${REPLACED_TEXT}/g" btrdb/version.py

git add ../tools/version.go
git commit -m "Release v$1.$2.$3"
git tag v$1.$2.$3
git push origin v$1.$2.$3

sleep 10
git push