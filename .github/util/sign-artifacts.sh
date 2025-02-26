#!/usr/bin/env bash

###################################################################
## This script signs the release artifacts
###################################################################

set -e
set -x

if [ -z ${1+x} ]; then
  echo "This script requires the directory to be passed to it. Example: sign-artifacts.sh artifacts";
  exit 1;
fi

archiveDir=$1

if [ -z ${GPG_PASSWORD+x} ]; then
  echo "GPG_PASSWORD must be set";
  exit 1
fi

declare -a file_patterns=("*.jar" "*-installer-*" "*.zip" "*.tar.gz")

for file_pattern in "${file_patterns[@]}"
do
  echo "Searching for $file_pattern files..."
  for i in `find $archiveDir -name $file_pattern -maxdepth 1`; do
    echo "Signing $i"
    rm -f $i.asc
    rm -f $i.md5
    rm -f $i.sha1

    gpg --batch --pinentry-mode=loopback --passphrase "$GPG_PASSWORD" -ab $i
    md5sum < $i > $i.md5
    sha1sum < $i > $i.sha1
  done
done

##Fix files
sed -i 's/ -//' $archiveDir/*.md5
sed -i 's/ -//' $archiveDir/*.sha1

