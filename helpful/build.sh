#!/bin/bash

# script to automatically build the latest version from github
# creating sources dir with git clone, git pull, maven package, cp of jar file

DIRECTORY=sources


if [ ! -d "$DIRECTORY" ]; then
  echo "$DIRECTORY does not exists cloning into $DIRECTORY..."
  git clone git@github.com:redPanda-project/redpandaj.git $DIRECTORY
fi


cd $DIRECTORY


echo "getting lates changes..."
git pull


echo "creating jar file"
mvn package

echo "cp jar file into working directory..."
cp ./target/redpanda.jar ../


cd ..
