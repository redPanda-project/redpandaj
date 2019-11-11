#!/bin/bash

# script to automatically build the latest version from github
# creating sources dir with git clone, git pull, maven package, cp of jar file

DIRECTORY=sources


if [ ! -d "$DIRECTORY" ]; then
  echo "$DIRECTORY does not exists cloning into $DIRECTORY..."
  git clone git://github.com/redPanda-project/redpandaj.git $DIRECTORY
fi


cd $DIRECTORY


echo "getting lates changes..."
git pull


echo "creating jar file"
mvn package

echo "cp jar file into working directory..."
cp ./target/redpanda.jar ../


cd ..


if [ ! -d "bin" ]; then
  echo "bin directory does not exists lets copy the start script over..."
  mkdir bin
  cp ./$DIRECTORY/helpful/redpanda-console.sh ./bin/redpanda-console.sh
  chmod +x ./bin/redpanda-console.sh
fi

echo "updating build script from gitrepo"
  cp ./$DIRECTORY/helpful/build.sh build.sh

echo "update sucesfully..."
echo "start repanda by typing ./bin/redpanda-console.sh"
