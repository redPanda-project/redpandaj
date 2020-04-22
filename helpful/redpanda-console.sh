#!/bin/bash

# redpanda start script with auto restart and update installing...

FILE=update
errorReportFileAllowed=errorReports.allowed
errorReportFileDisallowed=errorReports.disallowed
SETTINGS_FILE=bin/settings

#touch $FILE

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT
trapCount=0

function ctrl_c() {
  echo "** Trapped CTRL-C"
  if [ $trapCount -gt 1 ]; then
    exit
  fi
  #rm -f $FILE
  trapCount+=1
}

if [ -f $SETTINGS_FILE ]; then
  while read line; do
    JAVA_HOME=$line
  done <$SETTINGS_FILE
else
  JAVA_HOME=""
fi

cmd=$JAVA_HOME
cmd+="java -Xmx1024m"

while [ true ]; do

  #move update if available
  if [ -f $FILE ]; then
    echo "installing update..."
    rm redpanda.jar.bak
    mv redpanda.jar redpanda.jar.bak
    mv update redpanda.jar
  fi

  extras="sentry="

  if [ -f $errorReportFileDisallowed ]; then
    extras+="no"
  elif [ -f $errorReportFileAllowed ]; then
    extras+="yes"
  else
    extras+="unknown"
    echo " "
    while true; do
      read -p "The server is able to send error reports to a system called Sentry, do you want to activate error reporting and accept that information of this instance are send to the Sentry servers? [y/n]" yn
      case $yn in
      [Yy]*)
        touch $errorReportFileAllowed
        break
        ;;
      [Nn]*)
        touch $errorReportFileDisallowed
        break
        ;;
      *) echo "Please answer yes or no." ;;
      esac
      continue
    done
  fi

  $cmd -jar redpanda.jar $extras

  if [ -f $FILE ]; then
    echo "restart..."
  else
    echo "quit..."
    break
  fi

done
