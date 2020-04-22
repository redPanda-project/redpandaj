#!/bin/bash

# redpanda start script with auto restart and update installing...

FILE=update

#touch $FILE

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
        echo "** Trapped CTRL-C"
	rm -f $FILE
}


while read line; do
    JAVA_HOME=$line
done < bin/settings




cmd=$JAVA_HOME
cmd+="java -Xmx1024m"




while [ true ]
do


#move update if available
if [ -f $FILE ];
then
   echo "installing update..."
   rm redpanda.jar.bak
   mv redpanda.jar redpanda.jar.bak
   mv update redpanda.jar
fi





#rm redpanda.jar.bak
#mv redpanda.jar redpanda.jar.bak
#mv update redpanda.jar

#rm -f $FILE

#$cmd -jar loadupdate.jar

$cmd -jar redpanda.jar


if [ -f $FILE ];
then
   echo "restart..."
else
   echo "quit..."
   break
fi

done

