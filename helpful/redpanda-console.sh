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
cmd+="java -Xmx256m"




while [ true ]
do


#move update if available
if [ -f $FILE ];
then
   echo "installing update..."
   rm redPandaj.jar.bak
   mv redPandaj.jar redPandaj.jar.bak
   mv update redPandaj.jar
fi





#rm redPandaj.jar.bak
#mv redPandaj.jar redPandaj.jar.bak
#mv update redPandaj.jar

#rm -f $FILE

#$cmd -jar loadupdate.jar

$cmd -jar redPandaj.jar


if [ -f $FILE ];
then
   echo "restart..."
else
   echo "quit..."
   break
fi

done

