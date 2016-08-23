#!/usr/bin/env bash

export CURR_HOME=$(cd "$(dirname "$0")"; pwd)

cd "$CURR_HOME"

export JAVA_HOME=/home/hadoop/jdk1.8.0_77

export PATH=$JAVA_HOME/bin:$PATH

JVM_GC="-XX:+UseParallelGC -XX:+UseParallelOldGC  -Xloggc:logs/gc.log -verbose:gc -XX:+PrintGCDetails  -XX:+PrintGCDateStamps"


JAVA_OPTS=" -Xms1g -Xmx1g  $JVM_GC"

if [ ! -x "logs" ];then
    mkdir logs
fi

nohup java $JAVA_OPTS  -cp conf:$(echo $(ls lib/*) | sed 's/ /:/g')   com.ylq.framework.Bootstrap > logs/framework.log 2>&1 <&- &