#!/usr/bin/env bash

DIR="$(cd "`dirname "$0"`"; pwd)"
cd $DIR
echo "$@"

# local | dev | prd
APP_MODE=${MODE:-prd}
APP_NAME=""
MAIN_JAR=""
MAIN_CLASS=""

JARS="
"

FILES="
"

# yarn resources
QUEUE=${QUEUE:-default}
EXECUTOR_NUM=${EXECUTOR_NUM:-1}
EXECUTOR_CORE=${EXECUTOR_CORE:-3}
EXECUTOR_MEM=${EXECUTOR_MEM:-6g}
DRIVER_MEM=${DRIVER_MEM:-6g}

LIB=""
CONF=""

GC_OPT="-XX:+UseG1GC -verbose:gc -XX:+PrintGCTimeStamps -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:+UseLargePagesInMetaspace"

spark-submit \
  --class ${MAIN_CLASS} \
  --name ${APP_NAME} \
  --master yarn \
  --deploy-mode cluster \
  --queue $QUEUE \
  --num-executors $EXECUTOR_NUM \
  --executor-memory $EXECUTOR_MEM \
  --executor-cores $EXECUTOR_CORE \
  --driver-memory $DRIVER_MEM \
  --conf "spark.yarn.submit.waitAppCompletion=false" \
  --conf "spark.ui.enabled=true" \
  --conf "spark.eventLog.enabled=false" \
  --conf "spark.driver.extraJavaOptions=-DAPP_MODE=${APP_MODE} ${GC_OPT}" \
  --conf "spark.executor.extraJavaOptions=-DAPP_MODE=${APP_MODE} ${GC_OPT}" \
  --files ${FILES} \
  --jars ${JARS} \
  $LIB/$MAIN_JAR
