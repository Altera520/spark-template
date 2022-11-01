#!/usr/bin/env bash

DIR="$(cd "`dirname "$0"`"; pwd)"
cd $DIR

source ./spark-env.sh

# app env
# ================================================= #
# local | dev | prd
APP_MODE=${MODE:-prd}
JAR_NAME=service-example-0.0.1-SNAPSHOT-all.jar
MAIN_CLASS=${MAIN_CLASS}
APP_NAME=${APP_NAME:-$MAIN_CLASS}
DEPLOY_MODE=client
CONFIG_FILE="service-example.conf"

# priority: application.conf -> resources/reference.conf
FILES="
$CONF/log4j2.properties
$CONF/$CONFIG_FILE#application.conf
"

JARS="
"

OPTS="
-DAPP_MODE=${APP_MODE}
${GC_OPT}
$(config_opt $DEPLOY_MODE $CONFIG_FILE)
"

# yarn resources
# ================================================= #
QUEUE=${QUEUE:-default}
DRIVER_CORE=${DRIVER_CORE:-1}
DRIVER_MEM=${DRIVER_MEM:-4g}
EXECUTOR_NUM=${EXECUTOR_NUM:-1}
EXECUTOR_CORE=${EXECUTOR_CORE:-1}
EXECUTOR_MEM=${EXECUTOR_MEM:-4g}


# spark job submit
# ================================================= #
${SPARK_HOME}/bin/spark-submit \
 --master yarn \
 --deploy-mode ${DEPLOY_MODE} \
 --driver-cores ${DRIVER_CORE} \
 --driver-memory ${DRIVER_MEM} \
 --executor-cores ${EXECUTOR_CORE} \
 --executor-memory ${EXECUTOR_MEM} \
 --conf "spark.driver.extraJavaOptions=$(join_by ' ' $OPTS)" \
 --conf "spark.executor.extraJavaOptions=$(join_by ' ' $OPTS)" \
 --conf "spark.yarn.submit.waitAppCompletion=false" \
 --files $(join_by ',' . $FILES) \
 --jars $(join_by ',' . $JARS) \
 --class ${MAIN_CLASS} $LIB/$JAR_NAME $@

