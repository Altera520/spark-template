#!/usr/bin/env bash

SPARK_HOME="$BDP_HOME/spark"
CONF="$HOME/spark/conf"
LIB="$HOME/spark/lib"
GC_OPT="-XX:+UseG1GC -verbose:gc -XX:+PrintGCTimeStamps -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:+UseLargePagesInMetaspace"
REFERENCE_CONF="-DDB_URL=jdbc:mysql://bdp-nn1:3306/test -DDB_DRIVER_CLASSNAME=com.mysql.cj.jdbc.Driver -DDB_USERNAME=test -DDB_PASSWORD=test"

function join_by {
  local d=${1-} f=${2-}
  if shift 2; then
    printf %s "$f" "${@/#/$d}"
  fi
}

function config_opt {
  DEPLOY_MODE=$1
  CONFIG_FILE=$2
  if [ $DEPLOY_MODE = client ]; then
    echo "-Dconfig.file=$CONF/$CONFIG_FILE"
  else
    echo ""
  fi
}