#!/usr/bin/env bash

# Docker guide
# https://neo4j.com/developer/docker-run-neo4j/

if [ -z ${NEO4J_FOLDER} ]
then
  echo "Please set environment variable NEO4J_FOLDER (the path containing bin, data, import, etc"
  exit 1;
fi

NEO4J_CONF="${NEO4J_FOLDER}/conf"
NEO4J_LOGS="${NEO4J_FOLDER}/logs"
NEO4J_DATA="${NEO4J_FOLDER}/data"
NEO4J_IMPORT="${NEO4J_FOLDER}/import"
NEO4J_PLUGINS="${NEO4J_FOLDER}/plugins"

sudo docker run -d \
    --name=football_graph_db \
    --publish=7474:7474 \
    --publish=7687:7687 \
    --volume=${NEO4J_CONF}:/conf \
    --volume ${NEO4J_LOGS}:/logs \
    --volume=${NEO4J_DATA}:/data \
    --volume=${NEO4J_IMPORT}:/import \
    --volume=${NEO4J_PLUGINS}:/plugins \
    --env=NEO4J_dbms_connector_bolt_enabled:true \
    --env=NEO4J_dbms_connector_bolt_tls__level:OPTIONAL \
    --env=NEO4J_dbms_connector_http_enabled:true \
    --env=NEO4J_dbms_default__advertised__address=localhost \
    --env=NEO4J_dbms_default__listen__address=0.0.0.0 \
    neo4j:4.1.3
