#!/usr/bin/env bash

# Neo4j installation: -- script written at version 4.1.3
# https://neo4j.com/docs/operations-manual/current/installation/


PROCESSED_FILE_DIRECTORY=$1
if [ -z ${PROCESSED_FILE_DIRECTORY} ]
then
  echo "Please provide path to processed neo4j import files"
  exit 1;
fi

if [ -z ${NEO4J_FOLDER} ]
then
  echo "Please set environment variable NEO4J_FOLDER (the path containing bin, data, import, etc"
  exit 1;
fi

# note: DB must be called "neo4j" in community edition, because managing multiple named databases requires Enterprise
${NEO4J_FOLDER}/bin/neo4j-admin import \
    --verbose \
    --database neo4j \
    --high-io true \
    --skip-bad-relationships true \
    --ignore-empty-strings true \
    --multiline-fields true \
    --id-type STRING \
    --max-memory 6G \
    --nodes "${PROCESSED_FILE_DIRECTORY}/nodes.csv,${PROCESSED_FILE_DIRECTORY}/football_event_graph_nodes.csv.gz" \
    --relationships "${PROCESSED_FILE_DIRECTORY}/relations.csv,${PROCESSED_FILE_DIRECTORY}/football_event_graph_relations.csv.gz"
