#!/bin/bash

. client.config

java -cp "lib/*" -Daxibase.tsd.api.server.name=$ATSD_SERVER  \
 -Daxibase.tsd.api.server.port=$ATSD_PORT -Daxibase.tsd.api.username=$ATSD_USERNAME  \
 -Daxibase.tsd.api.password=$ATSD_PASSWORD com.axibase.tsd.example.AtsdClientWriteExample