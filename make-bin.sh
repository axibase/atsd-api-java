#!/bin/bash

. deploy.config

mvn -DargLine="-Daxibase.tsd.api.server.name=$ATSD_SERVER \
-Daxibase.tsd.api.server.port=$ATSD_PORT \
-Daxibase.tsd.api.server.tcp.port=$ATSD_TCP_PORT \
-Daxibase.tsd.api.username=$ATSD_USERNAME \
-Daxibase.tsd.api.password=$ATSD_PASSWORD" clean assembly:assembly
