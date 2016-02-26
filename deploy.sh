#!/bin/bash

. deploy.config

export JAVA_HOME=$JAVA7_HOME

mvn -s settings.xml -DargLine="-Daxibase.tsd.api.server.name=$ATSD_SERVER \
-Daxibase.tsd.api.server.port=$ATSD_PORT -Daxibase.tsd.api.username=$ATSD_USERNAME \
-Daxibase.tsd.api.password=$ATSD_PASSWORD" clean javadoc:jar \
source:jar -Dgpg.keyname=$GPG_KEYNAME -Dgpg.passphrase=$GPG_PASSPHRASE deploy
