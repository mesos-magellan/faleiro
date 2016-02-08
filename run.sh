#!/usr/bin/env bash
echo "************************* SETTING ENV VARS ************************* "
export PRINCIPAL="mesos_master"
export FRAMEWORK_USER="magellan"
echo "************************* BUILDING WITH MAVEN ************************* "
mvn package $@
echo "************************* RUNNING FALEIRO ************************* "
java -cp target/faleiro-1.0-SNAPSHOT.jar org.magellan.faleiro.Web
