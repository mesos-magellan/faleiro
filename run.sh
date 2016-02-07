#!/usr/bin/env bash
echo "************************* SETTING ENV VARS ************************* "
export LIBPROCESS_IP="10.144.144.1"
export PRINCIPAL="mesos_master"
export FRAMEWORK_USER="magellan"
echo "************************* BUILDING WITH MAVEN ************************* "
mvn package "$@"
echo "************************* RUNNING FALEIRO ************************* "
java -cp target/faleiro-1.0-SNAPSHOT.jar org.magellan.faleiro.Web
