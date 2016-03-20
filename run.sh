#!/usr/bin/env bash
echo "************************* SETTING ENV VARS ************************* "
export PRINCIPAL="mesos_master"
export FRAMEWORK_USER="vagrant"
export MASTER_ADDRESS="10.144.144.10:5050"
# http://askubuntu.com/a/560466/165699
export LIBPROCESS_IP="$(ip addr show eth1 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)"
export LIBPROCESS_PORT="5055"
export ZK_IP="127.0.0.1"
export ZK_PORT="2181"
export ZKNODE_PATH="/faleiro"
export ATOMIX_LOGS_DIR="logs"
echo "************************* BUILDING WITH MAVEN ************************* "
mvn package $@
echo "************************* RUNNING FALEIRO ************************* "
java -cp target/faleiro-1.0-SNAPSHOT.jar org.magellan.faleiro.Web
