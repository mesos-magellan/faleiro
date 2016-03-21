#!/usr/bin/env bash

LOGDIR="/var/log/faleiro"
CONFDIR="/etc/faleiro"
DATADIR="/var/lib/faleiro"
ORIGUSER="$USER"

# Create log directory and give $USER access to the folder
sudo mkdir -p $LOGDIR
sudo chown -hR $ORIGUSER $LOGDIR
# Create config directory and give $USER access to the folder
sudo mkdir -p $CONFDIR
sudo chown -hR $ORIGUSER $CONFDIR
# Create data directory and give $USER access to the folder
sudo mkdir -p $DATADIR
sudo chown -hR $ORIGUSER $DATADIR
