#!/usr/bin/env bash

set -x

if [[ `ip addr show eth1` ]]; then
    export LIBPROCESS_IP="$(ip addr show eth1 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)"
    export MASTER_ADDRESS="10.144.144.10:5050"
    supervisord -c supervisord.conf
    supervisorctl -c supervisord.conf reload
elif [[ `ip addr show tun0` ]]; then
    export LIBPROCESS_IP="$(ip addr show tun0 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)"
    export MASTER_ADDRESS="10.8.0.210:5050"
    supervisord -c supervisord_linode.conf
    supervisorctl -c supervisord_linode.conf reload
else
    echo "Neither ETH1_IP nor TUN0_IP was set!" 1>&2
    exit 1
fi
