#!/usr/bin/env bash

set -x

if [[ `ip addr show eth1` ]]; then
  export LIBPROCESS_IP="$(ip addr show eth1 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)"
elif [[ `ip addr show tun0` ]]; then
  export LIBPROCESS_IP="$(ip addr show tun0 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)"
else
  echo "Neither ETH1_IP nor TUN0_IP was set!" 1>&2
  exit 1
fi
supervisord
supervisorctl reload
