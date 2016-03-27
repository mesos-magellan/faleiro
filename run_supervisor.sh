#!/usr/bin/env bash

set -x

if [[ $ETH1_IP ]]; then
  export LIBPROCESS_IP="$ETH1_IP"
elif [[ $TUN0_IP ]]; then
  export LIBPROCESS_IP="$TUN0_IP"
else
  echo "Neither ETH1_IP nor TUN0_IP was set!" 1>&2
  exit 1
fi
supervisord
supervisorctl reload
