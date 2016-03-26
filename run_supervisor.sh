#!/usr/bin/env bash

export LIBPROCESS_IP="$ETH1_IP"
supervisord
supervisorctl reload
