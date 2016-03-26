# faleiro
Scheduler

[![Build Status](https://travis-ci.org/mesos-magellan/faleiro.svg?branch=master)](https://travis-ci.org/mesos-magellan/faleiro)
[![Coverage Status](https://coveralls.io/repos/github/mesos-magellan/faleiro/badge.svg?branch=master)](https://coveralls.io/github/mesos-magellan/faleiro?branch=master)

## Setup

```bash
./install.sh  # Create directories for logging, config and data
```

## Running with `supervisord`

### Running and Managing the Service

```bash
# If supervisord isn't already going, start it
supervisord -c supervisord.conf
# We can use the following commands to get status, start, stop, restart
supervisorctl status
supervisorctl start all
supervisorctl restart all
supervisorctl stop all
# These commands must all be run from the root directory
```

You can also use the `supervisorctl` shell.
```
$ supervisorctl
> help
```

### Viewing Logs

* When running with supervisor, output from the server is redirected to logs in `/var/log/faleiro/`.
* `cd /var/log/faleiro/; grc tail -f stdout.log` to follow

### Managing Instances with Supervisor Web UI

* Visit http://10.144.144.21:9001/ (or whichever scheduler you'd like to check out)
    * Log in with faleiro:faleiro for username:password
