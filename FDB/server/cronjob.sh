#!/bin/bash

# This script is to be run as a cronjob. It checks that Store and Catalogue FDB-servers are running, and restarts the servers if not.
# This is a short term solution until we can create a long running service with a dedicated service account.

services="Store Catalogue"

for service in $services; do
    service_l=$(echo "$service" | tr '[:upper:]' '[:lower:]')
    logs=/scratch/mch/vcherkas/fdb_remote/$service_l/log.out
    pid=$(grep -oP 'pid is \K\d+' "$logs")
    # Check if a pid was found
    if [ -n "$pid" ]; then
        # Use kill command with signal 0 to check if the process is running
        if kill -0 "$pid" 2>/dev/null; then
            echo $(date '+%Y-%m-%d %H:%M') "$service with PID $pid is still running."
        else
            echo $(date '+%Y-%m-%d %H:%M') "$service with PID $pid is not running! Restarting $service."
            cd /scratch/mch/vcherkas/fdb_remote/$service_l
            export PATH=/scratch/mch/vcherkas/vcherkas/spack-root/linux-sles15-zen3/gcc-11.3.0/fdb-remote-5xqbous4s5ylzecygflbnkcb27cp7rqm/bin:$PATH
            export GRIB_DEFINITION_PATH=/scratch/mch/vcherkas/eccodes-cosmo-resources/definitions:/scratch/mch/vcherkas/eccodes/definitions
            export FDB_HOME=.
            nohup fdb-server > log.out 2> log.err < /dev/null &
            sleep 2
            pid=$(grep -oP 'pid is \K\d+' "$logs")
            if [ -n "$pid" ]; then
                echo $(date '+%Y-%m-%d %H:%M') "New $service PID running: $pid"
            else
                echo $(date '+%Y-%m-%d %H:%M') "Could not restart $service."
            fi
        fi
    else
        echo $(date '+%Y-%m-%d %H:%M') "No $service pid found."
    fi
done
