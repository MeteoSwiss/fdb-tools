#!/bin/bash

# This script is to be run as a cronjob. It checks that Store and Catalogue FDB-servers are running, and restarts the servers if not.
# This is a short term solution until we can create a long running service with a dedicated service account.

# Example cronjob:
# 0 */3 * * * /scratch/mch/vcherkas/fdb_remote/cronjob.sh >> /scratch/mch/vcherkas/fdb_remote/cronjob.log  2>&1

# Use the --restart flag to restart the catalogue and server.

export PATH=/scratch/mch/vcherkas/spack-view/bin:$PATH
export GRIB_DEFINITION_PATH=/scratch/mch/vcherkas/eccodes-cosmo-resources/definitions:/scratch/mch/vcherkas/eccodes/definitions
export FDB_DEBUG=1
export SCRATCH=/scratch/mch/vcherkas

start_service() {

    local service=$1
    local service_l=$(echo "$service" | tr '[:upper:]' '[:lower:]')
    local service_dir="$SCRATCH/fdb_remote/$service_l"

    cd $SCRATCH/fdb_remote/$service_l
    export FDB_HOME=.
    mv log.out log_$(date '+%Y%m%d%H%M').out
    nohup fdb-server $service_l > log.out 2> log.err < /dev/null &

    sleep 2

    local pid=$(grep -oP 'pid is \K\d+' "$SCRATCH/fdb_remote/$service_l/log.out")

    if [ -n "$pid" ]; then
        echo $(date '+%Y-%m-%d %H:%M') "New $service PID running: $pid"
    else
        echo $(date '+%Y-%m-%d %H:%M') "Could not restart $service."
    fi
}


for service in Store_1 Store_2 Store_3 Catalogue; do

    service_l=$(echo "$service" | tr '[:upper:]' '[:lower:]')
    cd $SCRATCH/fdb_remote/$service_l
    logs=$SCRATCH/fdb_remote/$service_l/log.out
    pid=$(grep -oP 'pid is \K\d+' "$logs")

    # Check if a pid was 
    
    if [ -n "$pid" ]; then
    
        # Use kill command with signal 0 to check if the process is running
        if kill -0 "$pid" 2>/dev/null; then

            echo $(date '+%Y-%m-%d %H:%M') "$service with PID $pid is still running."

            if [ "$1" == "--restart" ]; then
                echo $(date '+%Y-%m-%d %H:%M') "Restarting $service. Killing process $pid."
                kill "$pid"
                start_service "$service"
            fi
        else
            echo $(date '+%Y-%m-%d %H:%M') "$service with PID $pid is not running! Restarting $service."
            start_service "$service"
        fi
    else
        echo $(date '+%Y-%m-%d %H:%M') "No $service pid found. Starting $service."
        start_service "$service"
    fi
done
