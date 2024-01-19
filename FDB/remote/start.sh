#!/bin/bash

# This script is to be run as a cronjob. It checks that Store and Catalogue FDB-servers are running, and restarts the servers if not.
# This is a short term solution until we can create a long running service with a dedicated service account.

# Use the --restart flag to restart the catalogue and server.

export FDB_REMOTE=$(dirname "$(pwd)")

export PATH=/scratch/mch/vcherkas/spack-fdb-view/bin:$PATH
export GRIB_DEFINITION_PATH=/store_new/mch/msopr/icon_workflow_2/eccodes-cosmo-resources/definitions:/store_new/mch/msopr/icon_workflow_2/eccodes_2.25.1/definitions
export FDB_DEBUG=1

start_service() {

    local service=$1
    local service_l=$(echo "$service" | tr '[:upper:]' '[:lower:]')
    local service_dir="$FDB_REMOTE/$service_l"

    cd $FDB_REMOTE/$service_l
    export FDB_HOME=.
    
    nohup fdb-server > log.out 2> log.err < /dev/null &

    sleep 2

    local pid=$(grep -oP 'pid is \K\d+' "$FDB_REMOTE/$service_l/log.out")

    if [ -n "$pid" ]; then
        echo $(date '+%Y-%m-%d %H:%M') "New $service PID running: $pid"
    else
        echo $(date '+%Y-%m-%d %H:%M') "Could not restart $service."
    fi
}


for service in Store Catalogue; do

    service_l=$(echo "$service" | tr '[:upper:]' '[:lower:]')
    cd $FDB_REMOTE/$service_l
    logs=$FDB_REMOTE/$service_l/log.out
    pid=$(grep -oP 'pid is \K\d+' "$logs")

    # Check if a pid was 
    
    if [ -n "$pid" ]; then
    
        # Use kill command with signal 0 to check if the process is running
        if kill -0 "$pid" 2>/dev/null; then

            echo $(date '+%Y-%m-%d %H:%M') "$service with PID $pid is still running."

            if [ "$1" == "--restart" ]; then
                echo $(date '+%Y-%m-%d %H:%M') "Restarting $service."
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
