# FDB Remote

This folder contains:

* Configuration files
  * FDB catalogue server
  * FDB store server
  * A client to test the remote FDB
* A script `start.sh` to check that the FDB-servers are running and restart them if not. This can be run as a cronjob, eg.

```
crontab -l
   * * * * * start.sh >> start.log  2>&1
```

If you want to run your remote FDB, change the ports in the configurations and then run `start.sh`.

If you want to test the remote FDB via the client, change directory to the client folder, `source setup.sh` and then issue FDB CLI commands as usual. eg.

```
fdb-read request.mars output.grib
```
