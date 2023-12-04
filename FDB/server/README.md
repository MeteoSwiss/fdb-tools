This folder contains a script to check that the FDB-servers are still running. 

This can be setup as a cronjob eg:
```
crontab -l
   * * * * * cronjob.sh >> cronjob.log  2>&1
```

See information for the currently running cronjob on Balfrin at https://meteoswiss.atlassian.net/l/cp/vniFqtv1
