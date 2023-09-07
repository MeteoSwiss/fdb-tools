import sys
import json
import os
import time

import logging

logPath=os.getenv('RUN_LOG_FOLDER')
logFileName='TOTAL_FDBFWRITE'

if not os.path.exists(logPath):
    os.makedirs(logPath)

logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger()

fileHandler = logging.FileHandler("{0}/{1}.log".format(logPath, logFileName))
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

rootLogger.setLevel(logging.DEBUG)

file_to_ingest = sys.argv[1]
logging.debug(f"File to Ingest: {file_to_ingest}")

gribfile = file_to_ingest
gribfilelog = os.path.join(logPath, 'fdb-archive-'+os.getenv("SLURM_JOB_ID"))

if os.getenv('CODING') == 'grib':
    command = f"fdbf-write --verbose --keys=generatingProcessIdentifier,productionStatusOfProcessedData,paramId,dataDate,dataTime,endStep,productDefinitionTemplateNumber,typeOfLevel,level,scaleFactorOfFirstFixedSurface,scaledValueOfFirstFixedSurface,scaleFactorOfSecondFixedSurface {gribfile} > {gribfilelog}"
elif os.getenv('CODING') == 'mars':
        command = f"fdb-write --verbose {gribfile} > {gribfilelog}"

os.system(command)


if os.getenv('CODING') == 'grib':
    with open(gribfilelog,'r') as source: 
        keys_archived=[line for line in source if line.startswith('Archiving {')]

        rootLogger.debug('[File %s] GRIB records: %s', gribfile, len(keys_archived))    
        seen = set()
        dup = set()
        dup_count=0
        for line in keys_archived:
            if line in seen:
                dup.add(line)
                dup_count+=1
            else:
                seen.add(line)

        rootLogger.debug('[File %s] Duplicate records: %s', gribfile , dup_count)   