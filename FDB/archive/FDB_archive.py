import sys
import os
import logging
from collections import Counter

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

with open(gribfilelog,'r') as source: 
    keys_archived=[line for line in source if line.startswith('Archiving {')]

    rootLogger.debug(f'[File {gribfile}] GRIB records: {len(keys_archived)}')
    counter = Counter(keys_archived)
    dup_count = sum(1 for cnt in counter.values() if cnt > 1)
    duplicates = [key for key, cnt in counter.items() if cnt > 1]
    total_dup = sum(cnt for cnt in counter.values() if cnt > 1)

    rootLogger.debug(f'[File {gribfile}] Duplicate records: {total_dup} Unique duplicates: {dup_count}')
    for duplicate in duplicates:
        rootLogger.debug(f'Duplicate record: {duplicate}')
    