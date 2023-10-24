import sys
import os
import logging
from collections import Counter
import argparse

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

parser = argparse.ArgumentParser(description='Archive some files to FDB.')
parser.add_argument('files', metavar='N', type=str, nargs='+',
                    help='Paths to files to archive')
args = parser.parse_args()

logfile = os.path.join(logPath, 'fdb-write-'+os.getenv("SLURM_JOB_ID"))

rootLogger.debug(f'GRIB_DEFINITION_PATH={os.environ["GRIB_DEFINITION_PATH"]}')

if os.getenv('CODING') == 'grib':

    grib_keys=['generatingProcessIdentifier',
               'productionStatusOfProcessedData',
               'paramId',
               'dataDate',
               'dataTime',
               'endStep',
               'productDefinitionTemplateNumber',
               'typeOfLevel',
               'level',
               'scaleFactorOfFirstFixedSurface',
               'scaledValueOfFirstFixedSurface',
               'scaleFactorOfSecondFixedSurface']
    grib_keys=(',').join(grib_keys)
    command = f"fdbf-write --verbose --keys={grib_keys} {' '.join(args.files)} > {logfile}"

else:
    command = f"fdb-write --verbose {' '.join(args.files)} > {logfile}"

rootLogger.debug(f'{command}')
os.system(command)

with open(logfile,'r') as source: 
    keys_archived=[line for line in source if line.startswith('Archiving {')]

    rootLogger.debug(f'GRIB records: {len(keys_archived)}')
    counter = Counter(keys_archived)
    dup_count = sum(1 for cnt in counter.values() if cnt > 1)
    duplicates = [key for key, cnt in counter.items() if cnt > 1]
    total_dup = sum(cnt for cnt in counter.values() if cnt > 1)

    rootLogger.debug(f'Duplicate records: {total_dup} Unique duplicates: {dup_count}')
    for duplicate in duplicates:
        rootLogger.debug(f'Duplicate record: {duplicate}')
