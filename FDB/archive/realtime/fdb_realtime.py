"""
This python service polls the filesystem for new ICON model data (which has been regridded to rotated lat lon).
It delays the archival of files which are potentially still being modified.
Modified files are not archived, only newly created files.
"""

import os
import re
import logging
import shutil
import datetime as dt
import time
from pathlib import Path
import eccodes
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger('realtimefdb')


MAX_FDB_ROOT_SIZE = "1TB"
MAX_FDB_NUM_FORECASTS = 2
FDB_PATTERN_REGEX = re.compile(r'\d{8}:\d{4}:+')

SNS_ARN = 'arn:aws:sns:eu-central-2:493666016161:model-events'
session = boto3.Session(profile_name='model-events')
client = session.client('sns', config=Config(region_name = 'eu-central-2'))

class FSPoller():
    def __init__(self, path, fdb_root):
        self.regex = re.compile(r"^" + re.escape(path) + r"\d{8}_633/resource/_FXINP_lfff\d{8}_\d{3}([zp]|)$")
        self.items = set()
        self.path = Path(path)
        self.glob = './*_633/resource/_FXINP_lfff*'
        self.previous_state = set(self.path.rglob(self.glob))
        self.root = fdb_root

        import pyfdb
        self.fdb = pyfdb.FDB()

    def watch(self):
        while True:
            current_state = set(self.path.rglob(self.glob))
            if added := current_state - self.previous_state:
                for path in added:
                    logger.info(f"Path created: {path}")
                    if path.is_file() and self.regex.match(str(path)):
                        self.items.add(path)

                self.previous_state = current_state

            if is_directory_larger_than(self.root, MAX_FDB_ROOT_SIZE):
                msg = f"FDB is > {MAX_FDB_ROOT_SIZE}, stopping listener."
                logger.exception(msg)
                raise Exception(msg)
            

            archived = archive_files(list(self.items), self.fdb)
            self.items = self.items - set(archived)

            if archived:
                response = send_notifications(archived)

                logger.info(f"Flushing FDB")
                self.fdb.flush()

            while len(stored_forecasts := get_archived_forecasts(self.root)) > MAX_FDB_NUM_FORECASTS:
                fdb_wipe_oldest_forecast(self.root, stored_forecasts)

            time.sleep(15)



def archive_files(paths: list[Path], fdb) -> list[Path]:
    """Archive files to FDB."""

    archived = []

    if not paths:
        logger.info(f"Created files: none")
    else:
        for file in list(paths):
            last_modified_datetime = dt.datetime.fromtimestamp(os.path.getmtime(file))
            if dt.datetime.now() - last_modified_datetime > datetime.timedelta(seconds=2):
                logger.info(f"Archiving to FDB: {file}")
                fdb.archive(open(file, "rb").read())
                archived.append(file)
            else:
                logger.info(f"To archive in next iteration: {file}")
    return archived



def get_archived_forecasts(root: str) -> list[dt.datetime]:
    """Check the forecast date and times which are currently archived in FDB."""

    subdirectories = [d.stem for d in Path(root).iterdir() if d.is_dir()]

    forecasts = set()
    # Iterate over the subdirectories and delete those that match the date:time pattern
    for subdirectory in subdirectories:
        if match := FDB_PATTERN_REGEX.match(subdirectory):
            forecast = dt.datetime.strptime(match.group(0), "%Y%m%d:%H%M:")
            forecasts.add(forecast)
    return list(forecasts)



def fdb_wipe_oldest_forecast(root: str, forecasts: list[dt.datetime]):
    """Delete oldest forecast stored in FDB."""

    to_delete = min(forecasts).strftime("%Y%m%d:%H%M:")
    subdirectories = [d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
    for subdirectory in subdirectories:
        if to_delete in subdirectory:
            logger.warning(f"Deleting directory: {os.path.join(root, subdirectory)}")
            shutil.rmtree(os.path.join(root, subdirectory))



def is_directory_larger_than(directory, size_limit_human_readable):
    # Convert human-readable size to bytes
    size_limit_bytes = {
        'KB': 1024,
        'MB': 1024 ** 2,
        'GB': 1024 ** 3,
        'TB': 1024 ** 4
    }[size_limit_human_readable[-2:]] * float(size_limit_human_readable[:-2])

    # Get the size of the directory
    size_in_bytes = get_directory_size(directory)

    # Check if the directory size is larger than the limit
    return size_in_bytes > size_limit_bytes



def get_directory_size(directory):
    total_size = 0
    with os.scandir(directory) as it:
        for entry in it:
            if entry.is_file():
                total_size += entry.stat().st_size
            elif entry.is_dir():
                total_size += get_directory_size(entry.path)
    return total_size



def chunk_events(event_list, chunk_size):
    """Yield successive n-sized chunks from the list of events."""
    for i in range(0, len(event_list), chunk_size):
        yield event_list[i:i + chunk_size]



def send_notifications(paths: list[str]):
    """Send messages to SNS queue"""

    entries = []
    for idx, path in enumerate(paths):

        with open(path, "rb") as f:
            gid = eccodes.codes_grib_new_from_file(f)
            if gid is None:
                msg = f"Could not read grib file {path}."
                logger.exception(msg)
                raise RuntimeError(msg)
            
            date = eccodes.codes_get_string(gid, 'mars.date')
            time = eccodes.codes_get_string(gid, 'mars.time')
            step = eccodes.codes_get_string(gid, 'mars.step')
            levtype = eccodes.codes_get_string(gid, 'mars.levtype')

            eccodes.codes_release(gid)

        entries.append({
                'Id': str(idx),
                'Message': 'Information about newly available forecast data.',
                'Subject': 'string',
                'MessageStructure': 'string',
                'MessageAttributes': {
                    'date': {
                        'DataType': 'String',
                        'StringValue': f'{date}',
                    },
                    'time': {
                        'DataType': 'String',
                        'StringValue': f'{time}',
                    },
                    'step': {
                        'DataType': 'String',
                        'StringValue': f'{step}',
                    },
                    'levtype': {
                        'DataType': 'String',
                        'StringValue': f'{levtype}',
                    }
                },
            }
        )

    entries_chunked = list(chunk_events(entries, 10))

    responses = []

    for chunk in entries_chunked:
        try:
            response = client.publish_batch(
                TopicArn=SNS_ARN,
                PublishBatchRequestEntries=chunk
                )
            if "Successful" in response:
                for msg_meta in response["Successful"]:
                    logger.info(
                        "Message sent: %s: %s",
                        msg_meta["MessageId"],
                        paths[int(msg_meta["Id"])],
                    )
            if "Failed" in response:
                for msg_meta in response["Failed"]:
                    logger.warning(
                        "Failed to send: %s: %s",
                        msg_meta["MessageId"],
                        paths[int(msg_meta["Id"])],
                    )
        except ClientError as error:
            logger.exception("Send messages failed to queue: %s", SNS_ARN)
            raise error
        else:
            responses.append(response)
    return responses



if __name__ == "__main__":

    osm_path = "/opr/osm/aare/wd/"

    mod = Path(__file__)
    root = mod.parent / mod.stem / "fdb-root-realtime"

    if not root.exists():
        root.mkdir(parents=True)

    os.environ['FDB_HOME'] = '/scratch/mch/vcherkas/spack-root/linux-sles15-zen3/gcc-11.3.0/fdb-5.11.94-uicdnp73mblndo2fw6x5zmkz5ex5equo'
    os.environ['FDB5_HOME'] = os.environ['FDB_HOME']
    os.environ['FDB5_CONFIG'] = "{'type':'local','engine':'toc','schema':'/store_new/mch/msopr/icon_workflow_2/realtime_fdb/fdb-schema','spaces':[{'handler':'Default','roots':[{'path':'"+root+"'}]}]}"
    os.environ['ECCODES_DEFINITION_PATH'] = '/store_new/mch/msopr/icon_workflow_2/realtime_fdb/definitions/eccodes-cosmo-resources/definitions:/store_new/mch/msopr/icon_workflow_2/realtime_fdb/definitions/eccodes/definitions'

    poller = FSPoller(osm_path, root)

    poller.watch()
