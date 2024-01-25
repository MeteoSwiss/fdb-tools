import fdb_realtime
import os
import shutil
from pathlib import Path

def test_polling():

    path = os.path.join( Path(__file__).parent, Path(__file__).stem, "events/")
    root = os.path.join( Path(__file__).parent, Path(__file__).stem, "fdb-root-realtime-test/")

    os.environ['FDB_HOME'] = '/scratch/mch/vcherkas/vcherkas/spack-root/linux-sles15-zen3/gcc-11.3.0/fdb-5.11.17-4hcp6n5lien4rzi4tqu2roa4zvsrfeur'
    os.environ['FDB5_HOME'] = os.environ['FDB_HOME']
    os.environ['FDB5_CONFIG'] = "{'type':'local','engine':'toc','schema':'/store_new/mch/msopr/icon_workflow_2/realtime_fdb/fdb-schema','spaces':[{'handler':'Default','roots':[{'path':'"+root+"'}]}]}"
    os.environ['ECCODES_DEFINITION_PATH'] = '/store_new/mch/msopr/icon_workflow_2/realtime_fdb/definitions/eccodes-cosmo-resources/definitions:/store_new/mch/msopr/icon_workflow_2/realtime_fdb/definitions/eccodes/definitions'

    if not os.path.exists(path):
        os.makedirs(path)

    if not os.path.exists(root):
        os.makedirs(root)
    else:
        shutil.rmtree(root, ignore_errors=True)
        os.makedirs(root)

    test_path = path+'/01234567_633/resource'
    if not os.path.exists(test_path):
        os.makedirs(test_path)
    else:
        shutil.rmtree(test_path, ignore_errors=True)
        os.makedirs(test_path)

    print('Once running, run this in another terminal to add a grib file to the watched folder.')
    print('')
    print(f'ls /opr/osm/aare/wd/`ls /opr/osm/aare/wd/ | grep _633 | tail -n 1`/resource/*lfff* | tail -n 1 | xargs  cp -t {test_path} ')
    print('')
    poller = fdb_realtime.FSPoller(path, root)

    poller.watch()


if __name__ == "__main__":

    test_polling()
