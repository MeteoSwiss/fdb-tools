import argparse
import eccodes as ec
from typing import Any
import hashlib
import json
import os
import gribapi
import sys
from pathlib import Path


def dict_hash(dictionary) -> str:
    """MD5 hash of a dictionary."""
    dhash = hashlib.md5()
    # We need to sort arguments so {'a': 1, 'b': 2} is
    # the same as {'b': 2, 'a': 1}
    encoded = json.dumps(dictionary, sort_keys=True).encode()
    dhash.update(encoded)
    return dhash.hexdigest()


def main():
    parser = argparse.ArgumentParser(
        prog="check_mars_model",
        description="script to check that mars data is defined and "
        "unique for all records within a file",
    )
    argin = parser.add_mutually_exclusive_group(required=True)
    argin.add_argument("-f", "--filename", help="grib filename to process")
    argin.add_argument(
        "-d",
        "--dir",
        help="directory from which all grib files are" "recursively processed",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="verbose information for debugging"
    )
    parser.add_argument(
        "--dump", action="store_true", help="grib dump first instance of duplicate records to file for debugging"
    )

    parser.add_argument(
        "-e",
        "--exception",
        help="list of (comma separated) paramId that should be excluded from checking."
        "This is used in case there are duplicates in the files that should be excluded from checking for clashes",
    )
    args = parser.parse_args()

    files = []
    if args.filename:
        files = [args.filename]
    else:
        for root, _, ff in os.walk(args.dir):
            for f in ff:
                files.append(os.path.join(root, f))

    # the syntax of the schema keys follows FDB:
    #    ? at the end marks an optional key
    schema_keys = (
        "mars.class",
        "mars.stream",
        "mars.expver",
        "mars.model",
        "mars.type",
        "mars.levtype",
        "mars.param",
        "mars.levelist?",
        "mars.step",
        "mars.date",
        "mars.time",
        "mars.number?",
        # This is needed to differentiate between 1h and 10min data for step 0,
        # however is commented out since it is not a mars key
        # "indicatorOfUnitOfTimeRange",
    )
    hash_keys = {}
    param_exception = (
        [int(x) for x in args.exception.split(",")] if args.exception else None
    )
    index = {}

    for file in files:
        print("Processing file", file)
        cnt = 0
        with open(file, "rb") as f:
            while 1:
                gid = ec.codes_grib_new_from_file(f)
                if gid is None:
                    break

                cnt += 1
                try:
                    ec.codes_get(gid, "edition")
                except gribapi.error.UnsupportedEditionError:
                    raise RuntimeError(file + " is not a grib file ")

                vals = {}
                for key in schema_keys:
                    if args.verbose:
                        print("[", key, "]")

                    def _get_codes_key(gid, key):
                        if key == "level":
                            val = ec.codes_get_double(gid, key)
                        else:
                            val = ec.codes_get(gid, key)
                        if val == "unknown":
                            raise RuntimeError("unknown key:" + key)

                        return val

                    if key[-1] == "?":
                        key = key[:-1]
                        try:
                            val = _get_codes_key(gid, key)
                        except gribapi.errors.KeyValueNotFoundError:
                            val = None
                    else:
                        val = _get_codes_key(gid, key)

                    if val is not None:
                        vals[key] = val

                hash = dict_hash(vals)
                if hash in hash_keys.keys():
                    if param_exception and vals["param"] in param_exception:
                        continue
                    if args.dump:
                        with open(Path.cwd() / 'duplicate_1.dump', "w") as fout:
                            ec.codes_dump(gid, fout, mode='debug')
                        with open(Path.cwd() / 'duplicate_2.dump', "w") as fout:
                            ec.codes_dump(index[hash][2], fout, mode='debug')
                    raise RuntimeError(
                        "Hash already found,",
                        vals,
                        " for record #:",
                        cnt,
                        ",in file :",
                        file,
                        ". It was already inserted with index: ",
                        index[hash],
                    )
                index[hash] = (cnt, file, gid)
                hash_keys[dict_hash(vals)] = vals


if __name__ == "__main__":
    sys.exit(main())
