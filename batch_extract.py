import argparse
import logging
from pathlib import Path
import firebase_admin
from firebase_admin import credentials
import firebase_download
import firebase_list_devices
# import write_csv


def batch_extract(args):
    # Setup
    logging.basicConfig(level=args.log_level.upper())

    # 1. Download logs
    firebase_download.download(args)
    # 1.1. Zip all logs
    # TODO

    # 2. Download device states
    list_devices = firebase_list_devices.list_devices()
    macs = [entry["mac"] for entry in list_devices]
    logging.debug(macs)
    # 2.1 Write as JSON
    firebase_list_devices.write(
        list_devices,
        firebase_list_devices.parse(
            ["-J", "-o", str(args.outdir.joinpath("device_list.json")),
             "-l", args.log_level]))
    # 2.2 Write as CSV
    firebase_list_devices.write(
        list_devices,
        firebase_list_devices.parse(
            ["-o", str(args.outdir.joinpath("device_list.csv")),
             "-l", args.log_level]))

    # 3. Write CSV and JSON for each mac
    # for mac in macs:
    #     # Write as JSON
    #     write_csv.write("throuhgput", args=mac)
    #     write_csv.write("latency", args=mac)
    #     # Write as CSV
    #     write_csv.write("throuhgput", args=mac)
    #     write_csv.write("latency", args=mac)
    #     # Zip CSVs
    #     # TODO
    #     # Zip raw log
    #     # TODO

    # 4. Zip all CSVs
    # TODO

    print("Done!")


def parse(list_args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("outdir", type=Path,
                        help="Specify output directory.")
    parser.add_argument("-d", "--log-dir", type=Path, default=Path("./logs"),
                        help="Specify local log directory, default='./logs'")
    parser.add_argument("-l", "--log-level", default="warning",
                        help="Provide logging level, default is warning'")
    if (list_args is None):
        return parser.parse_args()
    else:
        return parser.parse_args(args=list_args)


if __name__ == '__main__':
    cred = credentials.Certificate(
        "nd-schmidt-firebase-adminsdk-d1gei-43db929d8a.json")
    firebase_admin.initialize_app(cred, {
        "storageBucket": "nd-schmidt.appspot.com",
        "databaseURL": "https://nd-schmidt-default-rtdb.firebaseio.com"
    })
    batch_extract(parse())
