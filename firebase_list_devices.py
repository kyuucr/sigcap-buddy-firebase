import argparse
import csv
from datetime import datetime
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import json
import logging
import sys


def get_last_timestamp(heartbeats, mac):
    filtered = list(filter(
        lambda x: x["mac"].replace(":", "-") == mac,
        heartbeats))
    last_val = filtered[len(filtered) - 1]
    logging.debug(last_val)

    return last_val["last_timestamp"]


def write(outarr, args):
    if (args.json):
        args.output_file.write(json.dumps(outarr))
    else:
        csv_writer = csv.DictWriter(
            args.output_file, fieldnames=["mac", "online", "last_timestamp"])
        csv_writer.writeheader()
        csv_writer.writerows(outarr)


def list_devices():
    heartbeats = db.reference("heartbeat").order_by_child(
        "last_timestamp").get().values()

    macs = list(dict.fromkeys(
        [val["mac"].replace(":", "-") for val in heartbeats]))
    logging.debug(macs)

    outarr = list()
    now_timestamp = datetime.timestamp(datetime.now()) * 1000

    for mac in macs:
        last_timestamp = get_last_timestamp(heartbeats, mac)
        outarr.append({
            "mac": mac,
            "online": (now_timestamp - last_timestamp) < 3600000,  # 1 hour
            "last_timestamp": datetime.fromtimestamp(
                last_timestamp / 1000).astimezone().isoformat()
        })

    return sorted(outarr, key=lambda x: (~x["online"], x["mac"]))


def parse(list_args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-J", "--json", action="store_true",
                        help="Output as JSON.")
    parser.add_argument("-o", "--output-file", nargs='?',
                        type=argparse.FileType('w'), default=sys.stdout,
                        help="Output result to file, default is to stdout'")
    parser.add_argument("-l", "--log-level", default="warning",
                        help="Provide logging level, default is warning'")
    if (list_args is None):
        return parser.parse_args()
    else:
        return parser.parse_args(args=list_args)


if __name__ == '__main__':
    args = parse()
    # Setup
    logging.basicConfig(level=args.log_level.upper())
    cred = credentials.Certificate(
        "nd-schmidt-firebase-adminsdk-d1gei-43db929d8a.json")
    firebase_admin.initialize_app(cred, {
        "databaseURL": "https://nd-schmidt-default-rtdb.firebaseio.com"
    })

    # List devices and write output
    outarr = list_devices()
    logging.debug(outarr)
    write(outarr, args)
    print("Done!")
