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
            args.output_file,
            fieldnames=["mac", "online", "last_timestamp"]
            if (args.mac == "disabled") else ["mac", "last_timestamp", "test_uuid"])
        csv_writer.writeheader()
        csv_writer.writerows(outarr)


def list_devices(mac_output="disabled"):
    heartbeats = db.reference("heartbeat").order_by_child(
        "last_timestamp").get().values()

    if (mac_output == "disabled"):
        macs = list(dict.fromkeys(
            [val["mac"].replace(":", "-") for val in heartbeats]))
        logging.debug(macs)

        outarr = list()
        now_timestamp = datetime.timestamp(datetime.now()) * 1000

        for mac in macs:
            last_timestamp = get_last_timestamp(heartbeats, mac)
            outarr.append({
                "mac": mac,
                "online": (now_timestamp - last_timestamp) < 3720000,  # 1h 2m
                "last_timestamp": datetime.fromtimestamp(
                    last_timestamp / 1000).astimezone().isoformat()
            })

        return sorted(outarr, key=lambda x: (~x["online"], x["mac"]))

    else:
        outarr = list(map(lambda x: {
            "mac": x["mac"],
            "last_timestamp": datetime.fromtimestamp(
                x["last_timestamp"] / 1000).astimezone().isoformat(),
            "test_uuid": x["test_uuid"] if "test_uuid" in x else "N/A"
        }, heartbeats))
        if (mac_output is not None):
            outarr = list(filter(
                lambda x: x["mac"].replace(":", "-") == mac_output,
                outarr))
        return outarr


def parse(list_args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mac", nargs='?', default="disabled",
                        help="Output heartbeats for all or a specified MAC'")
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
    outarr = list_devices(args.mac)
    logging.debug(outarr)
    write(outarr, args)
    print("Done!")
