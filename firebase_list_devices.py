import argparse
import csv
from datetime import datetime
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import json
import logging
import sys


def write(outarr, args):
    if (args.json):
        args.output_file.write(json.dumps(outarr))
    else:
        csv_writer = csv.DictWriter(
            args.output_file,
            fieldnames=["mac", "online", "start_timestamp", "last_timestamp"])
        csv_writer.writeheader()
        csv_writer.writerows(outarr)


def fetch_all():
    return db.reference("hb_append").get()


def get_list(heartbeats):
    outarr = list()
    now_timestamp = datetime.timestamp(datetime.now()) * 1000

    for mac in heartbeats:
        last = sorted(list(heartbeats[mac].values()),
                      key=lambda x: x["last_timestamp"],
                      reverse=True)[0]
        logging.debug(mac, last)
        # Online threshold = 1h 2m
        outarr.append({
            "mac": mac,
            "online": (now_timestamp - last["last_timestamp"]) < 3720000,
            "start_timestamp": last["start_timestamp"],
            "last_timestamp": last["last_timestamp"]})

    return sorted(outarr, key=lambda x: (~x["online"], x["mac"]))


def get_mac(heartbeats, mac_filter):
    outarr = list()

    for mac in heartbeats:
        if (mac_filter == "" or mac == mac_filter):
            for entry in heartbeats[mac].values():
                outarr.append({
                    "mac": mac,
                    "online": True,
                    "start_timestamp": entry["start_timestamp"],
                    "last_timestamp": entry["last_timestamp"]})

    return sorted(outarr, key=lambda x: (~x["online"], x["mac"]))


def list_devices(mac_output="disabled"):
    heartbeats = fetch_all()

    if (mac_output == "disabled"):
        return get_list(heartbeats)
    else:
        return get_mac(heartbeats, mac_output)


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
