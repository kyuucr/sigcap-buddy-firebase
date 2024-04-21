import argparse
import csv
from datetime import datetime
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import json
import logging
from pathlib import Path
import sys


def write(outarr, args):
    if (args.json):
        args.output_file.write(json.dumps(outarr))
    else:
        fieldnames = []
        if (args.mac == "disabled"):
            fieldnames += ["mac", "online", "start_timestamp",
                           "last_timestamp", "last_test_eth", "last_test_wlan"]
        else:
            fieldnames += ["mac", "online", "start_timestamp",
                           "last_timestamp"]
        csv_writer = csv.DictWriter(
            args.output_file,
            fieldnames=fieldnames)
        csv_writer.writeheader()
        csv_writer.writerows(outarr)


def fetch_all():
    return db.reference("hb_append").get()


def get_last_tests(mac, log_dir):
    output = {
        "eth": None,
        "wla": None
    }

    iperf_dir = log_dir.joinpath(mac, "iperf-log")
    speedtest_dir = log_dir.joinpath(mac, "speedtest-log")
    # Sort desc and take the first 4
    files = sorted(
        [path for path in iperf_dir.rglob("*") if path.is_file()],
        reverse=True)[:4]
    # Sort desc and take the first 2
    files += sorted(
        [path for path in speedtest_dir.rglob("*") if path.is_file()],
        reverse=True)[:2]
    logging.info(files)

    for file in files:
        logging.info("Reading %s", file)
        with open(file) as fd:
            json_dict = dict()
            try:
                json_dict = json.load(fd)
            except Exception as err:
                logging.debug("Cannot parse file %s as JSON, reason=%s",
                              file, err)
            else:
                if ("error" in json_dict):
                    continue
                if ("start" in json_dict):
                    iface = (json_dict["start"]["interface"][:3]
                             if "interface" in json_dict["start"]
                             else "eth")
                    time = datetime.fromtimestamp(
                        json_dict["start"]["timestamp"]["timesecs"]
                    ).astimezone()
                else:
                    iface = (json_dict["interface"]["name"][:3]
                             if "interface" in json_dict
                             else "eth")
                    time = datetime.fromisoformat(
                        json_dict["timestamp"]).astimezone()

                logging.info("Got iface %s, time %s",
                             iface, time.isoformat(timespec="seconds"))
                if (output[iface] is None or output[iface] < time):
                    output[iface] = time

    # Convert to timestamp
    output["eth"] = (datetime.timestamp(output["eth"]) * 1000
                     if output["eth"] is not None
                     else "NaN")
    output["wla"] = (datetime.timestamp(output["wla"]) * 1000
                     if output["wla"] is not None
                     else "NaN")
    logging.info("Last tests timestamp: %s", output)
    return output


def get_list(heartbeats, log_dir):
    outarr = list()
    now_timestamp = datetime.timestamp(datetime.now()) * 1000

    for mac in heartbeats:
        last = sorted(list(heartbeats[mac].values()),
                      key=lambda x: x["last_timestamp"],
                      reverse=True)[0]
        last_tests = get_last_tests(mac, log_dir)
        logging.debug(mac, last)
        # Online threshold = 1h 2m
        outarr.append({
            "mac": mac,
            "online": (now_timestamp - last["last_timestamp"]) < 3720000,
            "start_timestamp": last["start_timestamp"],
            "last_timestamp": last["last_timestamp"],
            "last_test_eth": last_tests["eth"],
            "last_test_wlan": last_tests["wla"]})

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


def list_devices(args):
    heartbeats = fetch_all()

    if (args.mac == "disabled"):
        return get_list(heartbeats, args.log_dir)
    else:
        return get_mac(heartbeats, args.mac)


def parse(list_args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mac", nargs='?', default="disabled",
                        help="Output heartbeats for all or a specified MAC'")
    parser.add_argument("-J", "--json", action="store_true",
                        help="Output as JSON.")
    parser.add_argument("-o", "--output-file", nargs='?',
                        type=argparse.FileType('w'), default=sys.stdout,
                        help="Output result to file, default is to stdout'")
    parser.add_argument("-d", "--log-dir", type=Path, default=Path("./logs"),
                        help="Specify local log directory, default='./logs'")
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
    outarr = list_devices(args)
    logging.debug(outarr)
    write(outarr, args)
    print("Done!")
