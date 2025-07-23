import argparse
import csv
from datetime import datetime, timezone
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
        fieldnames = ["mac", "online",
                      "start_timestamp", "last_timestamp"]
        if (args.include_legacy_mac):
            fieldnames.insert(1, 'rpi_id')
        if (args.mac == "disabled"):
            fieldnames += ["last_test_eth", "last_test_wlan",
                           "data_used_gbytes", "data_cap_gbytes"]
        csv_writer = csv.DictWriter(
            args.output_file,
            fieldnames=fieldnames,
            extrasaction='ignore')
        csv_writer.writeheader()
        csv_writer.writerows(outarr)


def fetch_all():
    return db.reference("hb_append").get()


def fetch_data_used():
    return db.reference("data_used").get()


def fetch_data_cap():
    query = db.reference("config").get()
    return {
        item["rpi_id"]: item["data_cap_gbytes"]
        for item in list(query.values())
        if ("rpi_id" in item and "data_cap_gbytes" in item)}


def read_json(file):
    logging.info("Reading %s", file)
    with open(file) as fd:
        try:
            json_dict = json.load(fd)
        except Exception as err:
            logging.debug("Cannot parse file %s as JSON, reason=%s",
                          file, err)
            return None
        else:
            return json_dict


def get_last_tests(mac, log_dir, now_timestamp):
    output = {
        "eth": None,
        "wla": None
    }
    search_thr = 2592000  # File search threshold: 1 month

    # Check speedtest first since speedtest runs last
    speedtest_dir = log_dir.joinpath(mac, "speedtest-log")
    speedtest_files = sorted(
        [path for path in speedtest_dir.rglob("*") if path.is_file()],
        reverse=True)

    logging.debug(speedtest_files)
    for file in speedtest_files:
        json_dict = read_json(file)
        if json_dict is None or "error" in json_dict:
            continue

        # Get interface and timestamp
        iface = (json_dict["interface"]["name"][:3]
                 if "interface" in json_dict
                 else "eth")
        time = datetime.fromisoformat(json_dict["timestamp"])
        logging.info("Got iface %s, time %s",
                     iface, time.isoformat(timespec="seconds"))

        # Assign output if it is unassigned
        if (output[iface] is None
                and (now_timestamp - time).total_seconds() <= search_thr):
            output[iface] = time

        # Exit if output is assigned for all interfaces
        # or the log timestamp is more than a day
        if ((output["eth"] is not None and output["wla"] is not None)
                or (now_timestamp - time).total_seconds() > search_thr):
            logging.info("Exiting log file reads, output: %s, time diff %ds",
                         output, (now_timestamp - time).total_seconds())
            break

    # Check iperf files if one of the interface timestamp is not assigned
    if (output["eth"] is None or output["wla"] is None):
        iperf_dir = log_dir.joinpath(mac, "iperf-log")
        iperf_files = sorted(
            [path for path in iperf_dir.rglob("*") if path.is_file()],
            reverse=True)

        logging.debug(iperf_files)
        for file in iperf_files:
            json_dict = read_json(file)
            if json_dict is None or "error" in json_dict:
                continue

            # Get interface and timestamp
            iface = (json_dict["start"]["interface"][:3]
                     if "interface" in json_dict["start"]
                     else "eth")
            time = datetime.fromtimestamp(
                json_dict["start"]["timestamp"]["timesecs"]).astimezone()
            logging.info("Got iface %s, time %s",
                         iface, time.isoformat(timespec="seconds"))

            # Assign output if it is unassigned
            if output[iface] is None:
                output[iface] = time

            # Exit if output is assigned for all interfaces
            # or the log timestamp is more than a day
            if ((output["eth"] is not None and output["wla"] is not None)
                    or (now_timestamp - time).total_seconds() > search_thr):
                logging.info(("Exiting log file reads, output: %s, "
                              "time diff %ds"),
                             output, (now_timestamp - time).total_seconds())
                break

    # Convert to timestamp
    output["eth"] = (datetime.timestamp(output["eth"]) * 1000
                     if output["eth"] is not None
                     else "NaN")
    output["wla"] = (datetime.timestamp(output["wla"]) * 1000
                     if output["wla"] is not None
                     else "NaN")
    logging.info("Last tests timestamp: %s", output)
    return output


def get_rpi_id(rpi_ids, mac):
    if not rpi_ids:
        return "NaN"

    logging.info("Get Pi ID for mac: %s", mac)
    rpi_id = [key for key, val in rpi_ids.items() if val == mac]
    logging.debug("Got rpi_id: %s", rpi_id)
    if len(rpi_id) == 0:
        logging.info("mac %s not found in the list of Pi IDs", mac)
        return "NaN"
    else:
        return rpi_id[0]


def get_list(heartbeats, log_dir, rpi_ids=None, include_legacy_mac=False):
    outarr = list()
    now_timestamp = datetime.now(timezone.utc).astimezone()
    logging.info("Current time: %s",
                 now_timestamp.isoformat(timespec="seconds"))
    data_used = fetch_data_used()
    data_cap = fetch_data_cap()

    for mac in heartbeats:
        if (not include_legacy_mac and not mac.startswith("RPI-")):
            logging.info(f"Skipping legacy mac {mac}.")
            continue
        heartbeats[mac] = {key: value for key, value in heartbeats[mac].items()
                           if isinstance(value, dict)}
        last = sorted(list(heartbeats[mac].values()),
                      key=lambda x: x["last_timestamp"],
                      reverse=True)[0]
        # Online threshold = 1h 2m
        online = (datetime.timestamp(now_timestamp) * 1000 - last[
            "last_timestamp"]) < 3720000    # ms
        last_tests = {"eth": "NaN", "wla": "NaN"}
        if online:
            last_tests = get_last_tests(mac, log_dir, now_timestamp)
        logging.debug(mac, last)

        # Data used
        data_used_gb = "NaN"
        if mac in data_used:
            temp = {key: value for key, value in data_used[mac].items()
                    if isinstance(value, dict)}
            data_used_gb = sorted(list(temp.values()),
                                  key=lambda x: x["last_timestamp"],
                                  reverse=True)[0]["data_used_gbytes"]
        # Data cap
        data_cap_gb = "NaN"
        if mac in data_cap:
            data_cap_gb = data_cap[mac]

        outarr.append({
            "mac": mac,
            "rpi_id": get_rpi_id(rpi_ids, mac),
            "online": online,
            "start_timestamp": last["start_timestamp"],
            "last_timestamp": last["last_timestamp"],
            "last_test_eth": last_tests["eth"],
            "last_test_wlan": last_tests["wla"],
            "data_used_gbytes": data_used_gb,
            "data_cap_gbytes": data_cap_gb})

    return sorted(outarr,
                  key=lambda x: (not x["online"], x["rpi_id"], x["mac"]))


def get_mac(heartbeats, mac_filter, rpi_ids=None):
    outarr = list()

    for mac in heartbeats:
        rpi_id = get_rpi_id(rpi_ids, mac)
        if (mac_filter == "" or mac == mac_filter):
            for entry in heartbeats[mac].values():
                outarr.append({
                    "mac": mac,
                    "rpi_id": rpi_id,
                    "online": True,
                    "start_timestamp": entry["start_timestamp"],
                    "last_timestamp": entry["last_timestamp"]})

    return sorted(outarr,
                  key=lambda x: (not x["online"], x["rpi_id"], x["mac"]))


def list_devices(args):
    heartbeats = fetch_all()
    rpi_ids = None
    if args.rpi_config.is_file():
        with open(args.rpi_config) as file:
            rpi_ids = json.load(file)

    if (args.mac == "disabled"):
        return get_list(
            heartbeats, args.log_dir, rpi_ids,
            args.include_legacy_mac)
    else:
        return get_mac(heartbeats, args.mac, rpi_ids)


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
    parser.add_argument("--rpi-config", type=Path,
                        default=Path("./.rpi-config.json"),
                        help="Specify RPI config file for RPI-ID translation")
    parser.add_argument("--include-legacy-mac", action="store_true",
                        help="Include legacy mac entries.")
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
