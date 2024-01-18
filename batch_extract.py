import argparse
from datetime import datetime
import json
import logging
from pathlib import Path
import subprocess
import firebase_admin
from firebase_admin import credentials
import firebase_download
import firebase_list_devices
import write_csv


def batch_extract(args):
    # Setup
    logging.basicConfig(level=args.log_level.upper())

    print("1. Download and zip all logs")
    firebase_download.download(args)
    cmd_out = subprocess.check_output(
        ["zip", "-r",
         str(args.outdir.joinpath("all_rawlogs.zip").resolve()),
         "."], cwd=str(args.log_dir.resolve())).decode("utf-8")
    logging.debug(cmd_out)

    print("2. Download and write device states")
    list_devices = firebase_list_devices.list_devices()
    macs = [entry["mac"] for entry in list_devices]
    logging.info(macs)
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

    print("3. Write CSV and JSON for each MAC")
    for mac in macs:
        out_tput = write_csv.read_logs(write_csv.parse(
            ["throughput",
             "-d", str(args.log_dir),
             "-m", mac,
             "-l", args.log_level]))
        out_lat = write_csv.read_logs(write_csv.parse(
            ["latency",
             "-d", str(args.log_dir),
             "-m", mac,
             "-l", args.log_level]))
        out_scan = write_csv.read_logs(write_csv.parse(
            ["wifi_scan",
             "-d", str(args.log_dir),
             "-m", mac,
             "-l", args.log_level]))
        out_hbeat = firebase_list_devices.list_devices(mac)

        # 3.1. Write tput
        if (len(out_tput) > 0):
            write_csv.write(out_tput, write_csv.parse(
                ["throughput", "-J",
                 "-o", str(args.outdir.joinpath(
                           "throughput_{}.json".format(mac))),
                 "-l", args.log_level]))
            write_csv.write(out_tput, write_csv.parse(
                ["throughput",
                 "-o", str(args.outdir.joinpath(
                           "throughput_{}.csv".format(mac))),
                 "-l", args.log_level]))

        # 3.2. Write lat
        if (len(out_lat) > 0):
            write_csv.write(out_lat, write_csv.parse(
                ["latency", "-J",
                 "-o", str(args.outdir.joinpath(
                           "latency_{}.json".format(mac))),
                 "-l", args.log_level]))
            write_csv.write(out_lat, write_csv.parse(
                ["latency",
                 "-o", str(args.outdir.joinpath(
                           "latency_{}.csv".format(mac))),
                 "-l", args.log_level]))

        # 3.3. Write wifi_scan
        if (len(out_scan) > 0):
            write_csv.write(out_scan, write_csv.parse(
                ["wifi_scan", "-J",
                 "-o", str(args.outdir.joinpath(
                           "wifi_scan_{}.json".format(mac))),
                 "-l", args.log_level]))
            write_csv.write(out_scan, write_csv.parse(
                ["wifi_scan",
                 "-o", str(args.outdir.joinpath(
                           "wifi_scan_{}.csv".format(mac))),
                 "-l", args.log_level]))

        # 3.4. Write heartbeats
        if (len(out_hbeat) > 0):
            firebase_list_devices.write(
                out_hbeat,
                firebase_list_devices.parse(
                    ["-J", "-m", mac, "-o", str(args.outdir.joinpath(
                        "heartbeat_{}.json".format(mac))),
                     "-l", args.log_level]))
            firebase_list_devices.write(
                out_hbeat,
                firebase_list_devices.parse(
                    ["-m", mac, "-o", str(args.outdir.joinpath(
                        "heartbeat_{}.csv".format(mac))),
                     "-l", args.log_level]))

        # 3.5. Zip CSVs & JSONs
        if (len(out_tput) > 0 or len(out_lat) > 0):
            cmd_out = subprocess.check_output(
                ["zip", "-r",
                 str(args.outdir.joinpath(
                     "data_{}.zip".format(mac)).resolve()),
                 ".", "-i",
                 "*_{}.json".format(mac),
                 "*_{}.csv".format(mac)],
                cwd=str(args.outdir.resolve())).decode("utf-8")
            logging.debug(cmd_out)

        # 3.6. Zip raw logs
        if (args.log_dir.joinpath(mac).is_dir()):
            cmd_out = subprocess.check_output(
                ["zip", "-r",
                 str(args.outdir.joinpath(
                     "rawlogs_{}.zip".format(mac)).resolve()),
                 "."],
                cwd=str(args.log_dir.joinpath(mac).resolve())).decode("utf-8")
            logging.debug(cmd_out)

    print("4. Zip all CSVs & JSONs")
    cmd_out = subprocess.check_output(
        ["zip", "-r",
         str(args.outdir.joinpath("all_data.zip").resolve()),
         ".", "-i", "*.json", "*.csv"],
        cwd=str(args.outdir.resolve())).decode("utf-8")
    logging.debug(cmd_out)

    print("5. Write last update JSON")
    with open(args.outdir.joinpath("last_update.json"), "w") as fd:
        fd.write(json.dumps({
            "last_update": datetime.now().astimezone().isoformat(
                timespec="seconds")}))

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
