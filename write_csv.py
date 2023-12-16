import argparse
import csv
from datetime import datetime
import json
import logging
from pathlib import Path
import sys


def get_tput_line(mac, json_dict):
    outarr = list()
    if ("error" in json_dict):
        return outarr

    if ("start" in json_dict):
        # iperf file
        outarr.append({
            "timestamp": datetime.fromtimestamp(
                json_dict["start"]["timestamp"]["timesecs"]
            ).astimezone().isoformat(timespec="seconds"),
            "mac": mac,
            "type": "iperf",
            "direction": ("downlink"
                          if json_dict["start"]["test_start"]["reverse"] == 1
                          else "uplink"),
            "interface": (json_dict["start"]["interface"]
                          if "interface" in json_dict["start"]
                          else "eth0"),
            "host": "{}:{}".format(
                json_dict["start"]["connecting_to"]["host"],
                str(json_dict["start"]["connecting_to"]["port"])),
            "isp": "NaN",
            "duration_s": json_dict["end"]["sum_received"]["seconds"],
            "transfered_mbytes":
                json_dict["end"]["sum_received"]["bytes"] / 1e6,
            "tput_mbps":
                json_dict["end"]["sum_received"]["bits_per_second"] / 1e6,
        })
    elif ("type" in json_dict):
        # new speedtest file
        timestamp = datetime.fromisoformat(
            json_dict["timestamp"]).astimezone().isoformat(timespec="seconds")
        outarr.append({
            "timestamp": timestamp,
            "mac": mac,
            "type": "speedtest",
            "direction": "downlink",
            "interface": json_dict["interface"]["name"],
            "host": json_dict["server"]["host"],
            "isp": json_dict["isp"],
            "duration_s": json_dict["download"]["elapsed"] / 1e3,
            "transfered_mbytes": json_dict["download"]["bytes"] / 1e6,
            "tput_mbps": json_dict["download"]["bandwidth"] * 8 / 1e6
        })
        outarr.append({
            "timestamp": timestamp,
            "mac": mac,
            "type": "speedtest",
            "direction": "uplink",
            "interface": json_dict["interface"]["name"],
            "host": json_dict["server"]["host"],
            "isp": json_dict["isp"],
            "duration_s": json_dict["upload"]["elapsed"] / 1e3,
            "transfered_mbytes": json_dict["upload"]["bytes"] / 1e6,
            "tput_mbps": json_dict["upload"]["bandwidth"] * 8 / 1e6
        })
    elif ("beacons" in json_dict):
        pass
    else:
        # old speedtest file
        timestamp = datetime.fromisoformat(
            json_dict["timestamp"]).astimezone().isoformat(timespec="seconds")
        outarr.append({
            "timestamp": timestamp,
            "mac": mac,
            "type": "speedtest",
            "direction": "downlink",
            "interface": "eth0",
            "host": json_dict["server"]["host"],
            "isp": json_dict["client"]["isp"],
            "duration_s": "NaN",
            "transfered_mbytes": json_dict["bytes_received"] / 1e6,
            "tput_mbps": json_dict["download"] / 1e6
        })
        outarr.append({
            "timestamp": timestamp,
            "mac": mac,
            "type": "speedtest",
            "direction": "uplink",
            "interface": "eth0",
            "host": json_dict["server"]["host"],
            "isp": json_dict["client"]["isp"],
            "duration_s": "NaN",
            "transfered_mbytes": json_dict["bytes_sent"] / 1e6,
            "tput_mbps": json_dict["upload"] / 1e6
        })

    return outarr


def get_lat_line(mac, json_dict):
    return list()


def get_scan_line(mac, json_dict):
    return list()


def get_line(mode, mac, json_dict):
    logging.debug("mode=%s,mac=%s", mode, mac)
    match mode:
        case "throughput":
            return get_tput_line(mac, json_dict)
        case "latency":
            return get_lat_line(mac, json_dict)
        case "wifi_scan":
            return get_scan_line(mac, json_dict)


def write_csv(args):
    # Setup
    logging.basicConfig(level=args.log_level.upper())
    if (args.mac):
        logging.debug(args.mac)
        files = []
        for mac in args.mac:
            currdir = args.log_dir.joinpath(mac)
            files += [path for path in currdir.rglob("*") if path.is_file()]
    else:
        files = [path for path in args.log_dir.rglob("*") if path.is_file()]
    logging.debug(files)

    outarr = list()

    for file in files:
        logging.debug("Reading %s", file)
        with open(file) as fd:
            try:
                outarr += get_line(
                    args.mode,
                    file.parts[1],
                    json.load(fd))
            except Exception as err:
                logging.warning("Cannot parse file %s, reason=%s",
                                file, err)

    if (args.json):
        args.output_file.write(json.dumps(outarr))
    else:
        fieldnames = []
        match args.mode:
            case "throughput":
                fieldnames += ["timestamp", "mac", "type", "direction",
                               "interface", "host", "isp", "duration_s",
                               "transfered_mbytes", "tput_mbps"]
            case "latency":
                fieldnames += ["timestamp", "mac", "type", "interface",
                               "host", "isp", "latency_ms", "jitter_ms"]
            case "wifi_scan":
                fieldnames += ["timestamp", "mac", "bssid", "ssid", "rssi",
                               "primary_channel_num", "primary_freq",
                               "channel_num", "center_freq0", "center_freq1"
                               "bandwidth", "amendment"]

        csv_writer = csv.DictWriter(
            args.output_file, fieldnames=fieldnames)
        csv_writer.writeheader()
        csv_writer.writerows(outarr)
    print("Done!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["throughput", "latency", "wifi_scan"],
                        help="Choose type of CSV files.'")
    parser.add_argument("-J", "--json", action="store_true",
                        help="Output as JSON.")
    parser.add_argument("-m", "--mac", nargs='+',
                        help="Filter data to the speficied MAC address'")
    parser.add_argument("-o", "--output-file", nargs='?',
                        type=argparse.FileType('w'), default=sys.stdout,
                        help="Output result to file, default is to stdout'")
    parser.add_argument("-d", "--log-dir", type=Path, default=Path("./logs"),
                        help="Specify local log directory, default='./logs'")
    parser.add_argument("-l", "--log-level", default="warning",
                        help="Provide logging level, default is warning'")
    args = parser.parse_args()

    write_csv(args)
