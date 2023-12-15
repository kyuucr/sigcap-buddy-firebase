import argparse
from datetime import datetime
import json
import logging
from pathlib import Path
import sys


def get_tput_csv_line(mac, json_dict):
    if ("error" in json_dict):
        return ""

    if ("start" in json_dict):
        # iperf file
        outstring = datetime.fromtimestamp(
            json_dict["start"]["timestamp"]["timesecs"]
        ).astimezone().isoformat(timespec="seconds") + ","
        outstring += mac + ","
        outstring += "iperf,"
        outstring += ("downlink"
                      if json_dict["start"]["test_start"]["reverse"] == 1
                      else "uplink") + ","
        outstring += (json_dict["start"]["interface"]
                      if "interface" in json_dict["start"]
                      else "eth0") + ","
        outstring += json_dict["start"]["connecting_to"]["host"] + ":"
        outstring += str(json_dict["start"]["connecting_to"]["port"]) + ","
        outstring += "NaN,"
        outstring += str(json_dict["end"]["sum_received"]["seconds"]) + ","
        outstring += str(json_dict["end"]["sum_received"]["bytes"] / 1e6) + ","
        outstring += str(
            json_dict["end"]["sum_received"]["bits_per_second"] / 1e6) + "\n"
        return outstring
    elif ("type" in json_dict):
        # new speedtest file
        timestamp = datetime.fromisoformat(
            json_dict["timestamp"]).astimezone().isoformat(timespec="seconds")
        outstring = timestamp + ","
        outstring += mac + ","
        outstring += "speedtest,"
        outstring += "downlink,"
        outstring += json_dict["interface"]["name"] + ","
        outstring += json_dict["server"]["host"] + ","
        outstring += json_dict["isp"] + ","
        outstring += str(json_dict["download"]["elapsed"] / 1e3) + ","
        outstring += str(json_dict["download"]["bytes"] / 1e6) + ","
        outstring += str(json_dict["download"]["bandwidth"] * 8 / 1e6) + "\n"
        outstring += timestamp + ","
        outstring += mac + ","
        outstring += "speedtest,"
        outstring += "uplink,"
        outstring += json_dict["interface"]["name"] + ","
        outstring += json_dict["server"]["host"] + ","
        outstring += json_dict["isp"] + ","
        outstring += str(json_dict["upload"]["elapsed"] / 1e3) + ","
        outstring += str(json_dict["upload"]["bytes"] / 1e6) + ","
        outstring += str(json_dict["upload"]["bandwidth"] * 8 / 1e6) + "\n"
        return outstring
    elif ("beacons" in json_dict):
        return ""
    else:
        # old speedtest file
        timestamp = datetime.fromisoformat(
            json_dict["timestamp"]).astimezone().isoformat(timespec="seconds")
        outstring = timestamp + ","
        outstring += mac + ","
        outstring += "speedtest,"
        outstring += "downlink,"
        outstring += "eth0,"
        outstring += json_dict["server"]["host"] + ","
        outstring += json_dict["client"]["isp"] + ","
        outstring += "NaN,"
        outstring += str(json_dict["bytes_received"] / 1e6) + ","
        outstring += str(json_dict["download"] / 1e6) + "\n"
        outstring += timestamp + ","
        outstring += mac + ","
        outstring += "speedtest,"
        outstring += "uplink,"
        outstring += "eth0,"
        outstring += json_dict["server"]["host"] + ","
        outstring += json_dict["client"]["isp"] + ","
        outstring += "NaN,"
        outstring += str(json_dict["bytes_sent"] / 1e6) + ","
        outstring += str(json_dict["upload"] / 1e6) + "\n"
        return outstring


def get_lat_csv_line(mac, json_dict):
    return ""


def get_scan_csv_line(mac, json_dict):
    return ""


def get_csv_line(mode, mac, json_dict):
    logging.debug("mode=%s,mac=%s", mode, mac)
    match mode:
        case "throughput":
            return get_tput_csv_line(mac, json_dict)
        case "latency":
            return get_lat_csv_line(mac, json_dict)
        case "wifi_scan":
            return get_scan_csv_line(mac, json_dict)


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

    outstring = ""
    match args.mode:
        case "throughput":
            outstring += ("timestamp,mac,type,direction,interface,host,isp,"
                          "duration_s,transfered_mbytes,tput_mbps\n")
        case "latency":
            outstring += ("timestamp,mac,type,interface,host,isp,"
                          "latency_ms,jitter_ms\n")
        case "wifi_scan":
            outstring += ("timestamp,mac,bssid,ssid,rssi,primary_channel,"
                          "primary_freq\n")

    for file in files:
        logging.debug("Reading %s", file)
        with open(file) as fd:
            try:
                outstring += get_csv_line(
                    args.mode,
                    file.parts[1],
                    json.load(fd))
            except Exception as err:
                logging.warning("Cannot parse file %s, reason=%s",
                                file, err)

    args.output_file.write(outstring)
    print("Done!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["throughput", "latency", "wifi_scan"],
                        help="Choose type of CSV files.'")
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
