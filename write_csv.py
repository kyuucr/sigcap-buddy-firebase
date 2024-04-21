import argparse
import csv
from datetime import datetime
import json
import logging
import numpy as np
from pathlib import Path
import sys
import wifi_helper


def dict_reader(input_dict, nan_val, *dict_args):
    curr = input_dict

    # recursively read curr until arg not found or reached the end
    for arg in dict_args:
        if (arg not in curr):
            return nan_val
        curr = curr[arg]

    return curr


def get_tput_line(mac, json_dict):
    outarr = list()
    if ("error" in json_dict):
        return outarr

    if ("start" in json_dict):
        # iperf file
        iperf_tputs = list(map(
            lambda x: x["sum"]["bits_per_second"] / 1e6,
            json_dict["intervals"]))
        outarr.append({
            "timestamp": datetime.fromtimestamp(
                json_dict["start"]["timestamp"]["timesecs"]
            ).astimezone().isoformat(timespec="seconds"),
            "mac": mac,
            "test_uuid": (json_dict["start"]["test_uuid"]
                          if "test_uuid" in json_dict["start"]
                          else "unknown"),
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
            "isp": "unknown",
            "duration_s": json_dict["end"]["sum_received"]["seconds"],
            "transfered_mbytes":
                json_dict["end"]["sum_received"]["bytes"] / 1e6,
            "tput_mbps":
                json_dict["end"]["sum_received"]["bits_per_second"] / 1e6,
            "std_tput_mbps": np.std(iperf_tputs),
            "max_tput_mbps": max(iperf_tputs),
            "min_tput_mbps": min(iperf_tputs),
            "median_tput_mbps": np.median(iperf_tputs)
        })
    elif ("type" in json_dict):
        # new speedtest file
        timestamp = datetime.fromisoformat(
            json_dict["timestamp"]).astimezone().isoformat(timespec="seconds")
        test_uuid = (json_dict["test_uuid"]
                     if "test_uuid" in json_dict
                     else "unknown")
        outarr.append({
            "timestamp": timestamp,
            "mac": mac,
            "test_uuid": test_uuid,
            "type": "speedtest",
            "direction": "downlink",
            "interface": json_dict["interface"]["name"],
            "host": json_dict["server"]["host"],
            "isp": json_dict["isp"],
            "duration_s": json_dict["download"]["elapsed"] / 1e3,
            "transfered_mbytes": json_dict["download"]["bytes"] / 1e6,
            "tput_mbps": json_dict["download"]["bandwidth"] * 8 / 1e6,
            "std_tput_mbps": "NaN",
            "max_tput_mbps": json_dict["download"]["bandwidth"] * 8 / 1e6,
            "min_tput_mbps": json_dict["download"]["bandwidth"] * 8 / 1e6,
            "median_tput_mbps": json_dict["download"]["bandwidth"] * 8 / 1e6
        })
        outarr.append({
            "timestamp": timestamp,
            "mac": mac,
            "test_uuid": test_uuid,
            "type": "speedtest",
            "direction": "uplink",
            "interface": json_dict["interface"]["name"],
            "host": json_dict["server"]["host"],
            "isp": json_dict["isp"],
            "duration_s": json_dict["upload"]["elapsed"] / 1e3,
            "transfered_mbytes": json_dict["upload"]["bytes"] / 1e6,
            "tput_mbps": json_dict["upload"]["bandwidth"] * 8 / 1e6,
            "std_tput_mbps": "NaN",
            "max_tput_mbps": json_dict["upload"]["bandwidth"] * 8 / 1e6,
            "min_tput_mbps": json_dict["upload"]["bandwidth"] * 8 / 1e6,
            "median_tput_mbps": json_dict["upload"]["bandwidth"] * 8 / 1e6
        })
    elif ("pings" in json_dict):
        pass
    elif ("beacons" in json_dict):
        pass
    else:
        # old speedtest file
        timestamp = datetime.fromisoformat(
            json_dict["timestamp"]).astimezone().isoformat(timespec="seconds")
        outarr.append({
            "timestamp": timestamp,
            "mac": mac,
            "test_uuid": "unknown",
            "type": "speedtest",
            "direction": "downlink",
            "interface": "eth0",
            "host": json_dict["server"]["host"],
            "isp": json_dict["client"]["isp"],
            "duration_s": "NaN",
            "transfered_mbytes": json_dict["bytes_received"] / 1e6,
            "tput_mbps": json_dict["download"] / 1e6,
            "std_tput_mbps": "NaN",
            "max_tput_mbps": json_dict["download"] / 1e6,
            "min_tput_mbps": json_dict["download"] / 1e6,
            "median_tput_mbps": json_dict["download"] / 1e6
        })
        outarr.append({
            "timestamp": timestamp,
            "mac": mac,
            "test_uuid": "unknown",
            "type": "speedtest",
            "direction": "uplink",
            "interface": "eth0",
            "host": json_dict["server"]["host"],
            "isp": json_dict["client"]["isp"],
            "duration_s": "NaN",
            "transfered_mbytes": json_dict["bytes_sent"] / 1e6,
            "tput_mbps": json_dict["upload"] / 1e6,
            "std_tput_mbps": "NaN",
            "max_tput_mbps": json_dict["upload"] / 1e6,
            "min_tput_mbps": json_dict["upload"] / 1e6,
            "median_tput_mbps": json_dict["upload"] / 1e6
        })

    logging.debug(outarr)
    return outarr


def get_lat_line(mac, json_dict):
    outarr = list()
    if ("error" in json_dict):
        return outarr

    if ("start" in json_dict):
        # iperf file
        pass
    elif ("type" in json_dict):
        # new speedtest file
        timestamp = datetime.fromisoformat(
            json_dict["timestamp"]).astimezone().isoformat(timespec="seconds")
        test_uuid = (json_dict["test_uuid"]
                     if "test_uuid" in json_dict
                     else "unknown")
        outarr.append({
            "timestamp": timestamp,
            "mac": mac,
            "test_uuid": test_uuid,
            "type": "speedtest_idle",
            "interface": json_dict["interface"]["name"],
            "host": json_dict["server"]["host"],
            "isp": json_dict["isp"],
            "latency_ms": json_dict["ping"]["latency"],
            "jitter_ms": json_dict["ping"]["jitter"],
            "min_latency_ms": json_dict["ping"]["latency"],
            "max_latency_ms": json_dict["ping"]["latency"],
            "median_latency_ms": json_dict["ping"]["latency"]
        })
        if ("latency" in json_dict["download"]):
            outarr.append({
                "timestamp": timestamp,
                "mac": mac,
                "test_uuid": test_uuid,
                "type": "speedtest_dl_load",
                "interface": json_dict["interface"]["name"],
                "host": json_dict["server"]["host"],
                "isp": json_dict["isp"],
                "latency_ms": json_dict["download"]["latency"]["iqm"],
                "jitter_ms": json_dict["download"]["latency"]["jitter"],
                "min_latency_ms": json_dict["download"]["latency"]["iqm"],
                "max_latency_ms": json_dict["download"]["latency"]["iqm"],
                "median_latency_ms": json_dict["download"]["latency"]["iqm"]
            })
        if ("latency" in json_dict["upload"]):
            outarr.append({
                "timestamp": timestamp,
                "mac": mac,
                "test_uuid": test_uuid,
                "type": "speedtest_ul_load",
                "interface": json_dict["interface"]["name"],
                "host": json_dict["server"]["host"],
                "isp": json_dict["isp"],
                "latency_ms": json_dict["upload"]["latency"]["iqm"],
                "jitter_ms": json_dict["upload"]["latency"]["jitter"],
                "min_latency_ms": json_dict["upload"]["latency"]["iqm"],
                "max_latency_ms": json_dict["upload"]["latency"]["iqm"],
                "median_latency_ms": json_dict["upload"]["latency"]["iqm"]
            })
    elif ("pings" in json_dict):
        if (json_dict["pings"] is not None):
            iface = json_dict["interface"]
            test_uuid = json_dict["extra"]["test_uuid"]
            corr_test = "ping_" + json_dict["extra"]["corr_test"].replace(
                "-", "_")
            for entry in json_dict["pings"]:
                if (len(entry["responses"]) > 0):
                    latencies_ms = list(map(
                        lambda x: x["time_ms"],
                        entry["responses"]))
                    outarr.append({
                        "timestamp": datetime.fromisoformat(
                            entry["responses"][0]["timestamp"]
                        ).astimezone().isoformat(timespec="seconds"),
                        "mac": mac,
                        "test_uuid": test_uuid,
                        "type": corr_test,
                        "interface": iface,
                        "host": entry["destination"],
                        "isp": "unknown",
                        "latency_ms": dict_reader(
                            entry, "NaN", "round_trip_ms_avg"),
                        "jitter_ms": dict_reader(
                            entry, "NaN", "round_trip_ms_stddev"),
                        "min_latency_ms": dict_reader(
                            entry, "NaN", "round_trip_ms_min"),
                        "max_latency_ms": dict_reader(
                            entry, "NaN", "round_trip_ms_max"),
                        "median_latency_ms": np.median(latencies_ms)
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
            "test_uuid": "unknown",
            "type": "speedtest_idle",
            "interface": "eth0",
            "host": json_dict["server"]["host"],
            "isp": json_dict["client"]["isp"],
            "latency_ms": json_dict["ping"],
            "jitter_ms": "NaN",
            "min_latency_ms": json_dict["ping"],
            "max_latency_ms": json_dict["ping"],
            "median_latency_ms": json_dict["ping"]
        })

    logging.debug(outarr)
    return outarr


def get_scan_line(mac, json_dict):
    outlist = list()
    if ("beacons" not in json_dict or json_dict["beacons"] is None):
        return outlist

    timestamp = datetime.fromisoformat(
        json_dict["timestamp"]).astimezone().isoformat(timespec="seconds")
    test_uuid = "unknown"
    corr_test = "unknown"
    if ("extra" in json_dict):
        test_uuid = (json_dict["extra"]["test_uuid"]
                     if "test_uuid" in json_dict["extra"]
                     else "unknown")
        corr_test = (json_dict["extra"]["corr_test"]
                     if "corr_test" in json_dict["extra"]
                     else "unknown")
    for beacon in json_dict["beacons"]:
        primary_ch = int(beacon["channel"])
        primary_freq = int(float(
            beacon["freq"][:len(beacon["freq"]) - 4]) * 1000)
        outtemp = {
            "timestamp": timestamp,
            "mac": mac,
            "test_uuid": test_uuid,
            "corr_test": corr_test,
            "bssid": beacon["bssid"],
            "ssid": beacon["ssid"],
            "rssi_dbm": int(beacon["rssi"][:(len(beacon["rssi"]) - 4)]),
            "primary_channel_num": primary_ch,
            "primary_freq_mhz": primary_freq,
            "channel_num": primary_ch,
            "center_freq0_mhz": primary_freq,
            "center_freq1_mhz": 0,
            "bw_mhz": 0,
            "amendment": "unknown",
            "connected": "unknown" if "connected" not in beacon
            else beacon["connected"],
            "tx_power_dbm": "NaN",
            "link_margin_db": "NaN",
            "sta_count": "NaN",
            "ch_utilization": "NaN",
            "available_admission_capacity_sec": "NaN"
        }

        he_element = [x for x in beacon["extras"]
                      if x["type"] == "HE Operation"]
        if (len(he_element) > 0):
            he_element = he_element[0]["elements"]
            logging.debug(he_element)

            outtemp["amendment"] = "11ax"

        vht_element = [x for x in beacon["extras"]
                       if x["type"] == "VHT Operation"]
        if (len(vht_element) > 0):
            vht_element = vht_element[0]["elements"]
            logging.debug(vht_element)

            if (outtemp["amendment"] == "unknown"):
                outtemp["amendment"] = "11ac"

            # Resolve bandwidth
            match vht_element["channel_width"]:
                case 0:
                    # 20 or 40 MHz
                    freq_bw0 = wifi_helper.get_channel_from_num(
                        wifi_helper.get_freq_code(primary_freq),
                        vht_element["channel_center_freq_0"])

                    # This will resolve the channel bw
                    if (freq_bw0 is not None):
                        outtemp["channel_num"] = freq_bw0[0]
                        outtemp["center_freq0_mhz"] = freq_bw0[1]
                        outtemp["bw_mhz"] = freq_bw0[4]
                    else:
                        # If channel number not found, this must be 20 Mhz
                        outtemp["bw_mhz"] = 20
                case 1:
                    # 80 or 160 or 80+80 MHz
                    freq_bw0 = wifi_helper.get_channel_from_num(
                        wifi_helper.get_freq_code(primary_freq),
                        vht_element["channel_center_freq_0"])
                    freq_bw1 = wifi_helper.get_channel_from_num(
                        wifi_helper.get_freq_code(primary_freq),
                        vht_element["channel_center_freq_1"])

                    if (freq_bw0 is not None and freq_bw1 is not None):
                        # This might be 160 or 80+80 MHz
                        outtemp["bw_mhz"] = 160
                        outtemp["center_freq0_mhz"] = freq_bw0[1]
                        outtemp["center_freq1_mhz"] = freq_bw1[1]
                        if (freq_bw1[4] == 80):
                            # Must be 80+80 MHz
                            # Use the first channel segment number,
                            # may change later
                            outtemp["channel_num"] = freq_bw0[0]
                        elif (freq_bw1[4] == 160):
                            # Must be 160 MHz
                            # Use the second channel segment number
                            outtemp["channel_num"] = freq_bw1[0]
                    elif (freq_bw0 is not None):
                        # This must be 80 MHz
                        outtemp["channel_num"] = freq_bw0[0]
                        outtemp["center_freq0_mhz"] = freq_bw0[1]
                        outtemp["bw_mhz"] = freq_bw0[4]
                case 2:
                    # 160 MHz (deprecated)
                    freq_bw0 = wifi_helper.get_channel_from_num(
                        wifi_helper.get_freq_code(primary_freq),
                        vht_element["channel_center_freq_0"])
                    if (freq_bw0 is not None):
                        outtemp["channel_num"] = freq_bw0[0]
                        outtemp["center_freq0_mhz"] = freq_bw0[1]
                        outtemp["bw_mhz"] = freq_bw0[4]
                case 3:
                    # 80+80 MHz (deprecated)
                    outtemp["bw_mhz"] = 160
                    freq_bw0 = wifi_helper.get_channel_from_num(
                        wifi_helper.get_freq_code(primary_freq),
                        vht_element["channel_center_freq_0"])
                    freq_bw1 = wifi_helper.get_channel_from_num(
                        wifi_helper.get_freq_code(primary_freq),
                        vht_element["channel_center_freq_1"])

                    # Check the second channel segment first
                    if (freq_bw1 is not None):
                        outtemp["center_freq1_mhz"] = freq_bw1[1]
                        outtemp["channel_num"] = freq_bw1[0]
                    if (freq_bw0 is not None):
                        outtemp["center_freq0_mhz"] = freq_bw0[1]
                        outtemp["channel_num"] = freq_bw0[0]

        ht_element = [x for x in beacon["extras"]
                      if x["type"] == "HT Operation"]
        if (len(ht_element) > 0):
            ht_element = ht_element[0]["elements"]
            logging.debug(ht_element)

            if (outtemp["amendment"] == "unknown"):
                outtemp["amendment"] = "11n"

            # Resolve bandwidth if it hasn't been resolved yet
            if (outtemp["bw_mhz"] == 0):
                outtemp["bw_mhz"] = 20
                if (ht_element["sta_channel_width"] == 1):
                    ch = wifi_helper.get_channel_from_freq(
                        primary_freq, 40)
                    if (ch is not None):
                        outtemp["channel_num"] = ch[0]
                        outtemp["center_freq0_mhz"] = ch[1]
                        outtemp["bw_mhz"] = 40

        tpc_element = [x for x in beacon["extras"]
                       if x["type"] == "TPC Report"]
        if (len(tpc_element) > 0):
            tpc_element = tpc_element[0]["elements"]
            logging.debug(tpc_element)

            outtemp["tx_power_dbm"] = tpc_element["tx_power"]
            outtemp["link_margin_db"] = tpc_element["link_margin"]

        bss_load_element = [x for x in beacon["extras"]
                            if x["type"] == "BSS Load"]
        if (len(bss_load_element) > 0):
            bss_load_element = bss_load_element[0]["elements"]
            logging.debug(bss_load_element)

            outtemp["sta_count"] = bss_load_element["sta_count"]
            outtemp["ch_utilization"] = bss_load_element["ch_utilization"]
            if ("available_admission_cap" in bss_load_element):
                outtemp["available_admission_capacity_sec"] = bss_load_element[
                    "available_admission_cap"] * 32 / 1e6

        logging.debug(outtemp)
        # Only add to list if we got bandwidth resolved
        if (outtemp["bw_mhz"] != 0):
            outlist.append(outtemp)

    return outlist


def get_line(mode, mac, json_dict):
    logging.info("mode=%s,mac=%s", mode, mac)
    match mode:
        case "throughput":
            return get_tput_line(mac, json_dict)
        case "latency":
            return get_lat_line(mac, json_dict)
        case "wifi_scan":
            return get_scan_line(mac, json_dict)


def write(outarr, args):
    if (args.json):
        args.output_file.write(json.dumps(outarr))
    else:
        fieldnames = []
        match args.mode:
            case "throughput":
                fieldnames += ["timestamp", "mac", "test_uuid", "type",
                               "direction", "interface", "host", "isp",
                               "duration_s", "transfered_mbytes", "tput_mbps",
                               "std_tput_mbps", "max_tput_mbps",
                               "min_tput_mbps", "median_tput_mbps"]
            case "latency":
                fieldnames += ["timestamp", "mac", "test_uuid", "type",
                               "interface", "host", "isp", "latency_ms",
                               "jitter_ms", "min_latency_ms", "max_latency_ms",
                               "median_latency_ms"]
            case "wifi_scan":
                fieldnames += ["timestamp", "mac", "test_uuid", "corr_test",
                               "bssid", "ssid", "rssi_dbm",
                               "primary_channel_num", "primary_freq_mhz",
                               "channel_num", "center_freq0_mhz",
                               "center_freq1_mhz", "bw_mhz", "amendment",
                               "connected", "tx_power_dbm", "link_margin_db",
                               "sta_count", "ch_utilization",
                               "available_admission_capacity_sec"]

        csv_writer = csv.DictWriter(
            args.output_file, fieldnames=fieldnames)
        csv_writer.writeheader()
        csv_writer.writerows(outarr)


def read_logs(args):
    if (args.mac):
        logging.info(args.mac)
        files = []
        for mac in args.mac:
            currdir = args.log_dir.joinpath(mac)
            files += [path for path in currdir.rglob("*") if path.is_file()]
    else:
        files = [path for path in args.log_dir.rglob("*") if path.is_file()]
    logging.info(files)

    outarr = list()

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
                outarr += get_line(
                    args.mode,
                    file.parts[1],
                    json_dict)

    return outarr


def parse(list_args=None):
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
    if (list_args is None):
        return parser.parse_args()
    else:
        return parser.parse_args(args=list_args)


if __name__ == '__main__':
    # Setup
    args = parse()
    logging.basicConfig(level=args.log_level.upper())

    outarr = read_logs(args)
    write(outarr, args)
    print("Done!")
