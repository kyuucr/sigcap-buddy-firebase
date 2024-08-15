import argparse
import csv
from datetime import datetime
import json
import logging
import numpy as np
from pathlib import Path
import re
import sys
import wifi_helper


re_dbm = re.compile(r"([-\d\.]+) dBm")
re_mbps = re.compile(r"([-\d\.]+) MBit/s")


def dbm_to_mw(dbm):
    return 10 ** (dbm / 10)


def mw_to_dbm(mw):
    return 10 * np.log10(mw)


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
        iperf_mean_lat = np.mean([val["sender"]["mean_rtt"] / 1e3
                                  for val in json_dict["end"]["streams"]])
        if iperf_mean_lat == 0:
            return outarr
        iperf_min_lat = np.mean([val["sender"]["min_rtt"] / 1e3
                                 for val in json_dict["end"]["streams"]])
        iperf_max_lat = np.mean([val["sender"]["max_rtt"] / 1e3
                                 for val in json_dict["end"]["streams"]])
        outarr.append({
            "timestamp": datetime.fromtimestamp(
                json_dict["start"]["timestamp"]["timesecs"]
            ).astimezone().isoformat(timespec="seconds"),
            "mac": mac,
            "test_uuid": (json_dict["start"]["test_uuid"]
                          if "test_uuid" in json_dict["start"]
                          else "unknown"),
            "type": ("iperf-rtt-dl"
                     if json_dict["start"]["test_start"]["reverse"] == 1
                     else "iperf-rtt-ul"),
            "interface": (json_dict["start"]["interface"]
                          if "interface" in json_dict["start"]
                          else "eth0"),
            "host": "{}:{}".format(
                json_dict["start"]["connecting_to"]["host"],
                str(json_dict["start"]["connecting_to"]["port"])),
            "isp": "unknown",
            "latency_ms": iperf_mean_lat,
            "jitter_ms": "NaN",
            "min_latency_ms": iperf_min_lat,
            "max_latency_ms": iperf_max_lat,
            "median_latency_ms": "NaN"
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
            "type": "speedtest-idle",
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
                "type": "speedtest-dl",
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
                "type": "speedtest-ul",
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
            corr_test = "ping-" + json_dict["extra"]["corr_test"]
            for entry in json_dict["pings"]:
                entry["responses"] = list(
                    filter(lambda x: ("type" in x and x["type"] == "reply"),
                           entry["responses"]))
                if (len(entry["responses"]) > 0):
                    latencies_ms = list(map(
                        lambda x: x["time_ms"] if "time_ms" in x else "NaN",
                        entry["responses"]))
                    latencies_ms = [lat for lat in latencies_ms
                                    if lat != "NaN" and lat is not None]
                    lat_median = ("NaN" if len(latencies_ms) == 0
                                  else np.median(latencies_ms))
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
                        "median_latency_ms": lat_median
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
            "type": "speedtest-idle",
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
    logging.debug("json_dict exists!")

    timestamp = datetime.fromisoformat(
        json_dict["timestamp"]).astimezone().isoformat(timespec="seconds")
    test_uuid = "unknown"
    corr_test = "unknown"
    interface = (json_dict["interface"] if "interface" in json_dict
                 else "unknown")
    if ("extra" in json_dict):
        test_uuid = (json_dict["extra"]["test_uuid"]
                     if "test_uuid" in json_dict["extra"]
                     else "unknown")
        corr_test = (json_dict["extra"]["corr_test"]
                     if "corr_test" in json_dict["extra"]
                     else "unknown")

    # Process link
    links = {
        "link_mean_rssi_dbm": "NaN",
        "link_max_rssi_dbm": "NaN",
        "link_min_rssi_dbm": "NaN",
        "link_median_rssi_dbm": "NaN",
        "link_mean_tx_bitrate_mbps": "NaN",
        "link_max_tx_bitrate_mbps": "NaN",
        "link_min_tx_bitrate_mbps": "NaN",
        "link_median_tx_bitrate_mbps": "NaN",
        "link_mean_rx_bitrate_mbps": "NaN",
        "link_max_rx_bitrate_mbps": "NaN",
        "link_min_rx_bitrate_mbps": "NaN",
        "link_median_rx_bitrate_mbps": "NaN"
    }
    logging.debug("links in json_dict? %s", "links" in json_dict)
    if "links" in json_dict:
        logging.debug("rssi in json_dict? %s", "rssi" in json_dict["links"])
        if len(json_dict["links"]) > 0 and "rssi" in json_dict["links"][0]:
            rssi_dbm = [re_dbm.findall(val["rssi"])
                        for val in json_dict["links"]]
            rssi_dbm = np.array([int(val[0]) for val in rssi_dbm
                                 if len(val) > 0])
            if len(rssi_dbm) > 0:
                links["link_mean_rssi_dbm"] = mw_to_dbm(np.mean(dbm_to_mw(
                    rssi_dbm)))
                links["link_max_rssi_dbm"] = np.max(rssi_dbm)
                links["link_min_rssi_dbm"] = np.min(rssi_dbm)
                links["link_median_rssi_dbm"] = np.median(rssi_dbm)

        if (len(json_dict["links"]) > 0
                and "tx_bitrate" in json_dict["links"][0]):
            tx_bitrate_mbps = [re_mbps.findall(val["tx_bitrate"])
                               for val in json_dict["links"]]
            tx_bitrate_mbps = [float(val[0]) for val in tx_bitrate_mbps
                               if len(val) > 0]
            if len(tx_bitrate_mbps) > 0:
                links["link_mean_tx_bitrate_mbps"] = np.mean(tx_bitrate_mbps)
                links["link_max_tx_bitrate_mbps"] = np.max(tx_bitrate_mbps)
                links["link_min_tx_bitrate_mbps"] = np.min(tx_bitrate_mbps)
                links["link_median_tx_bitrate_mbps"] = np.median(
                    tx_bitrate_mbps)

        if (len(json_dict["links"]) > 0
                and "rx_bitrate" in json_dict["links"][0]):
            rx_bitrate_mbps = [re_mbps.findall(val["rx_bitrate"])
                               for val in json_dict["links"]]
            rx_bitrate_mbps = [float(val[0]) for val in rx_bitrate_mbps
                               if len(val) > 0]
            if len(rx_bitrate_mbps) > 0:
                links["link_mean_rx_bitrate_mbps"] = np.mean(rx_bitrate_mbps)
                links["link_max_rx_bitrate_mbps"] = np.max(rx_bitrate_mbps)
                links["link_min_rx_bitrate_mbps"] = np.min(rx_bitrate_mbps)
                links["link_median_rx_bitrate_mbps"] = np.median(
                    rx_bitrate_mbps)
    logging.debug("links: %s", links)

    # Print beacons
    for beacon in json_dict["beacons"]:
        primary_ch = int(beacon["channel"])
        primary_freq = int(float(
            beacon["freq"][:len(beacon["freq"]) - 4]) * 1000)
        outtemp = {
            "timestamp": timestamp,
            "mac": mac,
            "test_uuid": test_uuid,
            "corr_test": corr_test,
            "interface": interface,
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
            "available_admission_capacity_sec": "NaN",
            "link_mean_rssi_dbm": "NaN",
            "link_max_rssi_dbm": "NaN",
            "link_min_rssi_dbm": "NaN",
            "link_median_rssi_dbm": "NaN",
            "link_mean_tx_bitrate_mbps": "NaN",
            "link_max_tx_bitrate_mbps": "NaN",
            "link_min_tx_bitrate_mbps": "NaN",
            "link_median_tx_bitrate_mbps": "NaN",
            "link_mean_rx_bitrate_mbps": "NaN",
            "link_max_rx_bitrate_mbps": "NaN",
            "link_min_rx_bitrate_mbps": "NaN",
            "link_median_rx_bitrate_mbps": "NaN"
        }

        # Only update "link_*" attributes if the current beacon is connected.
        if "connected" in beacon and beacon["connected"] is True:
            for key in links:
                outtemp[key] = links[key]

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
                               "interface", "bssid", "ssid", "rssi_dbm",
                               "primary_channel_num", "primary_freq_mhz",
                               "channel_num", "center_freq0_mhz",
                               "center_freq1_mhz", "bw_mhz", "amendment",
                               "connected", "tx_power_dbm", "link_margin_db",
                               "sta_count", "ch_utilization",
                               "available_admission_capacity_sec",
                               "link_mean_rssi_dbm", "link_max_rssi_dbm",
                               "link_min_rssi_dbm", "link_median_rssi_dbm",
                               "link_mean_tx_bitrate_mbps",
                               "link_max_tx_bitrate_mbps",
                               "link_min_tx_bitrate_mbps",
                               "link_median_tx_bitrate_mbps",
                               "link_mean_rx_bitrate_mbps",
                               "link_max_rx_bitrate_mbps",
                               "link_min_rx_bitrate_mbps",
                               "link_median_rx_bitrate_mbps"]

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

    match args.mode:
        case "throughput":
            files = list(filter(lambda x: ("iperf-log" in str(x)
                                           or "speedtest-log" in str(x)),
                                files))
        case "latency":
            files = list(filter(lambda x: ("iperf-log" in str(x)
                                           or "ping-log" in str(x)
                                           or "speedtest-log" in str(x)),
                                files))
        case "wifi_scan":
            files = list(filter(lambda x: "wifi-scan" in str(x), files))
        case _:
            logging.warning("Unknown mode: %s", args.mode)

    logging.debug(files)

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
                curr_line = get_line(
                    args.mode,
                    file.parts[1],
                    json_dict)
                if len(curr_line) > 0 and (
                    (args.start_time is None)
                    or (args.start_time < datetime.fromisoformat(
                        curr_line[0]["timestamp"]))):
                    outarr += curr_line

    return outarr


def parse(list_args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["throughput", "latency", "wifi_scan"],
                        help="Choose type of CSV files.'")
    parser.add_argument("-J", "--json", action="store_true",
                        help="Output as JSON.")
    parser.add_argument("-m", "--mac", nargs='+',
                        help="Filter data to the speficied MAC address.")
    parser.add_argument("--start-time", nargs='?',
                        type=(lambda x: datetime.fromisoformat(x)),
                        help=("Filter data to the speficied start time (must "
                              "be in ISO format, ex: 2024-06-14T17:00-0500)"))
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
