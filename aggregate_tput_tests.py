import argparse
import pandas as pd
import numpy as np
import write_csv
from pathlib import Path
import logging
import sys


def dbm_to_mw(dbm):
    return 10 ** (dbm / 10)


def mw_to_dbm(mw):
    return 10 * np.log10(mw)


def create_csv(args):
    # Get tput data
    out_tput = pd.DataFrame.from_dict(write_csv.read_logs(write_csv.parse(
        ["throughput",
         "-d", str(args.log_dir),
         "-m", args.mac,
         "-l", args.log_level])))
    out_tput = out_tput[out_tput["test_uuid"] != "unknown"]
    out_tput = out_tput.rename(columns={"tput_mbps": "mean_tput_mbps"})
    out_tput["direction"] = out_tput["direction"].map(
        lambda x: "dl" if x == "downlink" else "ul")
    # Include type and direction on the uuid
    out_tput["test_uuid_concat"] = out_tput[[
        "test_uuid", "type", "direction"]].apply(lambda x: '-'.join(x), axis=1)
    out_tput["type_dir"] = out_tput[[
        "type", "direction"]].apply(lambda x: '-'.join(x), axis=1)
    logging.info(out_tput["test_uuid_concat"])

    # Pivot tput data based on the interface
    combined = out_tput.pivot(
        index="test_uuid_concat",
        columns="interface",
        values=["timestamp", "type_dir", "mean_tput_mbps", "std_tput_mbps",
                "min_tput_mbps", "max_tput_mbps", "median_tput_mbps"])
    combined.columns = ['_'.join(col) for col in combined.columns]
    # combined = combined[combined["mean_tput_mbps_wlan0"] > 0]
    combined["timestamp_eth0"] = combined["timestamp_eth0"].fillna(
        combined["timestamp_wlan0"])
    combined["type_dir_eth0"] = combined["type_dir_eth0"].fillna(
        combined["type_dir_wlan0"])
    combined = combined.drop(["type_dir_wlan0", "timestamp_wlan0"], axis=1)
    if "timestamp_wlan1" in combined:
        combined["timestamp_eth0"] = combined["timestamp_eth0"].fillna(
            combined["timestamp_wlan1"])
        combined["type_dir_eth0"] = combined["type_dir_eth0"].fillna(
            combined["type_dir_wlan1"])
        combined = combined.drop(["type_dir_wlan1", "timestamp_wlan1"], axis=1)
    combined = combined.rename(columns={"type_dir_eth0": "type",
                                        "timestamp_eth0": "timestamp"})
    logging.info(combined)

    # Get scan data
    raw_scan = write_csv.read_logs(write_csv.parse(
        ["wifi_scan",
         "-d", str(args.log_dir),
         "-m", args.mac,
         "-l", args.log_level]))
    raw_scan = [val for val in raw_scan if val["connected"] != "unknown"]
    out_scan = pd.DataFrame.from_dict(raw_scan)
    out_scan = out_scan[(out_scan["test_uuid"] != "unknown")
                        & (out_scan["interface"] != "unknown")
                        & (out_scan["corr_test"] != "idle")
                        & (out_scan["corr_test"] != "none")]
    out_scan = out_scan.rename(columns={"interface": "active_wlan"})

    # Get center freq and min-max freq
    out_scan["center_freq_mhz"] = out_scan["center_freq1_mhz"]
    out_scan.loc[out_scan["center_freq_mhz"] == 0,
                 "center_freq_mhz"] = out_scan["center_freq0_mhz"]
    out_scan["min_freq_mhz"] = out_scan["center_freq_mhz"] - (
        out_scan["bw_mhz"] / 2)
    out_scan["max_freq_mhz"] = out_scan["center_freq_mhz"] + (
        out_scan["bw_mhz"] / 2)
    logging.info(out_scan["max_freq_mhz"])

    # Separate iperf and speedtest
    out_scan_st_dl = out_scan[out_scan["corr_test"] == "speedtest"].copy()
    out_scan_st_dl["corr_test"] = "speedtest-dl"
    out_scan_st_ul = out_scan[out_scan["corr_test"] == "speedtest"].copy()
    out_scan_st_ul["corr_test"] = "speedtest-ul"
    out_scan_ip = out_scan[out_scan["corr_test"] != "speedtest"].copy()
    out_scan = pd.concat([out_scan_ip,
                          out_scan_st_dl,
                          out_scan_st_ul], ignore_index=True)
    out_scan["test_uuid_concat"] = out_scan[[
        "test_uuid", "corr_test"]].apply(lambda x: '-'.join(x), axis=1)
    out_scan = out_scan.set_index("test_uuid_concat")
    logging.info(out_scan.index)

    # Get the overlap of connected and neighboring APs
    out_scan_overlap = out_scan[~out_scan["connected"]].join(
        out_scan[out_scan["connected"]],
        on="test_uuid_concat",
        rsuffix='_conn')
    overlap_indices = ((out_scan_overlap["min_freq_mhz_conn"]
                        <= out_scan_overlap["max_freq_mhz"])
                       & (out_scan_overlap["min_freq_mhz"]
                          <= out_scan_overlap["max_freq_mhz_conn"]))
    logging.info(out_scan_overlap[overlap_indices])
    overlap_group = out_scan_overlap[overlap_indices].groupby(
        "test_uuid_concat")["rssi_dbm"]

    # Join tput with connected AP
    combined = combined.join(out_scan[out_scan["connected"]].filter(
        items=["active_wlan", "rssi_dbm", "primary_freq_mhz",
               "center_freq_mhz", "min_freq_mhz", "max_freq_mhz", "bw_mhz",
               "amendment", "tx_power_dbm", "link_margin_db", "sta_count",
               "ch_utilization", "available_admission_capacity_sec"]),
        on="test_uuid_concat")
    logging.info(combined)

    # Join with neighboring APs statistics
    combined = combined.join(overlap_group.count().rename("neighbor_count"),
                             on="test_uuid_concat")
    combined = combined.join(overlap_group.apply(
        lambda x: mw_to_dbm(dbm_to_mw(x).mean())).rename(
            "neighbor_mean_rssi_dbm"),
        on="test_uuid_concat")
    # combined = combined.join(overlap_group.apply(
    #     lambda x: mw_to_dbm(dbm_to_mw(x).std())).rename(
    #     "neighbor_std_rssi_dbm"),
    #     on="test_uuid_concat")
    combined = combined.join(overlap_group.median().rename(
        "neighbor_median_rssi_dbm"), on="test_uuid_concat")
    combined = combined.join(
        overlap_group.min().rename(
            "neighbor_min_rssi_dbm"),
        on="test_uuid_concat")
    combined = combined.join(
        overlap_group.max().rename(
            "neighbor_max_rssi_dbm"),
        on="test_uuid_concat")
    combined = combined.sort_values("timestamp")
    logging.info(combined)

    if args.json:
        combined.to_json(args.output_file, orient="split")
    else:
        combined.to_csv(args.output_file)


def parse(list_args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("mac", type=str,
                        help="Specify MAC address.")
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
    logging.basicConfig(level=args.log_level.upper())
    logging.info(f"MAC: {args.mac}")
    create_csv(args)
