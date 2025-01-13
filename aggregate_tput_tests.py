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


def write(df, args):
    if args.json:
        df.to_json(args.output_file, orient="split")
    else:
        df.to_csv(args.output_file)


def join_overlap(df_main, df_scan, col_prefix, overlap_fun):
    overlap_indices = overlap_fun(df_scan)
    logging.info(df_scan[overlap_indices])

    # Join neighboring APs' RSSI statistics
    overlap_group_rssi = df_scan[overlap_indices].groupby(
        "test_uuid_concat")["rssi_dbm"]
    df_main = df_main.join(overlap_group_rssi.count().rename(
        f"{col_prefix}_count"), on="test_uuid_concat")
    df_main = df_main.join(overlap_group_rssi.apply(
        lambda x: mw_to_dbm(dbm_to_mw(x).mean())).rename(
            f"{col_prefix}_mean_rssi_dbm"),
        on="test_uuid_concat")
    # df_main = df_main.join(overlap_group_rssi.apply(
    #     lambda x: mw_to_dbm(dbm_to_mw(x).std())).rename(
    #     f"{col_prefix}_std_rssi_dbm"),
    #     on="test_uuid_concat")
    df_main = df_main.join(overlap_group_rssi.median().rename(
        f"{col_prefix}_median_rssi_dbm"), on="test_uuid_concat")
    df_main = df_main.join(
        overlap_group_rssi.min().rename(
            f"{col_prefix}_min_rssi_dbm"),
        on="test_uuid_concat")
    df_main = df_main.join(
        overlap_group_rssi.max().rename(
            f"{col_prefix}_max_rssi_dbm"),
        on="test_uuid_concat")

    # Prepare group for optional beacon elements
    overlap_group = df_scan[overlap_indices][[
        "tx_power_dbm", "sta_count", "ch_utilization"]]
    overlap_group = overlap_group.astype(
        {"tx_power_dbm": float, "sta_count": float, "ch_utilization": float})

    # Join neighboring APs' TX power statistics
    overlap_group_tx_power = overlap_group[["tx_power_dbm"]].dropna().groupby(
        "test_uuid_concat")["tx_power_dbm"]
    df_main = df_main.join(overlap_group_tx_power.count().rename(
        f"{col_prefix}_num_bssid_with_power_elem"),
        on="test_uuid_concat")
    df_main = df_main.join(overlap_group_tx_power.mean().rename(
        f"{col_prefix}_mean_tx_power_dbm"),
        on="test_uuid_concat")
    # df_main = df_main.join(overlap_group_tx_power.std().rename(
    #     f"{col_prefix}_std_tx_power_dbm"),
    #     on="test_uuid_concat")
    df_main = df_main.join(overlap_group_tx_power.median().rename(
        f"{col_prefix}_median_tx_power_dbm"), on="test_uuid_concat")
    df_main = df_main.join(
        overlap_group_tx_power.min().rename(
            f"{col_prefix}_min_tx_power_dbm"),
        on="test_uuid_concat")
    df_main = df_main.join(
        overlap_group_tx_power.max().rename(
            f"{col_prefix}_max_tx_power_dbm"),
        on="test_uuid_concat")

    # Join neighboring APs' CH util statistics
    overlap_group_ch_util = overlap_group[["ch_utilization"]].dropna().groupby(
        "test_uuid_concat")["ch_utilization"]
    df_main = df_main.join(overlap_group_ch_util.count().rename(
        f"{col_prefix}_num_bssid_with_load_elem"),
        on="test_uuid_concat")
    df_main = df_main.join(overlap_group_ch_util.mean().rename(
        f"{col_prefix}_mean_ch_utilization"),
        on="test_uuid_concat")
    # df_main = df_main.join(overlap_group_ch_util.std().rename(
    #     f"{col_prefix}_std_ch_utilization"),
    #     on="test_uuid_concat")
    df_main = df_main.join(overlap_group_ch_util.median().rename(
        f"{col_prefix}_median_ch_utilization"), on="test_uuid_concat")
    df_main = df_main.join(
        overlap_group_ch_util.min().rename(
            f"{col_prefix}_min_ch_utilization"),
        on="test_uuid_concat")
    df_main = df_main.join(
        overlap_group_ch_util.max().rename(
            f"{col_prefix}_max_ch_utilization"),
        on="test_uuid_concat")

    # Join neighboring APs' STA count statistics
    overlap_group_sta_count = overlap_group[["sta_count"]].dropna().groupby(
        "test_uuid_concat")["sta_count"]
    df_main = df_main.join(overlap_group_sta_count.mean().rename(
        f"{col_prefix}_mean_sta_count"),
        on="test_uuid_concat")
    # df_main = df_main.join(overlap_group_sta_count.std().rename(
    #     f"{col_prefix}_std_sta_count"),
    #     on="test_uuid_concat")
    df_main = df_main.join(overlap_group_sta_count.median().rename(
        f"{col_prefix}_median_sta_count"), on="test_uuid_concat")
    df_main = df_main.join(
        overlap_group_sta_count.min().rename(
            f"{col_prefix}_min_sta_count"),
        on="test_uuid_concat")
    df_main = df_main.join(
        overlap_group_sta_count.max().rename(
            f"{col_prefix}_max_sta_count"),
        on="test_uuid_concat")

    return df_main


def create_tput_df(tput_dict):
    # Tput DF
    out_tput = pd.DataFrame.from_dict(tput_dict)
    out_tput = out_tput[out_tput["test_uuid"] != "unknown"]
    if (out_tput.shape[0] == 0):
        logging.warning("Tput table empty after filtering!")
        return

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
        values=["timestamp", "type_dir", "host", "mean_tput_mbps",
                "std_tput_mbps", "min_tput_mbps", "max_tput_mbps",
                "median_tput_mbps"])
    combined.columns = ['_'.join(col) for col in combined.columns]
    # combined = combined[combined["mean_tput_mbps_wlan0"] > 0]
    combined.insert(0, "timestamp", None)
    combined.insert(1, "type", None)
    if "timestamp_eth0" in combined:
        combined["timestamp"] = combined["timestamp"].fillna(
            combined["timestamp_eth0"])
        combined["type"] = combined["type"].fillna(
            combined["type_dir_eth0"])
        combined = combined.drop(["type_dir_eth0", "timestamp_eth0"], axis=1)
    if "timestamp_wlan0" in combined:
        combined["timestamp"] = combined["timestamp"].fillna(
            combined["timestamp_wlan0"])
        combined["type"] = combined["type"].fillna(
            combined["type_dir_wlan0"])
        combined = combined.drop(["type_dir_wlan0", "timestamp_wlan0"], axis=1)
    if "timestamp_wlan1" in combined:
        combined["timestamp"] = combined["timestamp"].fillna(
            combined["timestamp_wlan1"])
        combined["type"] = combined["type"].fillna(
            combined["type_dir_wlan1"])
        combined = combined.drop(["type_dir_wlan1", "timestamp_wlan1"], axis=1)
    logging.info(combined)

    return combined


def create_csv(tput_dict, wifi_scan_dict):
    combined = create_tput_df(tput_dict)
    if combined is None:
        return

    raw_scan = [val for val in wifi_scan_dict if val["connected"] != "unknown"]
    out_scan = pd.DataFrame.from_dict(raw_scan)
    if ("test_uuid" not in out_scan.columns
            or "interface" not in out_scan.columns
            or "corr_test" not in out_scan.columns):
        return
    out_scan = out_scan[(out_scan["test_uuid"] != "unknown")
                        & (out_scan["interface"] != "unknown")
                        & (out_scan["corr_test"] != "idle")
                        & (out_scan["corr_test"] != "none")]
    out_scan = out_scan.rename(columns={"interface": "active_wlan"})
    logging.info(out_scan)

    if (out_scan.shape[0] > 0):
        # Get center freq and min-max freq
        out_scan["center_freq_mhz"] = out_scan["center_freq1_mhz"]
        out_scan.loc[out_scan["center_freq_mhz"] == 0,
                     "center_freq_mhz"] = out_scan["center_freq0_mhz"]
        out_scan["min_freq_mhz"] = out_scan["center_freq_mhz"] - (
            out_scan["bw_mhz"] / 2)
        out_scan["max_freq_mhz"] = out_scan["center_freq_mhz"] + (
            out_scan["bw_mhz"] / 2)
        out_scan["primary_min_freq_mhz"] = out_scan["primary_freq_mhz"] - 10
        out_scan["primary_max_freq_mhz"] = out_scan["primary_freq_mhz"] + 10

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

        # Join tput with connected AP
        combined = combined.join(out_scan[out_scan["connected"]].filter(
            items=["active_wlan", "rssi_dbm", "primary_freq_mhz",
                   "center_freq_mhz", "min_freq_mhz", "max_freq_mhz", "bw_mhz",
                   "amendment", "tx_power_dbm", "link_margin_db", "sta_count",
                   "ch_utilization", "available_admission_capacity_sec",
                   "link_mean_rssi_dbm", "link_max_rssi_dbm",
                   "link_min_rssi_dbm", "link_median_rssi_dbm",
                   "link_mean_tx_bitrate_mbps", "link_max_tx_bitrate_mbps",
                   "link_min_tx_bitrate_mbps", "link_median_tx_bitrate_mbps",
                   "link_mean_rx_bitrate_mbps", "link_max_rx_bitrate_mbps",
                   "link_min_rx_bitrate_mbps", "link_median_rx_bitrate_mbps"]),
            on="test_uuid_concat")
        logging.info(combined)

        # Get the overlap of connected and neighboring APs
        out_scan_overlap = out_scan[~out_scan["connected"]].join(
            out_scan[out_scan["connected"]],
            on="test_uuid_concat",
            rsuffix='_conn')

        combined = join_overlap(
            combined, out_scan_overlap, "overlap_full",
            lambda x: ((x["min_freq_mhz_conn"]
                        <= x["max_freq_mhz"])
                       & (x["min_freq_mhz"]
                          <= x["max_freq_mhz_conn"])))

        combined = join_overlap(
            combined, out_scan_overlap, "overlap_primary",
            lambda x: ((x["primary_min_freq_mhz_conn"]
                        <= x["primary_max_freq_mhz"])
                       & (x["primary_min_freq_mhz"]
                          <= x["primary_max_freq_mhz_conn"])))

    combined = combined.sort_values("timestamp")
    logging.info(combined)

    return combined


def agg_tput(args):
    # Get tput data
    params = ["throughput",
              "-d", str(args.log_dir),
              "-m", args.mac,
              "-l", args.log_level]
    if args.start_time:
        params += ["--start-time", args.start_time]
    raw_tput = write_csv.read_logs(write_csv.parse(params))
    if len(raw_tput) == 0:
        logging.warning("No throughput logs! Exiting...")
        return

    # Get scan data
    params = ["wifi_scan",
              "-d", str(args.log_dir),
              "-m", args.mac,
              "-l", args.log_level]
    if args.start_time:
        params += ["--start-time", args.start_time]
    if args.end_time:
        params += ["--end-time", args.end_time]
    raw_scan = write_csv.read_logs(write_csv.parse(params))

    out_df = create_csv(raw_tput, raw_scan)
    if (out_df is not None):
        write(out_df, args)


def parse(list_args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("mac", type=str,
                        help="Specify MAC address.")
    parser.add_argument("--start-time", nargs='?',
                        help=("Filter data to the speficied start time (must "
                              "be in ISO format, ex: 2024-06-14T17:00-0500)"))
    parser.add_argument("--end-time", nargs='?',
                        help=("Filter data to the speficied end time (must "
                              "be in ISO format, ex: 2024-06-14T17:00-0500)"))
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
    agg_tput(args)
