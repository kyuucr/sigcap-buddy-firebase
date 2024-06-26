import aggregate_tput_tests
import argparse
import pandas as pd
import numpy as np
import write_csv
from pathlib import Path
import logging
import sys


def write(df, args, as_records=False):
    if args.json:
        if as_records:
            df = df.reset_index()
            df.to_json(args.output_file, orient="records")
        else:
            df.to_json(args.output_file, orient="split")
    else:
        df.to_csv(args.output_file)


def create_csv(agg_tput_df, by_direction=False):
    agg_tput_df["timestamp"] = pd.to_datetime(
        agg_tput_df["timestamp"], utc=True)
    agg_tput_df["timestamp_hour"] = agg_tput_df["timestamp"].apply(
        lambda x: x.hour)
    agg_tput_df["timestamp_date"] = agg_tput_df["timestamp"].apply(
        lambda x: str(x.date()))
    agg_tput_df["timestamp_dow"] = agg_tput_df["timestamp"].apply(
        lambda x: x.weekday())
    if by_direction:
        agg_tput_df["direction"] = agg_tput_df["type"].apply(
            lambda x: x.split("-")[1])

    # Determine available interfaces
    has_eth0 = "max_tput_mbps_eth0" in agg_tput_df.columns
    has_wlan0 = "max_tput_mbps_wlan0" in agg_tput_df.columns
    has_wlan1 = "max_tput_mbps_wlan1" in agg_tput_df.columns

    # Group by test type {iperf,speedtest}-{dl,ul}
    # or by direction {dl,ul}
    if by_direction:
        focus_cols = ["direction"]
        focus_col = "direction"
    else:
        focus_cols = ["type"]
        focus_col = "type"
    if has_eth0:
        focus_cols += ["max_tput_mbps_eth0"]
    if has_wlan0:
        focus_cols += ["max_tput_mbps_wlan0"]
    if has_wlan1:
        focus_cols += ["max_tput_mbps_wlan1"]
    if by_direction:
        all_tput = agg_tput_df[focus_cols].groupby(focus_col)
    else:
        all_tput = agg_tput_df[focus_cols].groupby("type")
    # Group by test type/direction and timestamp hour
    hourly_tput = agg_tput_df[focus_cols + ["timestamp_hour"]].groupby(
        [focus_col, "timestamp_hour"])

    # DFs
    all_tput_mean = all_tput.mean()
    all_tput_count_day = all_tput.count().apply(lambda x: x / 24)
    hourly_tput_stddev = hourly_tput.mean().groupby(focus_col).std()
    hourly_count_stddev = hourly_tput.count().groupby(focus_col).std()
    if has_eth0:
        all_tput_mean = all_tput_mean.rename(columns={
            "max_tput_mbps_eth0": "mean_tput_mbps_eth0"})
        all_tput_count_day = all_tput_count_day.rename(columns={
            "max_tput_mbps_eth0": "day_worth_of_data_eth0"})
        hourly_tput_stddev = hourly_tput_stddev.rename(columns={
            "max_tput_mbps_eth0": "hourly_tput_stddev_eth0"})
        hourly_count_stddev = hourly_count_stddev.rename(columns={
            "max_tput_mbps_eth0": "hourly_count_stddev_eth0"})
    if has_wlan0:
        all_tput_mean = all_tput_mean.rename(columns={
            "max_tput_mbps_wlan0": "mean_tput_mbps_wlan0"})
        all_tput_count_day = all_tput_count_day.rename(columns={
            "max_tput_mbps_wlan0": "day_worth_of_data_wlan0"})
        hourly_tput_stddev = hourly_tput_stddev.rename(columns={
            "max_tput_mbps_wlan0": "hourly_tput_stddev_wlan0"})
        hourly_count_stddev = hourly_count_stddev.rename(columns={
            "max_tput_mbps_wlan0": "hourly_count_stddev_wlan0"})
    if has_wlan1:
        all_tput_mean = all_tput_mean.rename(columns={
            "max_tput_mbps_wlan1": "mean_tput_mbps_wlan1"})
        all_tput_count_day = all_tput_count_day.rename(columns={
            "max_tput_mbps_wlan1": "day_worth_of_data_wlan1"})
        hourly_tput_stddev = hourly_tput_stddev.rename(columns={
            "max_tput_mbps_wlan1": "hourly_tput_stddev_wlan1"})
        hourly_count_stddev = hourly_count_stddev.rename(columns={
            "max_tput_mbps_wlan1": "hourly_count_stddev_wlan1"})

    # Group by timestamp day of the week
    cons_week = agg_tput_df[
        [focus_col, "timestamp_date", "timestamp_dow"]].groupby(
        [focus_col, "timestamp_dow"]).apply(
        lambda x: x.nunique())
    available_dow = agg_tput_df["timestamp_dow"].unique()
    for i in range(0, 7):
        if i not in available_dow:
            for focus_idx in agg_tput_df[focus_col].unique():
                cons_week.loc[(focus_idx, i), :] = 0
    logging.info(cons_week)
    cons_week = cons_week["timestamp_date"].groupby(focus_col).min()

    out_df = pd.concat(
        [all_tput_mean, all_tput_count_day, hourly_tput_stddev,
         hourly_count_stddev], axis=1)
    out_df["total_day"] = 0
    out_df["total_consecutive_week"] = 0
    for idx in out_df.index:
        out_df.loc[idx, "total_day"] = len(
            agg_tput_df[agg_tput_df[focus_col] == idx][
                "timestamp_date"].unique())
        out_df.loc[idx, "total_consecutive_week"] = cons_week[idx]
    return out_df


def combine_device_list(device_list_df, agg_tput_dfs, remove_empty=False):
    if isinstance(device_list_df, list):
        device_list_df = pd.DataFrame.from_dict(device_list_df)
    device_list_df = device_list_df.set_index("mac")
    col_index = device_list_df.shape[1] - 1
    col_to_add = ["mean_dl_tput_mbps_eth", "mean_dl_tput_mbps_wlan",
                  "mean_ul_tput_mbps_eth", "mean_ul_tput_mbps_wlan",
                  "total_day", "total_consecutive_week"]
    for i in range(len(col_to_add)):
        device_list_df.insert(col_index + i, col_to_add[i], None)

    for mac in device_list_df.index:
        if mac in agg_tput_dfs:
            out_df = create_csv(agg_tput_dfs[mac], by_direction=True)
            device_list_df.loc[mac, "total_day"] = max(
                out_df.loc["dl", "total_day"],
                out_df.loc["ul", "total_day"])
            device_list_df.loc[mac, "total_consecutive_week"] = max(
                out_df.loc["dl", "total_consecutive_week"],
                out_df.loc["ul", "total_consecutive_week"])
            if "mean_tput_mbps_eth0" in out_df.columns:
                device_list_df.loc[mac, [
                    "mean_dl_tput_mbps_eth",
                    "mean_ul_tput_mbps_eth"]] = (
                        out_df.loc["dl", "mean_tput_mbps_eth0"],
                        out_df.loc["ul", "mean_tput_mbps_eth0"])
            if "mean_tput_mbps_wlan1" in out_df.columns:
                device_list_df.loc[mac, [
                    "mean_dl_tput_mbps_wlan",
                    "mean_ul_tput_mbps_wlan"]] = (
                        out_df.loc["dl", "mean_tput_mbps_wlan1"],
                        out_df.loc["ul", "mean_tput_mbps_wlan1"])
            elif "mean_tput_mbps_wlan0" in out_df.columns:
                device_list_df.loc[mac, [
                    "mean_dl_tput_mbps_wlan",
                    "mean_ul_tput_mbps_wlan"]] = (
                        out_df.loc["dl", "mean_tput_mbps_wlan0"],
                        out_df.loc["ul", "mean_tput_mbps_wlan0"])
        elif remove_empty:
            # Remove row with non-existent tput data
            device_list_df = device_list_df[device_list_df.index != mac]

    return device_list_df


def data_stats(args):
    if args.device_list:
        device_list_df = pd.read_csv(args.device_list)
        agg_tput_dfs = dict()

        for mac in device_list_df["mac"].unique():
            # Get tput data
            raw_tput = write_csv.read_logs(write_csv.parse(
                ["throughput",
                 "-d", str(args.log_dir),
                 "-m", mac,
                 "-l", args.log_level]))
            if len(raw_tput) == 0:
                logging.warning("No throughput logs! Exiting...")
                continue

            agg_tput_df = aggregate_tput_tests.create_tput_df(raw_tput)
            if agg_tput_df is None:
                logging.warning("No aggregate throughput data! Exiting...")
                continue
            agg_tput_dfs[mac] = agg_tput_df

        write(combine_device_list(device_list_df, agg_tput_dfs), args)

    else:
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

        agg_tput_df = aggregate_tput_tests.create_tput_df(raw_tput)
        if agg_tput_df is None:
            logging.warning("No aggregate througput data! Exiting...")
            return

        out_df = create_csv(agg_tput_df, by_direction=args.by_direction)
        if (out_df is not None):
            write(out_df, args)


def parse(list_args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("mac", type=str,
                        help="Specify MAC address.")
    parser.add_argument("--start-time", nargs='?',
                        help=("Filter data to the speficied start time (must "
                              "be in ISO format, ex: 2024-06-14T17:00-0500)"))
    parser.add_argument("-J", "--json", action="store_true",
                        help="Output as JSON.")
    parser.add_argument("--by-direction", action="store_true",
                        help=("Use only test direction (DL/UL) instead of "
                              "test type + direction."))
    parser.add_argument("-o", "--output-file", nargs='?',
                        type=argparse.FileType('w'), default=sys.stdout,
                        help="Output result to file, default is to stdout'")
    parser.add_argument("--device-list", type=argparse.FileType('r'),
                        help="Device list CSV to combine.'")
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
    data_stats(args)
