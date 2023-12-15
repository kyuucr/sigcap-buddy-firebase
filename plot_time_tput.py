import argparse
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import AutoLocator
import pandas as pd
from pathlib import Path

parser = argparse.ArgumentParser(
    prog="plot-time.py",
    description="Plot iperf and speedtest throughput results as time series")
parser.add_argument("input_csv")
parser.add_argument("-o", "--output-folder", help="Output folder.",
                    default=Path("."), type=Path)
parser.add_argument("--tz", help="Set timezone.", default="US/Central")
args = parser.parse_args()

matplotlib.rcParams["timezone"] = args.tz
output_path = args.output_folder
output_path.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(args.input_csv)
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp")
macs = df.mac.unique()

for mac in macs:
    df_iperf_eth = df[(df["type"] == "iperf")
                      & (df["mac"] == mac)
                      & (df["interface"] == "eth0")]
    df_iperf_wlan = df[(df["type"] == "iperf")
                       & (df["mac"] == mac)
                       & (df["interface"] == "wlan0")]
    df_speedtest_eth = df[(df["type"] == "speedtest")
                          & (df["mac"] == mac)
                          & (df["interface"] == "eth0")]
    df_speedtest_wlan = df[(df["type"] == "speedtest")
                           & (df["mac"] == mac)
                           & (df["interface"] == "wlan0")]

    # DL
    fig, ax = plt.subplots(figsize=(12, 4))

    if (df_iperf_eth[df_iperf_eth["direction"] == "downlink"].shape[0] > 0):
        ax.plot(
            df_iperf_eth[
                df_iperf_eth["direction"] == "downlink"]["timestamp"],
            df_iperf_eth[
                df_iperf_eth["direction"] == "downlink"]["tput_mbps"],
            "b-",
            label="iperf DL Eth",
            linewidth=2,
            markersize=12)
    if (df_iperf_wlan[df_iperf_wlan["direction"] == "downlink"].shape[0] > 0):
        ax.plot(
            df_iperf_wlan[
                df_iperf_wlan["direction"] == "downlink"]["timestamp"],
            df_iperf_wlan[
                df_iperf_wlan["direction"] == "downlink"]["tput_mbps"],
            "b--",
            label="iperf DL WLAN",
            linewidth=2,
            markersize=12)
    if (df_speedtest_eth[
       df_speedtest_eth["direction"] == "downlink"].shape[0] > 0):
        ax.plot(
            df_speedtest_eth[
                df_speedtest_eth["direction"] == "downlink"]["timestamp"],
            df_speedtest_eth[
                df_speedtest_eth["direction"] == "downlink"]["tput_mbps"],
            "g-",
            label="Ookla DL Eth",
            linewidth=2,
            markersize=12)
    if (df_speedtest_wlan[
       df_speedtest_wlan["direction"] == "downlink"].shape[0] > 0):
        ax.plot(
            df_speedtest_wlan[
                df_speedtest_wlan["direction"] == "downlink"]["timestamp"],
            df_speedtest_wlan[
                df_speedtest_wlan["direction"] == "downlink"]["tput_mbps"],
            "g--",
            label="Ookla DL WLAN",
            linewidth=2,
            markersize=12)

    ax.set_xlabel(r"$\mathbf{Time}$", fontsize=20)
    ax.set_ylabel(r"$\mathbf{Tput (Mbps)}$", fontsize=20)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%m-%d %H:%M"))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    ax.xaxis.set_minor_locator(mdates.HourLocator(interval=1))
    ax.yaxis.set_minor_locator(AutoLocator())
    ax.grid(True, linestyle="--", which="major", axis="both")
    ax.grid(True, linestyle=":", which="minor", axis="x")
    # plt.title(title)
    # plt.xlim(0, 110)
    # plt.ylim(0, 900)
    # plt.yticks(np.arange(0, 700, 200), fontsize=20, rotation=45)
    plt.xticks(rotation=30, ha="right")
    dx = 7 / 72
    dy = 0 / 72
    offset = matplotlib.transforms.ScaledTranslation(dx,
                                                     dy,
                                                     fig.dpi_scale_trans)
    for label in ax.xaxis.get_majorticklabels():
        label.set_transform(label.get_transform() + offset)
    plt.legend(fontsize=10, loc="lower right")
    plt.tight_layout()
    plt.savefig(output_path.joinpath("dl-tput-{}.pdf".format(mac)))
    plt.savefig(output_path.joinpath("dl-tput-{}.png".format(mac)))
    plt.close()

    # UL
    fig, ax = plt.subplots(figsize=(12, 4))

    if (df_iperf_eth[df_iperf_eth["direction"] == "uplink"].shape[0] > 0):
        ax.plot(
            df_iperf_eth[
                df_iperf_eth["direction"] == "uplink"]["timestamp"],
            df_iperf_eth[
                df_iperf_eth["direction"] == "uplink"]["tput_mbps"],
            "b-",
            label="iperf UL Eth",
            linewidth=2,
            markersize=12)
    if (df_iperf_wlan[df_iperf_wlan["direction"] == "uplink"].shape[0] > 0):
        ax.plot(
            df_iperf_wlan[
                df_iperf_wlan["direction"] == "uplink"]["timestamp"],
            df_iperf_wlan[
                df_iperf_wlan["direction"] == "uplink"]["tput_mbps"],
            "b--",
            label="iperf UL WLAN",
            linewidth=2,
            markersize=12)
    if (df_speedtest_eth[
       df_speedtest_eth["direction"] == "uplink"].shape[0] > 0):
        ax.plot(
            df_speedtest_eth[
                df_speedtest_eth["direction"] == "uplink"]["timestamp"],
            df_speedtest_eth[
                df_speedtest_eth["direction"] == "uplink"]["tput_mbps"],
            "g-",
            label="Ookla UL Eth",
            linewidth=2,
            markersize=12)
    if (df_speedtest_wlan[
       df_speedtest_wlan["direction"] == "uplink"].shape[0] > 0):
        ax.plot(
            df_speedtest_wlan[
                df_speedtest_wlan["direction"] == "uplink"]["timestamp"],
            df_speedtest_wlan[
                df_speedtest_wlan["direction"] == "uplink"]["tput_mbps"],
            "g--",
            label="Ookla UL WLAN",
            linewidth=2,
            markersize=12)

    ax.set_xlabel(r"$\mathbf{Time}$", fontsize=20)
    ax.set_ylabel(r"$\mathbf{Tput (Mbps)}$", fontsize=20)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%m-%d %H:%M"))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    ax.xaxis.set_minor_locator(mdates.HourLocator(interval=1))
    ax.yaxis.set_minor_locator(AutoLocator())
    ax.grid(True, linestyle="--", which="major", axis="both")
    ax.grid(True, linestyle=":", which="minor", axis="x")
    # plt.title(title)
    # plt.xlim(0, 110)
    # plt.ylim(0, 900)
    # plt.yticks(np.arange(0, 700, 200), fontsize=20, rotation=45)
    plt.xticks(rotation=30, ha="right")
    dx = 7 / 72
    dy = 0 / 72
    offset = matplotlib.transforms.ScaledTranslation(dx,
                                                     dy,
                                                     fig.dpi_scale_trans)
    for label in ax.xaxis.get_majorticklabels():
        label.set_transform(label.get_transform() + offset)
    plt.legend(fontsize=10, loc="lower right")
    plt.tight_layout()
    plt.savefig(output_path.joinpath("ul-tput-{}.pdf".format(mac)))
    plt.savefig(output_path.joinpath("ul-tput-{}.png".format(mac)))
    plt.close()
