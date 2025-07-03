from argparse import ArgumentParser
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from uuid import getnode as get_mac
import json
import sys

cred = credentials.Certificate(
    "nd-schmidt-firebase-adminsdk-d1gei-43db929d8a.json")
firebase_admin.initialize_app(cred, {
    "databaseURL": "https://nd-schmidt-default-rtdb.firebaseio.com"
})

default_config = {
    "rpi_id": "",
    "monitor_interface": "",
    "wireless_interface": "wlan0",
    "speedtest_interval": 60,
    "upload_interval": 0,
    "iperf_ping_enabled": True,
    "ookla_enabled": True,
    "iperf_server": "ns-mn1.cse.nd.edu",
    "iperf_minport": 5201,
    "iperf_maxport": 5220,
    "iperf_duration": 5,
    "timeout_s": 120,
    "data_cap_gbytes": 100,
    "sampling_threshold": 1,
    "active_tests_sampling_threshold": 0.25,
    "broker_addr": "ns-mn1.cse.nd.edu",
    "broker_port": 1883,
    "publish_interval": 600
}

helper_dict = {
    "rpi_id": "ID assigned to the RPI (ex: RPI-01)",
    "monitor_interface": "WLAN interface for monitor mode",
    "wireless_interface": "WLAN interface for data transmission",
    "speedtest_interval": "Speedtest interval in minutes",
    "upload_interval": ("Upload interval in minutes, set to 0 to "
                        "upload right after the tests"),
    "iperf_ping_enabled": "Enable iPerf and ping measurements?",
    "ookla_enabled": "Enable Ookla measurements?",
    "iperf_server": "iperf target server",
    "iperf_minport": "Minimum port for iperf",
    "iperf_maxport": "Maximum port for iperf",
    "iperf_duration": "iperf duration in seconds",
    "timeout_s": "Command timeout in seconds",
    "data_cap_gbytes": "Data cap in GBytes",
    "sampling_threshold": "Testing sampling threshold [0.0, 1.0]",
    "active_tests_sampling_threshold": ("Active test sampling threshold "
                                        "[0.0, 1.0]"),
    "broker_addr": "MQTT broker address",
    "broker_port": "MQTT broker port",
    "publish_interval": "MQTT status publish interval in seconds"
}



def retrieve_config(input_key, type):
    """
    Retrieve config from Firebase DB.

    Args:
        input_key (string): Either MAC or RPI-ID.
        type (string): Type of input key, either "mac" or "rpi_id".

    Returns:
        tuple:
            - config (dict): Config dictionary.
            - db_key (string): Firebase DB key for editing or deleting.
            - mac (string): MAC associated with the config.
    """
    mac = None
    config = default_config.copy()
    query = db.reference("config").order_by_child(type).equal_to(
        input_key).get()
    values = list(query.values())
    db_keys = list(query.keys())
    if (len(values) > 0 and len(db_keys) > 0):
        val = values[0]
        for key in val:
            if (key != "mac"):
                config[key] = val[key]
            else:
                mac = val[key]
        return config, db_keys[0], mac
    else:
        return None, None, None


def main():
    def_mac = ":".join(("%012X" % get_mac())[i:i + 2] for i in range(0, 12, 2))
    parser = ArgumentParser(
        prog="firebase_store_config.py",
        description="Store sigcap-buddy configuration to Firebase DB")
    parser.add_argument("mac", help="input MAC")
    parser.add_argument("--show", action="store_true",
                        help="show configuration for associated MAC/RPI-ID")
    parser.add_argument("--delete", action="store_true",
                        help="delete configuration for associated MAC/RPI-ID")
    parser.add_argument("--rpi-id", action="store_true",
                        help="specify the mac input as RPI-ID instead of MAC")
    args = parser.parse_args()

    config = None
    db_key = None
    mac = None

    # Retrieve MAC if RPI-ID is specified
    if (args.rpi_id):
        config, db_key, mac = retrieve_config(args.mac, "rpi_id")
        if (mac is not None):
            print(f"Found MAC {mac} for {args.mac} !")
        else:
            # Exit if RPI-ID is not found
            print("RPI-ID is not found!")
            sys.exit(1)
    else:
        mac = args.mac.replace("-", ":").upper()
        config, db_key, _ = retrieve_config(args.mac, "mac")

    if (args.show):
        # Show mode
        if (config is not None):
            print(json.dumps(config, indent=2))
        else:
            print(f"Cannot find config entry for MAC {mac}!")
    elif (args.delete):
        # Delete mode
        if (db_key is not None):
            db.reference("config").child(db_key).delete()
            print(f"Config for MAC {mac} successfully deleted!")
        else:
            print(f"Cannot find config entry for MAC {mac}!")
    else:
        # Add/edit mode
        if (config is None):
            config = default_config.copy()
        for key in config:
            temp = input(helper_dict[key] + f" [{config[key]}]: ")
            if (temp):
                if ("enabled" in key):
                    temp = (temp == "true" or temp == "True")
                elif (isinstance(default_config[key], int)):
                    temp = int(temp)
                elif (isinstance(default_config[key], float)):
                    temp = float(temp)
                config[key] = temp
            elif (key == "rpi_id" and config[key] == ""):
                print("RPI-ID cannot be empty!")
                sys.exit(1)

        if (db_key is not None):
            db.reference("config").child(db_key).update(config)
        else:
            config["mac"] = mac
            db.reference("config").push().set(config)
        print(f"Config for MAC {mac} successfully added!")


if __name__ == "__main__":
    main()
