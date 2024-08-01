from argparse import ArgumentParser
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from uuid import getnode as get_mac
import json

cred = credentials.Certificate(
    "nd-schmidt-firebase-adminsdk-d1gei-43db929d8a.json")
firebase_admin.initialize_app(cred, {
    "databaseURL": "https://nd-schmidt-default-rtdb.firebaseio.com"
})

default_config = {
    "monitor_interface": "",
    "wireless_interface": "wlan0",
    "speedtest_interval": 60,
    "upload_interval": 0,
    "iperf_server": "ns-mn1.cse.nd.edu",
    "iperf_minport": 5201,
    "iperf_maxport": 5220,
    "iperf_duration": 5,
    "timeout_s": 120,
    "data_cap_gbytes": 100,
    "sampling_threshold": 0.25,
    "broker_addr": "ns-mn1.cse.nd.edu",
    "broker_port": 1883,
    "publish_interval": 600
}

helper_dict = {
    "monitor_interface": "WLAN interface for monitor mode",
    "wireless_interface": "WLAN interface for data transmission",
    "speedtest_interval": "Speedtest interval in minutes",
    "upload_interval": ("Upload interval in minutes, set to 0 to "
                        "upload right after the tests"),
    "iperf_server": "iperf target server",
    "iperf_minport": "Minimum port for iperf",
    "iperf_maxport": "Maximum port for iperf",
    "iperf_duration": "iperf duration in seconds",
    "timeout_s": "Command timeout in seconds",
    "data_cap_gbytes": "Data cap in GBytes",
    "sampling_threshold": "Testing samping threshold [0, 1]",
    "broker_addr": "MQTT broker address",
    "broker_port": "MQTT broker port",
    "publish_interval": "MQTT status publish interval in seconds"
}


def main():
    def_mac = ":".join(("%012X" % get_mac())[i:i + 2] for i in range(0, 12, 2))
    parser = ArgumentParser(
        prog="firebase_store_config.py",
        description="Store sigcap-buddy configuration to Firebase DB")
    parser.add_argument("--delete", type=str, help="Specify MAC ID to delete")
    parser.add_argument("--show", type=str, help="Specify MAC ID to show")
    args = parser.parse_args()

    delete_mac = args.delete
    show_mac = args.show
    if (delete_mac is not None):
        # Delete mode
        delete_mac = delete_mac.replace("-", ":")
        query = db.reference("config").order_by_child("mac").equal_to(
            delete_mac).get()
        keys = list(query.keys())

        if (len(keys) > 0):
            db.reference("config").child(keys[0]).delete()
        else:
            print(f"Cannot find Wi-Fi entry for MAC {delete_mac}!")
    elif (show_mac is not None):
        # Show mode
        show_mac = show_mac.replace("-", ":")
        query = db.reference("config").order_by_child("mac").equal_to(
            show_mac).get()
        values = list(query.values())

        if (len(values) > 0):
            print(json.dumps(values[0], indent=2))
        else:
            print(f"Cannot find Wi-Fi entry for MAC {show_mac}!")
    else:
        # Add mode
        mac = input("Input MAC [{}]: ".format(def_mac)).replace("-", ":")
        if (mac == ""):
            mac = def_mac

        config = default_config.copy()
        query = db.reference("config").order_by_child("mac").equal_to(
            mac).get()
        values = list(query.values())
        keys = list(query.keys())
        if (len(values) > 0):
            val = values[0]
            for key in val:
                if (key != "mac" and val[key]):
                    config[key] = val[key]

        for key in config:
            temp = input(helper_dict[key] + f" [{config[key]}]: ")
            if (temp):
                if (isinstance(default_config[key], int)):
                    temp = int(temp)
                elif (isinstance(default_config[key], float)):
                    temp = float(temp)
                config[key] = temp

        if (len(keys) > 0):
            db.reference("config").child(keys[0]).update(config)
        else:
            config["mac"] = mac
            db.reference("config").push().set(config)
        print(f"Config for MAC {mac} successfully added!")


if __name__ == "__main__":
    main()
