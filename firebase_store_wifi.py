from argparse import ArgumentParser
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from getpass import getpass
from uuid import getnode as get_mac
import json

cred = credentials.Certificate(
    "nd-schmidt-firebase-adminsdk-d1gei-43db929d8a.json")
firebase_admin.initialize_app(cred, {
    "databaseURL": "https://nd-schmidt-default-rtdb.firebaseio.com"
})


def main():
    def_mac = ":".join(("%012X" % get_mac())[i:i + 2] for i in range(0, 12, 2))
    parser = ArgumentParser(prog="firebase_store_wifi.py",
                            description="Store Wi-Fi info to Firebase DB")
    parser.add_argument("--delete", type=str, help="Specify MAC ID to delete")
    parser.add_argument("--show", type=str, help="Specify MAC ID to show")
    args = parser.parse_args()

    delete_mac = args.delete
    show_mac = args.show
    if (delete_mac is not None):
        # Delete mode
        delete_mac = delete_mac.replace("-", ":")
        query = db.reference("wifi").order_by_child("mac").equal_to(
            delete_mac).get()
        keys = list(query.keys())

        if (len(keys) > 0):
            db.reference("wifi").child(keys[0]).delete()
        else:
            print(f"Cannot find Wi-Fi entry for MAC {delete_mac}!")
    elif (show_mac is not None):
        # Show mode
        show_mac = show_mac.replace("-", ":")
        query = db.reference("wifi").order_by_child("mac").equal_to(
            show_mac).get()
        values = list(query.values())

        if (len(values) > 0):
            wifi_entry = values[0]
            wifi_entry["pass"] = "<redacted>"
            print(json.dumps(wifi_entry, indent=2))
        else:
            print(f"Cannot find Wi-Fi entry for MAC {show_mac}!")
    else:
        # Add mode
        mac = input("Input MAC [{}]: ".format(def_mac)).replace("-", ":")
        if (mac == ""):
            mac = def_mac
        ssid = input("Input SSID: ")
        passwd = getpass()

        wifi_entry = {"mac": mac, "ssid": ssid, "pass": passwd}
        query = db.reference("wifi").order_by_child("mac").equal_to(mac).get()
        keys = list(query.keys())

        if (len(keys) > 0):
            db.reference("wifi").child(keys[0]).update(wifi_entry)
        else:
            db.reference("wifi").push().set(wifi_entry)
        print(f"Wi-Fi entry for MAC {mac} successfully added!")


if __name__ == "__main__":
    main()
