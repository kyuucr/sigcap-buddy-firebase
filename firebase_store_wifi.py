from argparse import ArgumentParser
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from getpass import getpass
import json
import sys

cred = credentials.Certificate(
    "nd-schmidt-firebase-adminsdk-d1gei-43db929d8a.json")
firebase_admin.initialize_app(cred, {
    "databaseURL": "https://nd-schmidt-default-rtdb.firebaseio.com"
})


def main():
    parser = ArgumentParser(prog="firebase_store_wifi.py",
                            description="Store Wi-Fi info to Firebase DB")
    parser.add_argument("--delete", type=str, help="Specify RPI-ID to delete")
    parser.add_argument("--show", type=str, help="Specify RPI-ID to show")
    args = parser.parse_args()

    delete_rpi_id = args.delete
    show_rpi_id = args.show
    if (delete_rpi_id is not None):
        # Delete mode
        query = db.reference("wifi_v2").order_by_child("rpi_id").equal_to(
            delete_rpi_id).get()
        keys = list(query.keys())

        if (len(keys) > 0):
            db.reference("wifi_v2").child(keys[0]).delete()
        else:
            print(f"Cannot find Wi-Fi entry for {delete_rpi_id}!")
    elif (show_rpi_id is not None):
        # Show mode
        query = db.reference("wifi_v2").order_by_child("rpi_id").equal_to(
            show_rpi_id).get()
        values = list(query.values())

        if (len(values) > 0):
            wifi_entry = values[0]
            wifi_entry["pass"] = "<redacted>"
            print(json.dumps(wifi_entry, indent=2))
        else:
            print(f"Cannot find Wi-Fi entry for {show_rpi_id}!")
    else:
        # Add mode
        rpi_id = input("Input RPI-ID: ")
        if (rpi_id == ""):
            print("Cannot use empty RPI-ID!")
            sys.exit(1)
        ssid = input("Input SSID: ")
        passwd = getpass()

        wifi_entry = {
            "rpi_id": rpi_id, "ssid": ssid, "pass": passwd
        }
        query = db.reference("wifi_v2").order_by_child("rpi_id").equal_to(
            rpi_id).get()
        keys = list(query.keys())

        if (len(keys) > 0):
            db.reference("wifi_v2").child(keys[0]).update(wifi_entry)
        else:
            db.reference("wifi_v2").push().set(wifi_entry)
        print(f"Wi-Fi entry for {rpi_id} successfully added!")


if __name__ == "__main__":
    main()
