import argparse
from datetime import datetime
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import logging
import sys

cred = credentials.Certificate(
    "nd-schmidt-firebase-adminsdk-d1gei-43db929d8a.json")
firebase_admin.initialize_app(cred, {
    "databaseURL": "https://nd-schmidt-default-rtdb.firebaseio.com"
})


def get_last_timestamp(heartbeats, mac):
    filtered = list(filter(
        lambda x: x["mac"].replace(":", "-") == mac,
        heartbeats))
    last_val = filtered[len(filtered) - 1]
    logging.debug(last_val)

    return last_val["last_timestamp"]


def list_devices(args):
    # Setup
    logging.basicConfig(level=args.log_level.upper())
    heartbeats = db.reference("heartbeat").order_by_child(
        "last_timestamp").get().values()

    macs = list(dict.fromkeys(
        [val["mac"].replace(":", "-") for val in heartbeats]))
    logging.debug(macs)

    outstring = "mac,online,last_timestamp\n"
    now_timestamp = datetime.timestamp(datetime.now()) * 1000

    for mac in macs:
        last_timestamp = get_last_timestamp(heartbeats, mac)
        outstring += "{},{},{}\n".format(
            mac,
            (now_timestamp - last_timestamp) < 3600000,  # 1 hour
            datetime.fromtimestamp(
                last_timestamp / 1000).astimezone().isoformat())

    args.output_file.write(outstring)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output-file", nargs='?',
                        type=argparse.FileType('w'), default=sys.stdout,
                        help="Output result to file, default is to stdout'")
    parser.add_argument("-l", "--log-level", default="warning",
                        help="Provide logging level, default is warning'")
    args = parser.parse_args()

    list_devices(args)
