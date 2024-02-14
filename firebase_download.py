import argparse
from base64 import b64decode
from crc32c import crc32c
import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
from google.cloud.storage import transfer_manager
import logging
from pathlib import Path
import struct


def compare_file_crc32c(path, comp_crc32c):
    with open(path, "rb") as fd:
        file_crc = crc32c(fd.read())
        to_compare = struct.unpack('>I', b64decode(comp_crc32c))[0]
        return file_crc == to_compare


def download(args):
    # Setup
    logging.basicConfig(level=args.log_level.upper())
    logdir = args.log_dir
    logging.info("logdir=%s", logdir)

    bucket = storage.bucket()
    logging.info("Got bucket: %s", bucket.name)

    # List files to download
    all_files = [{
        "name": file.name,
        "crc32c": file.crc32c
    } for file in bucket.list_blobs()]
    to_download = list(filter(
        lambda x: not logdir.joinpath(x["name"]).exists()
        or (x["name"].endswith("speedtest_logger.log")
            and not compare_file_crc32c(
                logdir.joinpath(x["name"]), x["crc32c"])),
        all_files))
    print("Found %d files to download" % len(to_download))
    logging.info(to_download)

    results = transfer_manager.download_many_to_path(
        bucket, [val["name"] for val in to_download],
        destination_directory=str(logdir),
        create_directories=True, max_workers=8
    )

    for file, result in zip(to_download, results):
        # The results list is either `None` or an exception for each filename
        # in the input list, in order.

        if isinstance(result, Exception):
            logging.warning("Failed to download %s due to exception: %s",
                            file, result)
        else:
            print(f"Downloaded {file['name']} crc32c {file['crc32c']}.")


def parse(list_args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--log-dir", type=Path, default=Path("./logs"),
                        help="Specify local log directory, default='./logs'")
    parser.add_argument("-l", "--log-level", default="warning",
                        help="Provide logging level, default is warning'")
    if (list_args is None):
        return parser.parse_args()
    else:
        return parser.parse_args(args=list_args)


if __name__ == '__main__':
    cred = credentials.Certificate(
        "nd-schmidt-firebase-adminsdk-d1gei-43db929d8a.json")
    firebase_admin.initialize_app(cred, {
        "storageBucket": "nd-schmidt.appspot.com"
    })
    download(parse())
