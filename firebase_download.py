import argparse
from base64 import b64decode
from crc32c import crc32c
from datetime import datetime, timedelta, timezone
import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
from google.cloud.storage import transfer_manager
import logging
from pathlib import Path
from shutil import which
import struct
import subprocess


def compare_file_crc32c(path, comp_crc32c):
    with open(path, "rb") as fd:
        file_crc = crc32c(fd.read())
        to_compare = struct.unpack('>I', b64decode(comp_crc32c))[0]
        return file_crc == to_compare


def download_firebase(args):
    # Setup
    logging.basicConfig(level=args.log_level.upper())
    logdir = args.log_dir
    logging.info("logdir=%s", logdir)

    bucket = storage.bucket()
    logging.info("Got bucket: %s", bucket.name)

    # List files to download
    all_files = list()
    if args.start_date:
        # List files from start_date, if it is set
        start_time = args.start_date
        # Make start_time offset-aware
        if (start_time.tzinfo is None
                or start_time.tzinfo.utcoffset(start_time) is None):
            start_time = start_time.astimezone()
        # Start from one day prior, just to be sure
        start_time -= timedelta(days=1)
        # Get end date if exist
        end_time = datetime.now(timezone.utc).astimezone()
        if args.end_date:
            end_time = args.end_date
            # Make end_time offset-aware
            if (end_time.tzinfo is None
                    or end_time.tzinfo.utcoffset(end_time) is None):
                end_time = end_time.astimezone()

        while start_time < end_time:
            glob_str = f"*/*/{start_time.date().isoformat()}*"
            logging.info(f"glob_str={glob_str}")
            start_time += timedelta(days=1)
            iter_blobs = bucket.list_blobs(
                fields="items(name,crc32c),nextPageToken",
                match_glob=glob_str)
            all_files += [{
                "name": file.name,
                "crc32c": file.crc32c
            } for file in iter_blobs]

        # List log files
        iter_blobs = bucket.list_blobs(
            fields="items(name,crc32c),nextPageToken",
            match_glob="*/*.log.*")
        all_files += [{
            "name": file.name,
            "crc32c": file.crc32c
        } for file in iter_blobs]

    else:
        # List all files, takes longer
        iter_blobs = bucket.list_blobs(
            fields="items(name,crc32c),nextPageToken")
        all_files += [{
            "name": file.name,
            "crc32c": file.crc32c
        } for file in iter_blobs]
    logging.info(f"Total # of files: {len(all_files)}")

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


def rsync(args):
    if which("rsync") is None:
        raise Exception("rsync is not installed!")

    rsync_path = Path(".rsync")
    if (rsync_path.is_file()):
        with open(rsync_path, "r") as fd:
            src = fd.read()
    else:
        src = input("Input source dir path: ")
    cmd = f"rsync -avv {src} {args.log_dir.parent}"
    logging.info("Running command: %s", cmd)
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True)
    if (result.returncode == 0 or not result.stderr):
        logging.debug(result.stdout.decode("utf-8"))
        print("rsync success!")
    else:
        logging.error(result.stderr.decode("utf-8"))


def download(args):
    if (args.rsync):
        rsync(args)
    else:
        download_firebase(args)


def parse(list_args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--rsync", action="store_true",
                        help="Use rsync to server instead of Firebase")
    parser.add_argument("-d", "--log-dir", type=Path, default=Path("./logs"),
                        help="Specify local log directory, default='./logs'")
    parser.add_argument("-s", "--start-date",
                        type=lambda s: datetime.fromisoformat(s),
                        help=("Specify the start date from which the files "
                              "will be downloaded. Use ISO format, ex: "
                              "2024-05-22"))
    parser.add_argument("-e", "--end-date",
                        type=lambda s: datetime.fromisoformat(s),
                        help=("Specify the end date from which the files "
                              "will be downloaded. Use ISO format, ex: "
                              "2024-05-22"))
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
    args = parse()
    logging.basicConfig(level=args.log_level.upper())
    download(args)
