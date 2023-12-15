import argparse
import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
from google.cloud.storage import transfer_manager
import logging
from pathlib import Path

cred = credentials.Certificate(
    "nd-schmidt-firebase-adminsdk-d1gei-43db929d8a.json")
firebase_admin.initialize_app(cred, {
    "storageBucket": "nd-schmidt.appspot.com"
})


def download(args):
    # Setup
    logging.basicConfig(level=args.log_level.upper())
    logdir = args.log_dir
    logging.debug("logdir=%s", logdir)

    bucket = storage.bucket()

    # List files to download
    all_files = [file.name for file in bucket.list_blobs()]
    to_download = list(filter(lambda x: not logdir.joinpath(x).exists(),
                              all_files))
    print("Found %d files to download" % len(to_download))
    logging.debug(to_download)

    results = transfer_manager.download_many_to_path(
        bucket, to_download, destination_directory=str(logdir),
        create_directories=True, max_workers=8
    )

    for name, result in zip(to_download, results):
        # The results list is either `None` or an exception for each filename
        # in the input list, in order.

        if isinstance(result, Exception):
            logging.warning("Failed to download %s due to exception: %s",
                            name, result)
        else:
            print("Downloaded %s from %s." % (name, bucket.name))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--log-dir", type=Path, default=Path("./logs"),
                        help="Specify local log directory, default='./logs'")
    parser.add_argument("-l", "--log-level", default="warning",
                        help="Provide logging level, default is warning'")
    args = parser.parse_args()

    download(args)
