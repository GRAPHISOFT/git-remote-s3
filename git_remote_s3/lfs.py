# SPDX-FileCopyrightText: 2023-present Amazon.com, Inc. or its affiliates
#
# SPDX-License-Identifier: Apache-2.0

import sys
import logging
import json
import subprocess
import boto3
import threading
import os
from .common import parse_git_url
from .git import validate_ref_name
from urllib.parse import urlparse

if "lfs" in __name__:
    logging.basicConfig(
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(process)d - %(message)s",
        filename=".git/lfs/tmp/git-lfs-s3.log",
    )

logger = logging.getLogger(__name__)


class ProgressPercentage:
    def __init__(self, oid: str):
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self.oid = oid

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            progress_event = {
                "event": "progress",
                "oid": self.oid,
                "bytesSoFar": self._seen_so_far,
                "bytesSinceLast": bytes_amount,
            }
            sys.stdout.write(f"{json.dumps(progress_event)}\n")
            sys.stdout.flush()


def write_error_event(*, oid: str, error: str, flush=False):
    err_event = {
        "event": "complete",
        "oid": oid,
        "error": {"code": 2, "message": error},
    }
    sys.stdout.write(f"{json.dumps(err_event)}\n")
    if flush:
        sys.stdout.flush()


class LFSProcess:
    def __init__(self, s3uri: str):
        uri_scheme, profile, bucket, prefix = parse_git_url(s3uri)
        if bucket is None or prefix is None:
            logger.error(f"s3 uri {s3uri} is invalid")
            error_event = {
                "error": {"code": 32, "message": f"s3 uri {s3uri} is invalid"}
            }
            sys.stdout.write(f"{json.dumps(error_event)}\n")
            sys.stdout.flush()
            return
        self.prefix = prefix
        self.bucket = bucket
        self.profile = profile
        self.s3_bucket = None
        sys.stdout.write("{}\n")
        sys.stdout.flush()

    def init_s3_bucket(self):
        if self.s3_bucket is not None:
            return
        if self.profile is None:
            session = boto3.Session()
        else:
            session = boto3.Session(profile_name=self.profile)

        # Read the aws server url from git config "lfs.awsendpoint"
        result = subprocess.run(
            ["git", "config", "--get", "lfs.awsendpoint"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            logger.error(result.stderr.decode("utf-8").strip())
            error_event = {
                "error": {
                    "code": 2,
                    "message": "cannot read lfs.awsendpoint from git config",
                }
            }
            sys.stdout.write(f"{json.dumps(error_event)}")
            sys.stdout.flush()
            sys.exit(1)
        endpoint = result.stdout.decode("utf-8").strip()

        # Create the S3 resource with the custom endpoint_url
        if endpoint:
            s3 = session.resource("s3", endpoint_url=endpoint)
        else:
            s3 = session.resource("s3")
        self.s3_bucket = s3.Bucket(self.bucket)

    def upload(self, event: dict):
        logger.debug("upload")
        try:
            self.init_s3_bucket()
            if list(
                self.s3_bucket.objects.filter(
                    Prefix=f"{self.prefix}/lfs/{event['oid']}"
                )
            ):
                logger.debug("object already exists")
                sys.stdout.write(
                    f"{json.dumps({'event': 'complete', 'oid': event['oid']})}\n"
                )
                sys.stdout.flush()
                return
            self.s3_bucket.upload_file(
                event["path"],
                f"{self.prefix}/lfs/{event['oid']}",
                Callback=ProgressPercentage(event["oid"]),
            )
            sys.stdout.write(
                f"{json.dumps({'event': 'complete', 'oid': event['oid']})}\n"
            )
        except Exception as e:
            logger.error(e)
            write_error_event(oid=event["oid"], error=str(e))
        sys.stdout.flush()

    def download(self, event: dict):
        logger.debug("download")
        try:
            self.init_s3_bucket()
            temp_dir = os.path.abspath(".git/lfs/tmp")
            self.s3_bucket.download_file(
                Key=f"{self.prefix}/lfs/{event['oid']}",
                Filename=f"{temp_dir}/{event['oid']}",
                Callback=ProgressPercentage(event["oid"]),
            )
            done_event = {
                "event": "complete",
                "oid": event["oid"],
                "path": f"{temp_dir}/{event['oid']}",
            }
            sys.stdout.write(f"{json.dumps(done_event)}\n")
        except Exception as e:
            logger.error(e)
            write_error_event(oid=event["oid"], error=str(e))

        sys.stdout.flush()


def install(install_global = False):
    result = subprocess.run(
        ["git", "config", "--global" if install_global else "--add", "lfs.customtransfer.git-lfs-s3.path", "git-lfs-s3"],
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr.decode("utf-8").strip())
        sys.stderr.flush()
        sys.exit(1)
    #
    # Here, we had to use a slightly awkward syntax because git-lfs only accepts
    # http(s) urls to filter configuration values for custom transfer agent urls.
    # As we have to use an s3:// url to access our AWS server,
    # this can be only defined in git-lfs filters in the form http://s3///
    # ensuring the proper config value to be found among global configs.
    #
    result = subprocess.run(
        ["git", "config", "--global" if install_global else "--add",
         "lfs.http://s3///.standalonetransferagent" if install_global else "lfs.standalonetransferagent",
         "git-lfs-s3"],
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr.decode("utf-8").strip())
        sys.stderr.flush()
        sys.exit(1)

    #
    # Here, we had to define a fake lfs.url configuration globally with value s3://
    # The lfs.py on init will read the filtered configuration values. If lfs.url is
    # not provided, or it is provided, but contains this fake value, the init will
    # take the default value instead of using the setting. This is needed as git LFS
    # uses the git-lfs-s3 custom transfer agent only if an s3 url is provided in the
    # lfs.url setting. However, before initial clone of a repository this value cannot
    # be preset locally as there is no repo yet. Moreover, it cannot be preset globally
    # either, because its value is repo dependent. So, the workaround is that we provide
    # the fake value globally before the initial clone, to tell git LFS to use git-lfs-s3
    # on machines where the fake value has been set, and get large files from the
    # working (default) url at clone.
    #
    if install_global:
        result = subprocess.run(
            ["git", "config", "--global",
             "lfs.url",
             "s3://"],
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            sys.stderr.write(result.stderr.decode("utf-8").strip())
            sys.stderr.flush()
            sys.exit(1)

    sys.stdout.write("git-lfs-s3 installed\n")
    sys.stdout.flush()


def inferS3Url(event: dict) -> str:
    result = subprocess.run(
        ["git", "remote", "get-url", event["remote"]],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        logger.error(result.stderr.decode("utf-8").strip())
        error_event = {
            "error": {
                "code": 2,
                "message": f"lfs.url isn't specified and cannot resolve remote \"{event['remote']}\"",
            }
        }
        sys.stdout.write(f"{json.dumps(error_event)}")
        sys.stdout.flush()
        sys.exit(1)
    remote_url = result.stdout.decode("utf-8").strip()
    parsed_url = urlparse(remote_url)
    path = parsed_url.path
    repo_name = os.path.basename(path)  # extract last component from url path and use it as repo
    bucket_name = os.path.splitext(repo_name)[0]  # remove optional .git extension and use it as bucket
    return f"s3://{bucket_name}/{repo_name}"


def main():  # noqa: C901
    if len(sys.argv) > 1:
        if "install" == sys.argv[1]:
            install()
            sys.exit(0)
        elif "glinstall" == sys.argv[1]:
            install(install_global=True)
            sys.exit(0)
        elif "debug" == sys.argv[1]:
            logger.setLevel(logging.DEBUG)
        elif "enable-debug" == sys.argv[1]:
            subprocess.run(
                [
                    "git",
                    "config",
                    "--add",
                    "lfs.customtransfer.git-lfs-s3.args",
                    "debug",
                ]
            )
            print("debug enabled")
            sys.exit(0)
        elif "disable-debug" == sys.argv[1]:
            subprocess.run(
                ["git", "config", "--unset", "lfs.customtransfer.git-lfs-s3.args"]
            )
            print("debug disabled")
            sys.exit(0)
        else:
            print(f"unknown command {sys.argv[1]}")
            sys.exit(1)

    lfs_process = None
    while True:
        logger.debug("git-lfs-s3 starting")
        line = sys.stdin.readline()
        logger.debug(line)
        event = json.loads(line)
        if event["event"] == "init":
            # This is just another precaution but not strictly necessary since git would
            # already have validated the origin name
            if not validate_ref_name(event["remote"]):
                logger.error(f"invalid ref {event['remote']}")
                sys.stdout.write("{}\n")
                sys.stdout.flush()
                sys.exit(1)

            # try to get lfs.url from config
            result = subprocess.run(
                ["git", "config", "--get", "lfs.url"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if result.returncode != 0: # if lfs.url isn't specified (we try to infer it using remote's url)
                s3uri = inferS3Url(event)
            else: # lfs.url was found
                url_value = result.stdout.decode("utf-8").strip()
                if url_value == "s3://": # the fake value was provided (we try to infer it using remote's url)
                    s3uri = inferS3Url(event)
                else: # We have found a valid value in lfs.url
                    s3uri = url_value
            lfs_process = LFSProcess(s3uri=s3uri)

        elif event["event"] == "upload":
            lfs_process.upload(event)
        elif event["event"] == "download":
            lfs_process.download(event)
