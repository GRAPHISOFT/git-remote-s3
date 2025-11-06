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
from typing import Optional
from .common import parse_git_url
from .git import validate_ref_name
from urllib.parse import urlparse

if "lfs" in __name__:
    logging.basicConfig(
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(process)d - %(message)s",
        filename=".git/lfs/tmp/gs-lfs.log",
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


def write_error_event_with_code(code: int, error_message: str):
    logger.error(error_message)
    error_event = { "error": { "code": code, "message": error_message } }
    sys.stdout.write(f"{json.dumps(error_event)}\n")
    sys.stdout.flush()

def get_config_value_from_git(key: str) -> Optional[str]:
    """Get a configuration value from git config."""
    try:
        result = subprocess.run(
            ["git", "config", "--get", key],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode != 0:
            logger.debug(f"No value found for git config key: {key}")
            return None
        
        value = result.stdout.strip()
        logger.debug(f"Found value for git config key {key}: {value}")
        return value
    except Exception as e:
        logger.error(f"Failed to get git config value for key {key}: {e}")
        return None


class LFSProcess:
    def __init__(self, s3uri: str):
        uri_scheme, profile, bucket, prefix = parse_git_url(s3uri)
        if bucket is None or prefix is None:
            write_error_event_with_code(32, f"s3 uri {s3uri} is invalid")
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

        endpoint = get_config_value_from_git("lfs.awsendpoint")
        if not endpoint:
            raise Exception(f"lfs.awsendpoint isn't specified and cannot read from git config!")

        # Try to get credentials using git credential fill and assume role if available
        session = self._create_session_with_credentials(endpoint)
        s3 = session.resource("s3", endpoint_url=endpoint)
        self.s3_bucket = s3.Bucket(self.bucket)

    def _create_session_with_credentials(self, endpoint: str):
        """Create a boto3 session with credentials from git credential fill or fallback to default."""

        if self.profile is None:
            session = boto3.Session()
        else:
            session = boto3.Session(profile_name=self.profile)

        # Test to see if credentials are available
        credentials = session.get_credentials()
        if credentials and credentials.access_key and credentials.secret_key:
            logger.debug("Using credentials from profile or default credential chain")
            return session

        # Try to get web identity token using git credential fill
        web_identity_token = self._get_credential_from_git(endpoint)
        
        if web_identity_token:
            # Try to get a RoleARN from git config, if any
            role_arn = get_config_value_from_git("lfs.rolearn")
            if role_arn:
                logger.debug(f"Using role ARN from git config: {role_arn}")
            else:
                logger.debug("No role ARN found in git config")

            logger.debug("Using assume role with web identity from git credential")
            return self._create_session_with_assume_role(endpoint, web_identity_token, role_arn)
        else:
            # Fall back to profile or default credential chain
            logger.debug("No credential found, falling back to default credential chain")
            if self.profile is None:
                return boto3.Session()
            else:
                return boto3.Session(profile_name=self.profile)

    def _get_credential_from_git(self, endpoint: str) -> Optional[str]:
        """Get web identity token using git credential fill."""
        try:
            # Parse the endpoint URL to get protocol and host
            parsed_endpoint = urlparse(endpoint)
            protocol = parsed_endpoint.scheme or "https"
            host = parsed_endpoint.netloc or parsed_endpoint.path
            
            # Prepare input for git credential fill
            credential_input = f"protocol={protocol}\nhost={host}\n\n"
            
            # Run git credential fill
            result = subprocess.run(
                ["git", "credential", "fill"],
                input=credential_input,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            if result.returncode != 0:
                logger.debug(f"git credential fill failed: {result.stderr}")
                return None

            output_lines = result.stdout.strip().split('\n')
            for line in output_lines:
                if line.startswith('password='):
                    token = line.split('=', 1)[1]
                    return token
            
            logger.debug("No password found in git credential output")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get credential from git: {e}")
            return None

    def _create_session_with_assume_role(self, endpoint: str, web_identity_token: str, role_arn: Optional[str]):
        """Create a boto3 session using assume role with web identity via STS client."""
        try:
            # Create STS client with custom endpoint if provided
            sts_client = boto3.client('sts', endpoint_url=endpoint)
            
            # Call assume_role_with_web_identity
            response = sts_client.assume_role_with_web_identity(
                RoleArn=role_arn if role_arn else "arn:aws:iam::FakeRoleArn", # Some S3-compatible services may not require a valid RoleArn, but boto3 does
                WebIdentityToken=web_identity_token,
                RoleSessionName='git-remote-s3-lfs-session',
                DurationSeconds=3600
            )
            
            # Extract credentials from the response
            credentials = response['Credentials']
            
            # Create a new session with the temporary credentials
            session = boto3.Session(
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
            
            logger.debug("Successfully created session with temporary credentials via STS client")
            return session
            
        except Exception as e:
            logger.error(f"Failed to assume role via STS client: {e}")
            # Fall back to profile or default credential chain
            logger.debug("Falling back to default credential chain")
            if self.profile is None:
                return boto3.Session()
            else:
                return boto3.Session(profile_name=self.profile)

    def upload(self, event: dict):
        logger.debug("upload")
        try:
            self.init_s3_bucket()
            if list(
                self.s3_bucket.objects.filter(
                    Prefix=f"{self.prefix}/{event['oid']}"
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
                f"{self.prefix}/{event['oid']}",
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
            os.makedirs(temp_dir, exist_ok=True)
            self.s3_bucket.download_file(
                Key=f"{self.prefix}/{event['oid']}",
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
        ["git", "config", "--global" if install_global else "--add", "lfs.customtransfer.gs-lfs.path", "gs-lfs"],
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
    # this can be only defined in git-lfs filters in the form https://s3///
    # ensuring the proper config value to be found among global configs.
    #
    result = subprocess.run(
        ["git", "config", "--global" if install_global else "--add",
         "lfs.https://s3///.standalonetransferagent" if install_global else "lfs.standalonetransferagent",
         "gs-lfs"],
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
    # uses the gs-lfs custom transfer agent only if an s3 url is provided in the
    # lfs.url setting. However, before initial clone of a repository this value cannot
    # be preset locally as there is no repo yet. Moreover, it cannot be preset globally
    # either, because its value is repo dependent. So, the workaround is that we provide
    # the fake value globally before the initial clone, to tell git LFS to use gs-lfs
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

    sys.stdout.write("gs-lfs installed\n")
    sys.stdout.flush()


def inferS3Url(event: dict) -> str:
    result = subprocess.run(
        ["git", "remote", "get-url", event["remote"]],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        write_error_event_with_code(32, f"lfs.url isn't specified and cannot resolve remote '{event['remote']}'!")
        sys.exit(1)
    remote_url = result.stdout.decode("utf-8").strip()
    parsed_url = urlparse(remote_url)
    path = parsed_url.path or ""
    if path.endswith("/"):
        path = path.rstrip("/")  # remove any trailing slashes so basename works
    repo_name = os.path.basename(path) # extract last component from url path and use it as repo name
    if not repo_name:
        write_error_event_with_code(32, f"cannot infer repository name from remote url '{remote_url}'")
        sys.exit(1)
    bucket_name = os.path.splitext(repo_name)[0].lower()  # remove optional .git extension and use it as bucket
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
                    "lfs.customtransfer.gs-lfs.args",
                    "debug",
                ]
            )
            print("debug enabled")
            sys.exit(0)
        elif "disable-debug" == sys.argv[1]:
            subprocess.run(
                ["git", "config", "--unset", "lfs.customtransfer.gs-lfs.args"]
            )
            print("debug disabled")
            sys.exit(0)
        else:
            print(f"unknown command {sys.argv[1]}")
            sys.exit(1)

    lfs_process = None
    while True:
        logger.debug("gs-lfs starting")
        line = sys.stdin.readline()
        logger.debug(line)
        event = json.loads(line)
        if event["event"] == "init":
            # This is just another precaution but not strictly necessary since git would
            # already have validated the origin name
            if not validate_ref_name(event["remote"]):
                write_error_event_with_code(3, f"invalid ref {event['remote']}")
                sys.exit(1)

            lfs_url = get_config_value_from_git("lfs.url")
            if not lfs_url: # if lfs.url isn't specified (we try to infer it using remote's url)
                s3uri = inferS3Url(event)
            else: # lfs.url was found
                if lfs_url == "s3://": # the fake value was provided (we try to infer it using remote's url)
                    s3uri = inferS3Url(event)
                else: # We have found a valid value in lfs.url
                    s3uri = lfs_url
            lfs_process = LFSProcess(s3uri=s3uri)

        elif event["event"] == "upload":
            lfs_process.upload(event)
        elif event["event"] == "download":
            lfs_process.download(event)
