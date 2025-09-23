# SPDX-FileCopyrightText: 2023-present Amazon.com, Inc. or its affiliates
#
# SPDX-License-Identifier: Apache-2.0

import re

from .enums import UriScheme


def parse_git_url(url: str) -> tuple[UriScheme, str, str, str]:
    """Parses the elements in a s3:// remote origin URI

    Args:
        url (str): the URI to parse

    Returns:
        tuple[str, str, str, str]: uri scheme, prefix, bucket and profile extracted
        from the URI or None, None, None, None if the URI is invalid
    """
    if url is None:
        return None, None, None, None
    m = re.match(r"(s3|s3\+zip)://([^@]+@)?([a-z0-9][a-z0-9\.-]{2,62})/?(.+)?", url)
    if m is None or len(m.groups()) != 4:
        return None, None, None, None
    uri_scheme, profile, bucket, prefix = m.groups()
    if profile is not None:
        profile = profile[:-1]
    if prefix is not None:
        prefix = prefix.strip("/")
    if uri_scheme is not None:
        if uri_scheme == "s3":
            uri_scheme = UriScheme.S3
        if uri_scheme == "s3+zip":
            uri_scheme = UriScheme.S3_ZIP

    return uri_scheme, profile, bucket, prefix
