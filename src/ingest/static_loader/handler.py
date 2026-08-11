"""Download the weekly GTFS static schedule ZIP and unpack it into bronze.

Idempotent: the archive's SHA-256 is stored in SSM Parameter Store, and an
unchanged archive is a no-op. Uses urllib rather than requests because the
Lambda Python runtime ships boto3 but no third-party HTTP client, and this
function is packaged as a plain zip.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import urllib.request
import zipfile
from typing import Any

import boto3

LOG = logging.getLogger()
LOG.setLevel("INFO")

_s3 = None
_ssm = None


def s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def ssm_client():
    global _ssm
    if _ssm is None:
        _ssm = boto3.client("ssm")
    return _ssm


WANTED = {
    "agency.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "routes.txt",
    "shapes.txt",
    "stop_times.txt",
    "stops.txt",
    "trips.txt",
}


def download(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "transitpulse/1.0"})
    with urllib.request.urlopen(request, timeout=300) as response:  # noqa: S310
        return response.read()


def stored_digest(param: str) -> str | None:
    ssm = ssm_client()
    try:
        return ssm.get_parameter(Name=param)["Parameter"]["Value"]
    except ssm.exceptions.ParameterNotFound:
        return None


def version_from_event(event: dict[str, Any]) -> str:
    """EventBridge supplies an ISO timestamp; take the date part."""
    raw = str(event.get("time", ""))
    return raw[:10] if len(raw) >= 10 else "manual"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    bucket = os.environ["BRONZE_BUCKET"]
    param = os.environ["SSM_PARAM"]

    payload = download(os.environ["GTFS_URL"])
    digest = hashlib.sha256(payload).hexdigest()

    if stored_digest(param) == digest:
        LOG.info(json.dumps({"status": "unchanged", "sha256": digest}))
        return {"status": "unchanged", "sha256": digest}

    version = version_from_event(event)
    written = []

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for name in archive.namelist():
            base = os.path.basename(name)
            if base not in WANTED:
                continue
            key = f"static/gtfs/version={version}/{base}"
            s3_client().put_object(Bucket=bucket, Key=key, Body=archive.read(name))
            written.append(base)

    ssm_client().put_parameter(Name=param, Value=digest, Type="String", Overwrite=True)

    LOG.info(
        json.dumps({"status": "updated", "version": version, "files": written, "sha256": digest})
    )
    return {"status": "updated", "version": version, "files": written}
