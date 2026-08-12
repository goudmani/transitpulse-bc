"""Disable the ingestion schedule when estimated charges cross the threshold.

Wired to a CloudWatch billing alarm. Cheap insurance: the worst outcome of a
runaway pipeline is a bill, and this bounds it without human reaction time.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3

LOG = logging.getLogger()
LOG.setLevel("INFO")

_events = None
_sns = None


def events_client():
    global _events
    if _events is None:
        _events = boto3.client("events")
    return _events


def sns_client():
    global _sns
    if _sns is None:
        _sns = boto3.client("sns")
    return _sns


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    rule_name = os.environ["RULE_NAME"]
    topic_arn = os.environ["TOPIC_ARN"]

    events_client().disable_rule(Name=rule_name)

    message = (
        "TransitPulse ingestion has been DISABLED by the cost guard.\n\n"
        f"EventBridge rule disabled: {rule_name}\n\n"
        "Investigate spend in Cost Explorer, then re-enable with:\n"
        f"  aws events enable-rule --name {rule_name}\n"
    )
    sns_client().publish(
        TopicArn=topic_arn,
        Subject="TransitPulse: ingestion disabled by cost guard",
        Message=message,
    )

    LOG.warning(json.dumps({"action": "disabled_rule", "rule": rule_name}))
    return {"disabled": rule_name}
