"""Production health: Kinesis, Firehose, Lambda, Glue, Step Functions, alarms.

These answer "is it running". agent/tools/data.py answers the different and
independent question of "is the output any good" -- a green pipeline can still
produce a thin or unlabelled day.
"""

from __future__ import annotations

from langchain_core.tools import tool

from agent import config
from agent.tools._aws import (
    account_id,
    aws_error,
    client,
    metric_max,
    metric_series,
    metric_total,
    ok,
    problem,
    resources_by_prefix,
    table,
)

_HOUR = 3600


@tool
def check_alarms() -> str:
    """List every CloudWatch alarm for this project and its current state.

    Start here. An alarm already in ALARM is the cheapest possible signal and
    tells you where to look with the other tools.
    """
    try:
        cw = client("cloudwatch")
        alarms = cw.describe_alarms(AlarmNamePrefix=config.RESOURCE_PREFIX, MaxRecords=100)[
            "MetricAlarms"
        ]
    except Exception as exc:
        return aws_error("describe_alarms", exc)

    if not alarms:
        return problem(
            f"no alarms found with prefix '{config.RESOURCE_PREFIX}'. Either the "
            f"observability module is not deployed or the whole stack is torn down."
        )

    rows = [
        [
            a["AlarmName"].removeprefix(f"{config.RESOURCE_PREFIX}-"),
            a["StateValue"],
            a.get("StateUpdatedTimestamp", "").isoformat(timespec="minutes")
            if a.get("StateUpdatedTimestamp")
            else "-",
        ]
        for a in sorted(alarms, key=lambda a: a["StateValue"] != "ALARM")
    ]
    firing = [a["AlarmName"] for a in alarms if a["StateValue"] == "ALARM"]
    # INSUFFICIENT_DATA is called out separately because it is genuinely
    # ambiguous: on a metric that only publishes when something happens it means
    # "nothing happened", which can be correct (no errors) or a total outage.
    insufficient = [a["AlarmName"] for a in alarms if a["StateValue"] == "INSUFFICIENT_DATA"]

    header = (
        problem(f"{len(firing)} alarm(s) firing: {', '.join(firing)}")
        if firing
        else ok(f"{len(alarms)} alarms, none firing")
    )
    note = (
        f"\nNote: {len(insufficient)} alarm(s) in INSUFFICIENT_DATA. On error-count "
        f"metrics that usually means no errors were published, which is healthy."
        if insufficient
        else ""
    )
    return f"{header}\n\n{table(['alarm', 'state', 'since'], rows)}{note}"


@tool
def check_ingestion() -> str:
    """Kinesis and Firehose over the last 24h: records in, iterator age,
    throttling, delivery success, and conversion errors.

    Use this when bronze looks thin or the ingest-stalled alarm fired.
    """
    start, end = config.lookback_window()
    stream = f"{config.RESOURCE_PREFIX}-gtfs"
    firehose = f"{config.RESOURCE_PREFIX}-to-bronze"
    lines: list[str] = []

    try:
        put_records = metric_total(
            "AWS/Kinesis",
            "IncomingRecords",
            {"StreamName": stream},
            stat="Sum",
            period=_HOUR,
            start=start,
            end=end,
        )
        put_bytes = metric_total(
            "AWS/Kinesis",
            "IncomingBytes",
            {"StreamName": stream},
            stat="Sum",
            period=_HOUR,
            start=start,
            end=end,
        )
        # This is the metric that caught the original 2,400 rec/sec overrun
        # against the 1,000/shard limit. Anything above zero means records were
        # rejected and the poller's pacing has regressed.
        throttled = metric_total(
            "AWS/Kinesis",
            "WriteProvisionedThroughputExceeded",
            {"StreamName": stream},
            stat="Sum",
            period=_HOUR,
            start=start,
            end=end,
        )
        # Iterator age is the consumer-lag signal: if Firehose falls behind the
        # stream, this climbs and data ages out at the 24h retention edge.
        iter_age_ms = metric_max(
            "AWS/Kinesis",
            "GetRecords.IteratorAgeMilliseconds",
            {"StreamName": stream},
            stat="Maximum",
            period=_HOUR,
            start=start,
            end=end,
        )
        lines.append(
            f"kinesis {stream}: {put_records:,.0f} records in, "
            f"{put_bytes / 1e6:,.1f} MB, throttled={throttled:,.0f}, "
            f"max iterator age={iter_age_ms / 1000:,.0f}s"
        )
        if throttled > 0:
            lines.append(
                problem(
                    "WriteProvisionedThroughputExceeded is non-zero -- the poller "
                    "is putting faster than the shard allows. Check _pace() in "
                    "src/ingest/poller/handler.py."
                )
            )
        if put_records == 0:
            lines.append(
                problem("zero records into Kinesis in 24h -- ingestion is stopped or paused.")
            )
    except Exception as exc:
        lines.append(aws_error("kinesis metrics", exc))

    try:
        delivered = metric_total(
            "AWS/Firehose",
            "DeliveryToS3.Records",
            {"DeliveryStreamName": firehose},
            stat="Sum",
            period=_HOUR,
            start=start,
            end=end,
        )
        success = metric_series(
            "AWS/Firehose",
            "DeliveryToS3.Success",
            {"DeliveryStreamName": firehose},
            stat="Average",
            period=_HOUR,
            start=start,
            end=end,
        )
        worst = min(success) if success else 1.0
        lines.append(
            f"firehose {firehose}: {delivered:,.0f} records delivered to S3, "
            f"worst hourly success rate={worst:.3f}"
        )
        if worst < 1.0:
            lines.append(
                problem(f"Firehose delivery success dipped to {worst:.3f} -- check errors/ in S3.")
            )
    except Exception as exc:
        lines.append(aws_error("firehose metrics", exc))

    return "\n".join(lines)


@tool
def check_bronze_and_errors() -> str:
    """Did raw data land in S3 today, and is the Firehose errors/ prefix empty?

    The errors/ prefix is where record-format conversion failures go. It has been
    zero since the deaggregation processor was ordered ahead of metadata
    extraction; anything in it means that ordering broke.
    """
    try:
        s3 = client("s3")
        bucket = f"{config.RESOURCE_PREFIX}-bronze-{account_id()}"
        today = config.run_date()

        raw = s3.list_objects_v2(
            Bucket=bucket, Prefix=f"raw/trip_updates/dt={today}/", MaxKeys=1000
        )
        raw_count = raw.get("KeyCount", 0)
        raw_bytes = sum(o["Size"] for o in raw.get("Contents", []))

        errs = s3.list_objects_v2(Bucket=bucket, Prefix="errors/", MaxKeys=10)
        err_count = errs.get("KeyCount", 0)

        lines = [
            f"bronze s3://{bucket}/raw/trip_updates/dt={today}/: "
            f"{raw_count} objects, {raw_bytes / 1e6:,.1f} MB"
        ]
        if raw_count == 0:
            lines.append(
                problem(
                    f"no bronze objects for {today}. Note the day is partial until "
                    f"23:59 UTC -- an empty partition early in the UTC day is normal, "
                    f"an empty one late in the day is not."
                )
            )
        lines.append(
            problem(f"errors/ prefix has {err_count} object(s) -- conversion is failing")
            if err_count
            else ok("errors/ prefix is empty")
        )
        return "\n".join(lines)
    except Exception as exc:
        return aws_error("s3 bronze listing", exc)


@tool
def check_lambdas() -> str:
    """Invocations, errors, throttles and duration for every project Lambda over
    24h, plus the poller dead-letter queue depth.

    A non-empty DLQ is the strongest signal available: it means a payload failed
    every retry and is now sitting unprocessed.
    """
    start, end = config.lookback_window()
    try:
        fns = []
        paginator = client("lambda").get_paginator("list_functions")
        for page in paginator.paginate():
            fns.extend(resources_by_prefix(page["Functions"], "FunctionName"))
    except Exception as exc:
        return aws_error("list_functions", exc)

    if not fns:
        return problem(f"no Lambdas found with prefix '{config.RESOURCE_PREFIX}'")

    rows = []
    for fn in fns:
        name = fn["FunctionName"]
        dims = {"FunctionName": name}
        kw = {"period": _HOUR, "start": start, "end": end}
        try:
            inv = metric_total("AWS/Lambda", "Invocations", dims, stat="Sum", **kw)
            err = metric_total("AWS/Lambda", "Errors", dims, stat="Sum", **kw)
            thr = metric_total("AWS/Lambda", "Throttles", dims, stat="Sum", **kw)
            p_max = metric_max("AWS/Lambda", "Duration", dims, stat="Maximum", **kw)
            rows.append(
                [
                    name.removeprefix(f"{config.RESOURCE_PREFIX}-"),
                    f"{inv:,.0f}",
                    f"{err:,.0f}",
                    f"{thr:,.0f}",
                    f"{p_max / 1000:,.1f}s",
                    f"{fn.get('Timeout', '?')}s",
                    f"{fn.get('MemorySize', '?')}MB",
                ]
            )
        except Exception as exc:
            rows.append([name, "?", "?", "?", "?", "?", str(exc)[:40]])

    out = [table(["function", "invokes", "errors", "throttles", "max_dur", "timeout", "mem"], rows)]

    # A max duration close to the configured timeout is a warning even when the
    # error count is zero -- it means the next slow day starts timing out.
    out.append(
        "\nRead max_dur against timeout: within ~20% of the timeout means the next "
        "slow run fails, even though errors is currently 0."
    )

    try:
        sqs = client("sqs")
        url = sqs.get_queue_url(QueueName=f"{config.RESOURCE_PREFIX}-poller-dlq")["QueueUrl"]
        attrs = sqs.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        depth = int(attrs["ApproximateNumberOfMessages"])
        out.append(
            problem(f"poller DLQ has {depth} message(s)") if depth else ok("poller DLQ is empty")
        )
    except Exception as exc:
        out.append(aws_error("poller DLQ", exc))

    return "\n".join(out)


@tool
def check_etl() -> str:
    """Step Functions executions and Glue job runs over the last 24h.

    A *missing* execution is worse than a failed one: a failure sends an email,
    an absence is silent. The ETL is scheduled daily at 02:20 UTC, so by the time
    this agent runs there should be exactly one new SUCCEEDED execution.
    """
    lines: list[str] = []
    try:
        sfn = client("stepfunctions")
        machines = resources_by_prefix(sfn.list_state_machines()["stateMachines"], "name")
        etl = next((m for m in machines if m["name"] == f"{config.RESOURCE_PREFIX}-etl"), None)
        if not etl:
            lines.append(problem(f"state machine '{config.RESOURCE_PREFIX}-etl' not found"))
        else:
            execs = sfn.list_executions(stateMachineArn=etl["stateMachineArn"], maxResults=5)[
                "executions"
            ]
            rows = [
                [
                    e["status"],
                    e["startDate"].isoformat(timespec="minutes"),
                    f"{(e.get('stopDate', e['startDate']) - e['startDate']).total_seconds() / 60:.1f}m",
                ]
                for e in execs
            ]
            lines.append(
                "last 5 ETL executions:\n" + table(["status", "started", "duration"], rows)
            )

            start, _ = config.lookback_window()
            recent = [e for e in execs if e["startDate"] >= start]
            if not recent:
                lines.append(
                    problem(
                        "no ETL execution started in the last 24h. The 02:20 UTC "
                        "schedule did not fire, or the EventBridge rule is disabled."
                    )
                )
            elif any(e["status"] == "FAILED" for e in recent):
                lines.append(problem("an ETL execution FAILED in the last 24h"))
            else:
                lines.append(ok(f"{len(recent)} ETL execution(s) in the last 24h, none failed"))
    except Exception as exc:
        lines.append(aws_error("stepfunctions", exc))

    try:
        glue = client("glue")
        jobs = [j for j in glue.list_jobs()["JobNames"] if j.startswith(config.RESOURCE_PREFIX)]
        start, _ = config.lookback_window()
        rows = []
        for job in jobs:
            runs = glue.get_job_runs(JobName=job, MaxResults=3)["JobRuns"]
            recent = [r for r in runs if r["StartedOn"] >= start]
            latest = runs[0] if runs else None
            rows.append(
                [
                    job.removeprefix(f"{config.RESOURCE_PREFIX}-"),
                    latest["JobRunState"] if latest else "never run",
                    f"{latest.get('ExecutionTime', 0)}s" if latest else "-",
                    # DPU-seconds is the Glue cost driver, so a job whose runtime
                    # has crept up is a cost finding as much as a health one.
                    f"{latest.get('DPUSeconds', 0):,.0f}" if latest else "-",
                    str(len(recent)),
                ]
            )
        lines.append(
            "\nglue jobs:\n"
            + table(["job", "last_state", "exec_time", "dpu_sec", "runs_24h"], rows)
        )
    except Exception as exc:
        lines.append(aws_error("glue job runs", exc))

    return "\n".join(lines)


@tool
def check_schedules() -> str:
    """Are the EventBridge rules that drive this pipeline actually enabled?

    `make pause` disables the poll rule to stop ingestion, and forgetting to
    resume it is a completely silent failure: nothing errors, no alarm fires on
    an error metric, data simply stops arriving. Check this before concluding
    that a quiet pipeline is broken -- it may just be switched off.
    """
    try:
        rules = client("events").list_rules(NamePrefix=config.RESOURCE_PREFIX)["Rules"]
    except Exception as exc:
        return aws_error("list_rules", exc)

    if not rules:
        return problem(f"no EventBridge rules found with prefix '{config.RESOURCE_PREFIX}'")

    rows = [
        [
            r["Name"].removeprefix(f"{config.RESOURCE_PREFIX}-"),
            r.get("State", "?"),
            r.get("ScheduleExpression", "(event pattern)"),
        ]
        for r in rules
    ]
    disabled = [r["Name"] for r in rules if r.get("State") != "ENABLED"]
    header = (
        problem(f"{len(disabled)} rule(s) DISABLED: {', '.join(disabled)}")
        if disabled
        else ok(f"all {len(rules)} rules enabled")
    )
    return f"{header}\n\n{table(['rule', 'state', 'schedule'], rows)}"


@tool
def read_error_logs(function_name: str, max_lines: int = 15) -> str:
    """Pull recent ERROR/Exception/Traceback lines from a Lambda's CloudWatch log
    group. Pass the short name, e.g. 'poller', 'static-loader', 'online-features'.

    Only call this after another tool has shown a non-zero error count for that
    function -- it is the most expensive tool here in tokens.
    """
    short = function_name.removeprefix(f"{config.RESOURCE_PREFIX}-")
    group = f"/aws/lambda/{config.RESOURCE_PREFIX}-{short}"
    start, end = config.lookback_window()
    try:
        resp = client("logs").filter_log_events(
            logGroupName=group,
            startTime=int(start.timestamp() * 1000),
            endTime=int(end.timestamp() * 1000),
            filterPattern="?ERROR ?Exception ?Traceback ?Timed",
            limit=max(1, min(max_lines, 40)),
        )
    except Exception as exc:
        return aws_error(f"filter_log_events on {group}", exc)

    events = resp.get("events", [])
    if not events:
        return ok(f"no error lines in {group} over the last 24h")
    # Truncated hard: a single Spark or boto traceback can run to thousands of
    # tokens and one is enough to identify the fault.
    return f"{len(events)} error line(s) in {group}:\n" + "\n".join(
        e["message"].strip()[:300] for e in events
    )


HEALTH_TOOLS = [
    check_alarms,
    check_schedules,
    check_ingestion,
    check_bronze_and_errors,
    check_lambdas,
    check_etl,
    read_error_logs,
]
