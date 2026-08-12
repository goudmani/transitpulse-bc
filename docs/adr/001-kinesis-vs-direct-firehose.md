# ADR 001 — Kinesis Data Streams in front of Firehose

## Status
Accepted

## Context
The poller could write straight to Firehose with `put_record_batch`, which
would remove a component and about $11/month of shard cost. Firehose alone
delivers to S3 perfectly well.

## Decision
Publish to a Kinesis Data Stream, and attach Firehose as one consumer.

## Consequences
- A second consumer (the online feature writer) reads the same records without
  re-reading S3, which is what makes sub-100ms serving features possible.
- 24-hour retention means a failed consumer can be replayed rather than losing
  data.
- Costs ~$11/month for one shard, and one shard caps throughput at 1,000
  records/sec. At current feed volume this is comfortable; a second agency
  would require a second shard.
- Revisit if the online feature path is ever removed, at which point Firehose
  alone is the cheaper and simpler answer.
