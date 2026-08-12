# ADR 003 — No NAT Gateway

## Status
Accepted

## Context
A NAT Gateway costs roughly $32/month plus per-GB data processing, which would
be the single largest line item in a project budgeted at $40/month. The only
component needing outbound internet is the GTFS poller.

## Decision
Keep the poller Lambda outside the VPC (unattached Lambdas have internet access
at no cost), and give everything inside the VPC free S3 and DynamoDB gateway
endpoints.

## Consequences
- Roughly $35/month saved with no loss of function.
- The poller cannot reach private VPC resources. It does not need to.
- If a future component inside the VPC needs an AWS API not covered by a
  gateway endpoint, an interface endpoint (~$7.20/month) is the next step, not
  a NAT Gateway.
