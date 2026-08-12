# ADR 004 — SageMaker Serverless Inference

## Status
Accepted

## Context
A portfolio endpoint receives near-zero traffic, in bursts, when someone opens
the demo. An `ml.m5.large` real-time endpoint costs about $96/month whether or
not anyone calls it.

## Decision
Deploy to a serverless endpoint at 2048 MB with max concurrency 5.

## Consequences
- Costs approximately nothing at idle.
- Cold starts of roughly 1–3 seconds on the first request after idle. Measured
  and reported in the README rather than hidden.
- Provisioned concurrency is the documented production fix; it is deliberately
  not enabled here because the cost would not buy anything for this use case.
- Max payload and duration limits apply, both well within what a single-row
  XGBoost prediction needs.
