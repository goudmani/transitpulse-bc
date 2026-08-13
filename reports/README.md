# Daily reports

One file per day, `YYYY-MM-DD.md`, written by the ops agent at 03:30 UTC and
committed here by `.github/workflows/agent-daily.yml`.

`patches/` holds unified diffs the code agent proposed. **Nothing in this
directory has been applied or reviewed.** When a patch applies cleanly the
workflow opens it as a draft PR; when it does not, the diff stays here and the
reasoning stays in that day's report.

A missing day is itself a signal — it means the workflow did not run, not that
the pipeline was quiet. See [docs/agent.md](../docs/agent.md).
