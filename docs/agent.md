# The daily ops agent

A supervisor agent and four specialist subagents that inspect the running
TransitPulse pipeline once a day, then file a report in
[reports/](../reports/). Built on LangChain 1.x, running on Groq.

## Why it is shaped this way

The interesting problem here is not "call an LLM in a loop". It is that an agent
which runs unattended at 03:30 with nobody reading the logs has two failure modes
that matter more than being clever:

**It can be wrong quietly.** So the subagents do not compute anything. Every
number in the report came from a `boto3` call in `agent/tools/`, and the prompts
refuse findings that do not quote one. The supervisor carries findings through
verbatim; the only prose the model writes freely is the two-sentence summary at
the top. A model having an off day makes that summary bland — it cannot drop a
critical finding or invent a cost figure.

**It can do damage.** So it holds a read-only IAM role with an explicit deny on
every mutating action (`infra/modules/cicd/agent.tf`), the Athena tool rejects
anything that is not a `SELECT`, the repo tools cannot escape the repository or
read `.env`, and the code agent proposes diffs it is structurally unable to
apply. Patches land as a **draft PR** for a human.

The two are related: an agent that cannot change anything is one you can afford
to let be wrong.

## The agents

| Agent | Question it answers | Tools |
| --- | --- | --- |
| **infrastructure** | Is it running? | alarms, EventBridge rules, Kinesis/Firehose metrics, Lambda metrics + DLQ, Step Functions + Glue runs, error logs |
| **cost** | Is it spending more than it should, and on what? | Cost Explorer daily-by-service, threshold check, per-day driver attribution |
| **data_quality** | Is the output usable for training? | Athena: collection progress, per-day label rate and duplicate count, feature nulls |
| **code** | Does any of that trace to a defect in this repo? | read-only file listing, file read, regex search, git log |
| **supervisor** | What does a human need to know? | compiles the four reports, writes `reports/YYYY-MM-DD.md` |

Every run is traced end to end in LangSmith — see [Tracing](#tracing-langsmith).

## The docs agent

A second, separate run at **15:30 UTC** (`.github/workflows/agent-docs.yml`)
keeps the README honest. Twelve hours after the ops agent on purpose: Cost
Explorer lags ~24h and the ETL lands silver at 02:20 UTC, so mid-afternoon
describes a settled day.

It does two things, and the split between them is the design:

**Numbers are rendered, never written.** Everything between `agent:*:begin` and
`agent:*:end` markers in the README is regenerated from live queries with no
model involved. The queries live in
[`sql/07_profile_queries.sql`](../sql/07_profile_queries.sql) — one source of
truth, so editing the SQL changes what the README says and the charts and the
prose cannot disagree. A model asked to "update the figures" produces plausible
numbers, drifts slightly each day, and eventually states something false in a
document that looks maintained.

**Prose is checked, never rewritten.** Claims no query can settle — "a deployed
SageMaker model that beats the published schedule" — go to a model that compares
them against live facts and *reports* contradictions to
`reports/<date>-docs-drift.md`. It cannot edit them. A sentence is an argument,
and rewriting one should be a human decision.

The drift check returns structured `DriftClaim` objects requiring a **verbatim
quote**, and any quote not found in the file is dropped before reporting. The
first version returned prose and was unusable: it hedged, contradicted itself
mid-sentence, and invented findings to fill space. It also runs on the
higher-reasoning client, because judging whether a sentence is contradicted is
exactly the multi-step call that low effort does badly.

Charts are redrawn by re-running the profile queries into `data/processed/` and
calling `scripts/plot_profile.py`. That directory is gitignored — derived data
is regenerable — which is precisely why the charts had been pinned to the first
two days of collection: nothing automatic could rebuild their inputs.

```bash
make docs           # full: figures, charts, drift check
make docs-figures   # figures only -- no matplotlib, no model, no tokens
```

They run in a fixed order, not a model-routed one. The first three are
independent and answer different questions, so there is nothing to route; the
code agent runs last because it needs the others to tell it what broke. It is
handed only the critical and warning findings — given three healthy reports in
full, a model reliably invents something to "improve".

Each subagent returns a Pydantic `SubagentReport` through LangChain's
`ToolStrategy`, so the supervisor can sort and count findings without asking a
model to do it — and because a structured response costs a fraction of the
tokens of a subagent narrating what it found.

## What "normal" means to it

The prompts in `agent/subagents.py` carry this pipeline's measured baselines:
~16M bronze rows/day, ~12:1 collapse to stop events, 0 duplicate keys, ~$0.70/day,
exactly one ETL execution at 02:20 UTC, empty `errors/` prefix. Without those a
model grades everything against a generic prior and reports a normal day as
alarming.

They also carry the four cost fixes from August 2026 by name, so a DynamoDB line
item reappearing is reported as *"fix 1 was reverted"* rather than as a mystery
charge. And they carry the known stubs — `is_holiday`, `active_alert_on_route`
and the weather features are deliberately constant — so those are not rediscovered
as bugs every morning.

## Tracing (LangSmith)

Each daily run is **one trace**: a root span for the day, with the four
subagents, every model call and every tool call nested underneath. The report
tells you what the agent concluded; the trace tells you why — which is the only
practical way to check a finding you think is wrong, or to work out why a
subagent burned nine turns on a question that needed two.

`agent/tracing.py` owns this. Three things it does that a bare
`LANGSMITH_TRACING=true` does not:

- **One root span per run.** `run_daily_report()` is wrapped in `@traceable`, so
  the subagents nest under it. Without that root you get four unrelated
  top-level traces and lose the single view worth having. The function's return
  value becomes the root span's output, so the day's status and finding counts
  are visible at the top of the trace without expanding anything.
- **Named spans.** Every subagent invocation passes `run_name`. Without it each
  one shows up as `LangGraph` and a four-agent trace is unreadable.
- **An explicit flush.** LangChain posts traces from a background thread, and a
  CI job exits the moment the script returns — with the last spans still
  queued. `tracing.flush()` runs in a `finally` block, because the run that
  crashed is the one whose trace you most want.

Every span carries the git SHA, run date, model, environment (`ci` / `local`)
and the thresholds in force. The SHA is the one that earns its place: when a
finding starts appearing every morning, it tells you which commit it started
after.

The trace URL is written into the report footer, the Actions step summary, and
any PR the code agent opens.

**It is entirely optional.** With no `LANGSMITH_API_KEY`, `configure()` sets
`LANGSMITH_TRACING=false` and every helper returns a harmless default. That
explicit off matters — tracing on with no key makes LangChain retry and warn on
*every* model call, which buries the actual output. `--no-trace` forces it off
even when the key is present.

Locally the keys come from `.env` at the repo root, loaded in
`agent/__init__.py` with `override=False` so a real environment variable always
wins:

```
GROQ_API_KEY=gsk_...
LANGSMITH_API_KEY=lsv2_...
LANGSMITH_PROJECT=transitpulse-ops-agent
```

```bash
make agent                              # trace URL is logged and in the report
python -m agent.supervisor --no-trace   # opt out for one run
```

`.env` is gitignored and never exists in CI, where the secrets arrive as real
environment variables instead — so the loader is inert there.

In CI, add `LANGSMITH_API_KEY` as a repository secret. On the EU instance also
set a repository **variable** `LANGSMITH_ENDPOINT` to
`https://eu.api.smith.langchain.com`; on the US instance leave it unset.

What to look at first when a report looks wrong:

| Symptom | Where to look in the trace |
| --- | --- |
| A finding you believe is false | The subagent's span → the tool call it cites → the raw tool output |
| A subagent reporting `degraded` with no findings | Its final span — usually a structured-output miss, and the raw text is in `facts` |
| The run hitting the daily token cap | Token counts per span; one tool returning a large payload is the usual cause |
| A subagent looping | Its span count against `AGENT_MAX_STEPS` |

## Token budget

Groq's free tier allows 30 requests/min, 12K tokens/min and 100K tokens/day on
`llama-3.3-70b-versatile`. The binding constraint is tokens, not requests,
because every agent turn resends the whole transcript including tool output.
Two things keep a run inside it:

- A shared `InMemoryRateLimiter` at 0.25 rps across **all** agents. Sharing one
  `ChatGroq` instance is load-bearing — a client per subagent would give each its
  own bucket and defeat the limiting entirely.
- Tools that return compact tables rather than raw API responses. A raw
  `get_metric_data` reply is a few thousand tokens; the same information
  summarised is about thirty.

A full run is roughly 40–70K tokens. If you start hitting the daily cap, drop
`AGENT_MAX_STEPS` or switch `AGENT_MODEL` to `openai/gpt-oss-120b`, which has
200K tokens/day.

## Deploying it

### 1. Create the read-only AWS role

`infra/modules/cicd/agent.tf` adds `github-actions-transitpulse-agent`, reusing
the OIDC provider the deploy role already uses. It is **not** the
PowerUserAccess deploy role.

```bash
make plan          # expect ~3 new resources, all IAM, no changes to the pipeline
make apply
cd infra && terraform output -raw agent_role_arn
```

> **The OIDC trap that costs an afternoon.** Repositories created after
> 2026-07-15 sign the `sub` claim in GitHub's *immutable* form, appending numeric
> IDs: `repo:owner@184206526/name@1326056479:ref:refs/heads/main`. Every tutorial
> and every Terraform example still shows the legacy `repo:owner/name:*` form,
> which cannot match it. STS then rejects the token with *"Not authorized to
> perform sts:AssumeRoleWithWebIdentity"*, the identical message it returns for a
> role that does not exist, so the trust policy reads as perfect while failing
> every single time.
>
> `infra/modules/cicd/main.tf` accepts both forms. Set `github_repo_immutable` in
> your tfvars, obtained with:
>
> ```bash
> curl -s https://api.github.com/repos/<owner>/<name> | python3 -c \
>   "import sys,json;d=json.load(sys.stdin);print(f\"{d['owner']['login']}@{d['owner']['id']}/{d['name']}@{d['id']}\")"
> ```
>
> The tell that you are hitting this rather than a bad ARN: `aws iam get-role
> --query Role.RoleLastUsed` stays empty, meaning the role has never once been
> assumed.

### 2. Add the two repository secrets

Settings → Secrets and variables → Actions → New repository secret:

| Secret | Value |
| --- | --- |
| `GROQ_API_KEY` | your Groq key |
| `AGENT_ROLE_ARN` | the `agent_role_arn` output from step 1 |
| `LANGSMITH_API_KEY` | optional — tracing is skipped cleanly without it |

### 3. Verify permissions before spending any tokens

```bash
make agent-install
make agent-tools     # calls every tool, no model in the loop
```

Every tool should print real numbers. `TOOL_UNAVAILABLE` names a missing IAM
permission — add it to `agent.tf` and re-apply.

### 4. Run it once locally, end to end

```bash
export GROQ_API_KEY=...
make agent
```

Read `reports/<today>.md`. If a subagent's findings do not match what you know to
be true, the prompt in `agent/subagents.py` is the thing to edit — not the code.

### 5. Merge the workflow to `main`

**Scheduled workflows only run from the default branch.** On `dev` the cron will
never fire; `workflow_dispatch` still works for testing.

```bash
git checkout -b feat/ops-agent && git add -A
git commit -m "feat: daily ops agent (LangChain + Groq) with read-only AWS role"
git push -u origin feat/ops-agent
gh pr create --base dev --title "feat: daily ops agent" --fill
```

Then merge `dev` → `main` as usual. The first scheduled run is the next 03:30 UTC.

## Operating it

```bash
gh run list --workflow=agent-daily.yml            # recent runs
gh workflow run agent-daily.yml                   # trigger one now
```

Thresholds live in the workflow's `env:` block, so changing one is a YAML edit,
not a code change. Defaults: `$1.50`/day, `$45`/month projected, 21-day
collection target.

Two things worth knowing:

- **`COST_FILTER_BY_TAG` defaults to `false`** and should stay there until the
  `Project` tag is activated under Billing → Cost allocation tags. A filter on an
  inactive tag matches nothing and returns `$0.00`, which would have this agent
  report a runaway bill as healthy. Activation does not backfill.
- **Cost Explorer only answers in `us-east-1`.** Called in `ca-central-1` it
  returns an empty result set rather than an error — same failure, same
  consequence. `agent/tools/cost.py` pins the region.

## Cost of the agent itself

Groq free tier is $0. LangSmith bills per trace and this produces one a day, so
it stays comfortably inside the free Developer tier. GitHub Actions is free on public repos; on a private repo
this is about 5 minutes/day against the 2,000-minute monthly allowance. The AWS
calls are metric reads and a handful of small Athena queries — well under a cent
a day, and the Athena queries are capped by the workgroup's byte limit.

## Limitations

- **Reports are only as good as the baselines in the prompts.** When the pipeline
  changes shape — a new feed, a different poll rate — `_SHARED_CONTEXT` in
  `agent/subagents.py` has to change with it, or the agent grades against a
  world that no longer exists.
- **No memory between runs.** Each day is judged on its own 24-hour window, so a
  slow multi-week drift that never breaches a threshold on any single day will
  not be caught. Reports accumulate in `reports/` for a human to read across.
- **The code agent is the weakest link, by construction.** It proposes; most
  proposals should be closed unreviewed. It cannot touch Terraform, IAM or the
  silver merge key at all.
- **A missing report is silent.** Nothing currently alerts when the workflow
  itself fails to run. The cheapest fix is a scheduled-job monitor pinging on
  absence rather than on failure.
