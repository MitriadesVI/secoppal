# Natural Alpha V1 / A0–A1

The runtime observes at the seam between explicit handlers and normal flow.
It proposes no executable parameters. The existing parser, QueryPlan and SoQL
keep authority. A valid proposal with at least two jobs in Alpha returns a
host-authored stepwise clarification. Shadow always continues legacy.

The accepted dogfood scope is Rodrigo authenticated in Streamlit, only for
turns reaching this seam. Explicit fast paths are intentionally excluded;
Profile is provisionally excluded and its coverage gap must be reported.
There is no global multi-job detection or blocking guarantee. The accepted
scope is recorded in [ADR-096](../../docs/decisions/2026-09-13_ADR-096_natural-alpha-scoped-dogfood.md);
documentation does not activate the feature.

`SECOPPAL_NATURAL_ALPHA_MODE=off|shadow|alpha` defaults to `off`; every other
value is off. `SECOPPAL_NATURAL_ALPHA_EMAIL` is one exact normalized email.
Both observing modes require auth enabled, Streamlit, an authenticated identity
and the matching email. Admin status grants no eligibility. Fast paths run
before the seam. No mode is activated by this package.
The authenticated identity receipt is bound to its emitted object; shallow
copies, deep copies, plain mappings and JSON roundtrips do not retain authority.

The separate DeepSeek Flash Responses provider uses temperature 0, reasoning
none, JSON object output, no tools, one call, no retries, and its own timeout
(`SECOPPAL_NATURAL_ALPHA_TIMEOUT_SECONDS`, default 10 seconds, maximum 45).
After observation, a nested deadline gives legacy its full original 15 seconds.
The original context still measures end-to-end time; the nested deadline and
its telemetry are restored in `finally`. Only observed turns have additive
latency.
The deadline and planner authorities remain unchanged. The existing coordinator
has a minimal terminal-publication hook: it persists owner NONE on a private
result copy, without returning continuation references. Recovery failures are
surfaced as `NaturalAlphaPublicationError` with the original cause preserved.

Feedback adds explicit nullable columns through an idempotent additive migration
in the existing DB. Jobs store action, aspect and half-open raw code-point
intervals. No prompt, output JSON or repeated evidence text enters runtime
telemetry. Legacy traces receive metadata before their single log operation;
multi-job records one trace without a pending plan. Existing ratings still use
the trace ID. Streamlit projects host labels under “Alfa entendió” and only
host-resolved parameters/route under “SECOPPAL resolvió”.

The 40-case smoke corpus has 30 single-job and 10 composite queries. Gold is
explicitly a `PROVISIONAL_ORACLE`, authored before live and awaiting independent
adjudication; a numerical pass cannot authorize activation. `manifest.json`
pins corpus, gold, prompt, schema, provider contract and evaluator. Do not tune
them after viewing model outputs. No historical holdout is used.
The historical live run has 30/30 single classifications and 10/10 multi recall;
composite exact classification is 9/10. C06 returned FOLLOW/RUN where the
provisional gold expects FOLLOW/SAVE. It remains a known model edge for dogfood,
without prompt/gold/corpus changes or a claim of global runtime coverage.

From this worktree, using the canonical interpreter:

```sh
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/run_natural_alpha_v1.py --live --env-file /Users/rodrigoortiz/Documents/secoppal/.env --output experiments/natural_alpha_v1/live.json
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/run_natural_alpha_v1.py --replay experiments/natural_alpha_v1/live.json --output experiments/natural_alpha_v1/replay.json
```

Live refuses uncommitted frozen inputs or an existing output path. A provider
400 stops the run as `PROVIDER_CONTRACT_INCOMPATIBLE`; never remove temperature
or alter the schema to recover it silently. Missing credentials report
`PROVIDER_NOT_RUN`. Replay requires both call key and exact request equality,
consumes every record once and makes zero calls. Synthetic perfect-output tests
validate evaluator mechanics only; they are not model quality evidence.

No merge, push, deploy, restart or dogfood activation is performed here.
Rollback is `SECOPPAL_NATURAL_ALPHA_MODE=off`. HERA owns the global gate,
candidate freeze, independent review and the decision on 50 real dogfood turns.

## Read-only scoped checkpoint report

```sh
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/report_natural_alpha_dogfood.py --db /explicit/path/secoppal.db --email allowed@example.test --since 2026-09-13T00:00:00-05:00
```

The CLI does not construct Settings or open the source path with SQLite.
The authenticated operator must pass the trusted local feedback store via
`--db`; its directory and pre-existing files must not be controlled by a local
adversary. Descriptor binding and the verified snapshot guarantee consistent
acquisition and no source writes or new source sidecars. They do not
authenticate the provenance of regular bytes already present: compatible
hardlinks and byte copies are indistinguishable without an external authority.
Resistance to a malicious local filesystem is a separate design requirement
for store ownership/ACLs or authenticated store evidence, not a link-count
check in this reporter.
It makes a stable byte snapshot of the DB and its WAL, when present, in an
automatically removed temporary directory; SHM is never copied. Streaming
SHA-256 fingerprints and device/inode/size/mtime/ctime witnesses must agree
before and after copying, the DB/WAL file set must remain unchanged, and copied
bytes must match their source digests. DB and WAL must be regular files, not
symlinks. `lstat` is bound to an open descriptor with `O_NOFOLLOW` where supported;
descriptor device/inode/type and metadata are rechecked before, during and after
streaming. Copies come from those retained descriptors, never a reopened source
path. Symlinks and non-regular files fail explicitly, not as empty reports.
Appearance/disappearance of WAL or a changed regular-file witness retries from
a fresh temporary directory, at most three times; continued instability fails
explicitly as `SNAPSHOT_UNSTABLE`. A rollback journal's presence is conservatively
rejected, including apparently idle ones.

Only the verified temporary copy is opened with SQLite `mode=ro`. SQLite may
create its own SHM there; cleanup removes the copy and all its sidecars on
success or error. The report does not change source bytes, size, mtime or ctime,
and does not create or modify source sidecars. Reading may update atime depending
on the filesystem: atime and absolute metadata immutability are not guaranteed.
The report requires the trace/identity/Alpha columns and fails explicitly if they
are missing. It prints JSON to stdout. Email and raw query are not included;
raw query is never selected.
`--review-queue` adds only trace ID, timestamp, host route/status and rating.

The window includes the selected email's Streamlit traces from `--since`,
ordered by UTC timestamp then trace ID, through the Nth reached-Alpha turn
inclusively (`--checkpoint`, default 50). Until N is reached, it includes all
available selected traces and `checkpoint_ready=false`. Later turns do not
enter a completed checkpoint. `window_end` identifies its last trace/time.

Each eligible `run_query` turn opens one opaque host receipt before admission
and `coordinate_turn`. Its first trace — or one trace-only record when no route
traced — carries `natural_alpha_coverage_turn_id` and the host class
`REACHED_ALPHA`, `PROFILE_BYPASS`, `CONTROL_BYPASS`, `FAST_PATH_BYPASS`,
`OTHER_BYPASS` or `UNACCREDITED`, assigned from the seam witness, structured UI
inputs and result witnesses, never from query text. Later legacy traces of the
same turn count as `secondary_traces_window`. Only a confirmed SQLite commit
credits a turn; a JSONL append does not. A missing commit logs
`natural_alpha_coverage_persistence_failed stop_dogfood=true` and writes no
replacement trace: stop the dogfood. A turn that raises logs
`natural_alpha_coverage_turn_aborted stop_dogfood=true`. `UNACCREDITED` must be
0: the report fails closed with `accounting_valid=false`, null
`coverage_gap`/`coverage_rate`, `checkpoint_ready=false`, and the CLI exits 3
after printing. Valid Alpha columns without a receipt are never reached samples.

Structured UI inputs (profile action, actor candidate selection and seeded
refinement chips via `seed_resolved_params`) are controls, not language work,
unless the turn reached Alpha. FAST_PATH uses a closed list of host route
families: an unlisted pre-seam route is still accredited, as `OTHER_BYPASS`.

Before validating a checkpoint the operator must:

1. confirm the application logs for the window contain no
   `natural_alpha_coverage_persistence_failed` and no
   `natural_alpha_coverage_turn_aborted`; such turns are absent from SQLite and
   the report cannot see them;
2. run the report with `--since` later than the deploy/activation of this
   version; earlier traces carry no receipt and make the window fail closed.

`traffic_turns_window` counts turns; `natural_work_turns_window` is the language
denominator (REACHED_ALPHA + FAST_PATH + PROFILE + OTHER); controls are separate
and `eligible_turns_window` adds them. `reached_seam` counts mode alpha +
recorded eligibility 1 + nominal status agreeing with the receipt. `coverage_gap`,
`coverage_rate` and exact `excluded_by_route` expose Profile/fast-path gaps
without interpreting Spanish or changing routing. Ratings, utility and latency
statistics concern reached-Alpha turns only. Utility divides positive by rated
turns and is null without ratings; rating coverage is reported separately.
Average and nearest-rank p95 use observed finite latencies; missing denominators
or samples yield null. Checkpoint readiness is sample volume, not an automatic
dogfood approval or proof of semantic correctness, parity or side-effect safety.
