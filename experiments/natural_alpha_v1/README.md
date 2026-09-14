# Natural Alpha V1 / A0–A1

The runtime observes at the seam between explicit handlers and normal flow.
It proposes no executable parameters. The existing parser, QueryPlan and SoQL
keep authority. A valid proposal with at least two jobs in Alpha returns a
host-authored stepwise clarification. Shadow always continues legacy.

`SECOPPAL_NATURAL_ALPHA_MODE=off|shadow|alpha` defaults to `off`; every other
value is off. `SECOPPAL_NATURAL_ALPHA_EMAIL` is one exact normalized email.
Both observing modes require auth enabled, Streamlit, an authenticated identity
and the matching email. Admin status grants no eligibility. Fast paths run
before the seam. No mode is activated by this package.

The separate DeepSeek Flash Responses provider uses temperature 0, reasoning
none, JSON object output, no tools, one call, no retries, and its own timeout
(`SECOPPAL_NATURAL_ALPHA_TIMEOUT_SECONDS`, default 10 seconds, maximum 45).
After observation, a nested deadline gives legacy its full original 15 seconds.
The original context still measures end-to-end time; the nested deadline and
its telemetry are restored in `finally`. Only observed turns have additive
latency. No deadline, planner or turn coordinator implementation is changed.

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
