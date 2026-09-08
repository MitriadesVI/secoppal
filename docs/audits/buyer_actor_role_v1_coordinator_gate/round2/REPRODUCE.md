# Reproducir la ronda 2 del gate Buyer–Actor V1

GATE_CODE_HEAD: `f864b3b25e46943f79a247bf3f1314db359cdd1d`.
Rama: `integration/buyer-actor-role-v1-gate`.
Worktree: `/Users/rodrigoortiz/Documents/secoppal-buyer-actor-role-v1-gate`.
BASE / MAIN_BEFORE: `52a6bcc68efc50e06ba9aa0f60ff8d30f8cf1386`.
Runtime auditado: `e06c60482756e2d4238052818117568f9cd4e946`.

La ronda 1 vive en el directorio padre y **no se sobrescribió**. Esta ronda usa
`--output-dir`, que el adapter ahora respeta también en modo `full`; el destino
sigue obligado a quedar dentro del directorio del gate.

```sh
PY=/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python
RUN=docs/audits/buyer_actor_role_v1_coordinator_gate/run_gate.py
DEST=docs/audits/buyer_actor_role_v1_coordinator_gate/round2
$PY "$RUN" collect --output-dir "$DEST"
$PY "$RUN" full    --output-dir "$DEST"
$PY "$DEST/reconcile.py"          # resultados por caso, skips/xfails por procedencia
```

Antes de lanzar `full`, comprobar capacidad: la corrida consume del orden de
cientos de MiB de temporales que **no se liberan hasta terminar cada componente**.
Con menos de 1 GiB libre no se lanza (ver `CAPACITY.md`). La ronda 1 abortó por
ENOSPC con 438 MiB.

## Componente `buyer_local`

`environment` habilita `BUYER_ACTOR_LOCAL_INDEX` cuando `buyer_local` aparece en el
nombre del componente —substring, no sufijo—, de modo que una repetición focal
también ejecuta los ocho opt-in en vez de saltárselos en silencio:

```sh
$PY "$RUN" focal tests/test_buyer_actor_role_local_v1.py \
  tests/test_buyer_actor_independent_local.py \
  --run-id buyer_local_after --output-dir "$DEST"
```

En la suite global esos ocho casos aparecen como `skipped`: el índice sólo se
habilita en su componente. No son cobertura perdida.

## Adjudicación H4

`round2/h4_adjudication/` contiene los scripts y las salidas de ambos runtimes.
`h4_diagnose.py` ejecuta los dos turnos y los dos controles inversos con transporte
sintético y registro externo de red comprobado **después** del producto;
`h4_control_clean.py` ejecuta la consulta aislada en un workflow limpio.
`run_h4_diag.py` los lanza con el `environment` del adapter.

Para el lado BASE se copia el worktree, se restituye `app/` desde los blobs de
`52a6bcc` —incluida la eliminación de `app/core/buyer_actor_role.py`, que no existe
en BASE— y se comprueba árbol contra árbol antes de ejecutar. El test H4 y la
configuración de pytest permanecen idénticos; no se corre la suite BASE.

## Controles por mutación

`round2/mutation/MUTATION.md`. Ambos se hacen en una copia desechable, nunca en el
candidato, y la copia se elimina tras extraer su evidencia. No repetir campañas de
mutación de runtime ya aprobadas.
