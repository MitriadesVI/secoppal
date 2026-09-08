# Buyer–Actor V1 — recibo de integración

`git merge --ff-only ceecac7b75d8f8d95f02a634031029e52b1d4a2e`, fast-forward puro:
no se creó ningún commit de merge (`git log --merges 52a6bcc..HEAD` vacío).

| Referencia | SHA |
|---|---|
| MAIN_BEFORE | `52a6bcc68efc50e06ba9aa0f60ff8d30f8cf1386` |
| **MAIN_AFTER** | **`ceecac7b75d8f8d95f02a634031029e52b1d4a2e`** |
| GATE_CODE_HEAD probado | `f864b3b25e46943f79a247bf3f1314db359cdd1d` |
| Runtime auditado | `e06c60482756e2d4238052818117568f9cd4e946` |
| Runtime probado | `aab5d41d45e58f21b727929d4ec454f8fbbbf9cd` |

## Equivalencia, no repetición

La suite **no** se repitió tras el fast-forward. Se comprobó equivalencia:

- `main^{tree}` == `ceecac7^{tree}` == `eb8bf8a25804c0c5acfafdab63e26b278d544c1c`.
- Entre `f864b3b` (código probado) y `ceecac7` (HEAD integrado) el delta está
  **íntegramente bajo `docs/audits/`**: evidencia y documentación, ningún archivo
  de código, test o helper alcanzable por la suite. La colección sigue en 13.312.
- `git diff e06c604..main -- app/ streamlit_app.py` está vacío: el runtime en main
  es byte a byte el auditado.

## Preservación comprobada por hash

| Elemento | Antes | Después |
|---|---|---|
| `.env` (sha256, 24 primeros) | `95bf30538e5d3de77eaf1c3a` | `95bf30538e5d3de77eaf1c3a` |
| `data/` | 689.356 KiB, 10 entradas | 689.356 KiB, 10 entradas |
| Archivos no versionados | 239 | 239 |

No se tocaron otros worktrees ni sus untracked, ni metadatos Git más allá de mover
`main`. No hubo push (el repositorio no tiene remotos), despliegue, cambio de flags,
arranque ni reinicio de aplicaciones.

## Incidente durante la comprobación previa, y su reparación

Al comprobar si algún archivo local impedía el fast-forward se ejecutó
`git merge --ff-only --no-commit <sha> --no-ff`, con banderas contradictorias: Git
resolvió por `--no-ff --no-commit` y **realizó un merge real detenido antes de
commitear**, en vez del ensayo pretendido. `git merge --abort` falló con
`Entry 'app/core/actor_arbitration.py' not uptodate`.

Se reparó sin destruir nada y sin `reset`. Se comprobó primero que el árbol y el
índice ya eran **byte-idénticos a `ceecac7`** —el destino autorizado—
(`git diff ceecac7` y `git diff --cached ceecac7`, ambos vacíos), y que `.env`,
`data/` y los 239 untracked seguían intactos. Sólo entonces se retiraron los
archivos de estado del merge (`MERGE_HEAD`, `MERGE_MSG`, `MERGE_MODE`,
`AUTO_MERGE`) y se ejecutó el `git merge --ff-only` autorizado, que completó
limpiamente. El resultado es el fast-forward previsto, no un merge: HEAD, índice
y árbol coinciden con el candidato probado y no existe commit de merge.

Nunca hubo commit intermedio en `main` ni pérdida de contenido; el estado
intermedio fue un índice de merge sin commitear sobre `MAIN_BEFORE`.

## Alcance

Integración, **no activación**. No se emite PRODUCTION_GO. No se activó el default
ON del Frame ni su botón: ese cambio sigue separado y sin validar aquí.

[Gate completo, adjudicación y límites](../round2/COORDINATOR_GATE.md) ·
[Capacidad](../round2/CAPACITY.md) · [Mutaciones](../round2/mutation/MUTATION.md) ·
[Reproducción](../round2/REPRODUCE.md)
