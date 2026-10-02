# M2A — Enmienda R3, paso B4: procedimiento de re-sellado (release r3)

**Estado:** procedimiento escrito en B1; **no ejecutado**. **Rama:** `mission/m2a-contract-enm-r3` (mismo worktree).
**Contrato:** `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md`, con la enmienda R3 (Anexo D). **Casos:** SF192–SF195
(U3), entrada en `docs/audits/m2a_conversation/labeling_r3/R3_LABELER_INPUT.md`.

En la rama, el sello r2 está roto desde B1, y sólo por el sha256 del contrato (`interpreting_rules`). B4 lo rehace
**una sola vez**, con los cuatro casos dentro (D-4). B4 no toca `app/`: lo que falta en el tipo es la fase 1c de M2A-3,
después del re-sellado, y el barrido lo marca como xfail estricto.

## 0. Precondiciones (sin ellas, no empieces)

1. **Etiquetas ciegas.** Dos JSONL, uno de A y otro de B, con una etiqueta por caso en la forma `label_X` del esquema
   2.1. Los etiquetadores no vieron la propuesta (`M2A_CONTRACT_ENM_R3_PROPOSAL.md`), la matriz ni el test del barrido.
2. **Adjudicación del usuario** de toda discrepancia entre A y B: disposición, código, `ops`, `resulting_ir` y
   `alternatives`. Una adjudicación puede sacar un caso; si lo hace, actualiza los conteos en B4.2.
3. **Anexo D.2 resuelto** (2026-10-02; B1-fix): D-1 con la opción 1 y D-6 aprobado. Si después de etiquetar surge otra
   decisión de producto que toque el contrato, se aplica en B4.1.
4. **Árbol limpio.** `git status` vacío y `git log` revisado (otro agente puede haber commiteado en la rama).
5. **Directorio REAL privado:**
   `~/Documents/secoppal-asesoria-2026-10-01/privado_no_git/gold_followups_real/GOLD_CANDIDATE/` (en adelante `$PRIV`).

## 1. Pasos

### B4.1 Cerrar el texto del contrato (antes de cualquier hash)

- D.2 ya está resuelto. Aplica aquí sólo lo que decida la adjudicación.
- Si la adjudicación sacó algún caso, ajusta la cabecera (`154` / `203`), §20.1 gate 1 y el Anexo B.
- Desde aquí el contrato **no se toca**: su sha256 entra en el sello en B4.6.
- Re-ejecuta `tests/test_m2a_contract_r3_sweep.py`. Los pines leen el texto: si cambió el contrato, ajusta los pines
  del lado del contrato (nunca los xfail del lado del tipo).

### B4.2 Registrar la adjudicación

- **`docs/audits/m2a_conversation/gold/ADJUDICATION_R3.md`** contiene:
  - la decisión del usuario (U3) y el motivo (enmienda R3, Anexo D);
  - el sha256 de los dos JSONL de A y B;
  - por caso: discrepancias, decisión y digest del registro final;
  - el lado DEV/TEST de cada id nuevo (B4.6).

  Sigue el formato de `ADJUDICATION_2026-10-02.md`.
- **`docs/audits/m2a_conversation/gold/ADDITIONS_R3_synthetic.jsonl`** lleva un registro por caso, con las claves de
  nivel de registro del Gold sellado: `adjudicated_by: ["U3"]`, `ambiguous_by` (los etiquetadores que marcaron
  ambigüedad), `expected_disposition` (la adjudicada), `gold_id`, `label_A`, `label_B`, `m2a_scope` (adjudicado),
  `primary_agreement` (si A y B coincidían **antes** de adjudicar) y `slice: "SYNTHETIC_FOLLOWUP"`.
  - Con A = B, las etiquetas entran literales.
  - Con discrepancia, sigue el precedente de U1 (`build_sealed_gold.py:u1_sf166`): los campos disputados se reescriben
    con la decisión en **las dos** etiquetas, y la nota anterior queda como `[SUSTITUIDA por U3] …`.
  - Comprueba que `followup_sha256 == sha256(followup_text)` y que `prior` es byte a byte el de
    `R3_LABELER_INPUT.md`.

### B4.3 `build_sealed_gold.py`: mecanismo de adiciones

Hoy el script sólo parchea ids existentes: falla si un id parcheado no está, y no actualiza ni `ids` ni `n` del índice.
Cambios mínimos:

1. `ADDITIONS = DST / "ADDITIONS_R3_synthetic.jsonl"`, leído con `load`. Ningún id puede existir en el candidato GOLD
   ni en el FRONTIER; si existe, `SystemExit`.
2. `out = sorted(patched + additions, key=gold_id)`. El cuerpo sigue saliendo de `dump` (claves ordenadas).
3. En el índice, slice `SYNTHETIC.M2A_GOLD`: `ids` (ordenados), `n`, `by_disposition` y `sha256`. Además
   `idx["additions"] = {gid: sha256(registro)}`, `idx["release"] = "M2A_GOLD r3 (…)"`, y `rules` / `note` con U3 y
   `gold/ADJUDICATION_R3.md`.
4. El docstring nombra U3.
5. Ejecuta `build_sealed_gold.py` y después `build_sealed_gold.py --check` («reproduces byte for byte»).

### B4.4 Verificador (`check_m2a_gold_contract.py`)

1. `REASONS` += `DELTA_SLOT_CONFLICT`, `KEEP_ON_EMPTY_SLOT`, `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER`.
   `REASON_PREFIXES` += `UNSUPPORTED_PROPOSAL:`.
2. Alinea la cola de `UNRESOLVED_VALUE:` con A.3: sólo claves de dimensión de ranura y `UNIVERSE`, nunca otra. Hoy
   acepta cualquier cola, y el tipo y el contrato no.
3. **G9, coherencia de `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER`** (contrato §6.7):
   - `prior.ir` tiene `order` y `cardinality`;
   - las ops retiran ORDER sin `CLEAR(CARDINALITY)` ni `SET_ORDER`;
   - `resulting_ir` es `null`;
   - las `alternatives` son las de D-1 (opción 1):
     - exactamente una sin `order` ni `cardinality` (C1);
     - las demás conservan la `cardinality` del preestado y tienen un `order` de **otra** clave que la vigente, una
       por clave;
     - ninguna conserva el `order` vigente.

     El verificador no conoce las claves que la autoridad ofrece por dataset, así que no exige completitud por clave;
     la completitud la fija la adjudicación. El Gold etiqueta lo normativo, no el provisional sin E-ORDER.
4. Corre `check_m2a_gold_contract.py --real-dir $PRIV --json docs/audits/m2a_conversation/gold/REPRESENTABILITY_REPORT.json`:
   `"violations": 0`.

### B4.5 Mutantes (`mutate_check_m2a_gold_contract.py`)

Añade mutantes sobre los casos nuevos, cada uno muerto por **su** regla:

| Mutante | Debe morir por |
|---|---|
| `R3_SF192_executed_keeps_cardinality`: EXECUTE con `resulting_ir` sin `order` y con `cardinality` | `check_ir` «cardinality without order» |
| `R3_SF192_executed_drops_cardinality`: EXECUTE sin las dos y sin `CLEAR(CARDINALITY)` | G3 «cardinality changed without an op» |
| `R3_SF192_reason_outside_A3`: un código que no está en A.3 | G1 |
| `R3_SF192_alternative_keeps_current_order`: una alternativa con el `order` vigente | G9 nuevo (B4.4.3) |
| `R3_SF192_alternative_drops_cardinality_with_new_order`: una alternativa `SET_ORDER` sin la `cardinality` del preestado | G9 nuevo (B4.4.3) |
| uno por cada caso SF193–SF195, según la disposición adjudicada (p. ej., si SF195 ejecuta: «pierde la CARDINALITY sin op») | la regla que corresponda |

`mutate_check_m2a_gold_contract.py --scratch-dir <fuera del repo>` ⇒ `ALL_KILLED True`.

### B4.6 Sello (`m2a_gold_seal.py`) y partición

1. Edita el sello:
   - `ADJUDICATIONS["U3"] = "R3: SF192–SF195, etiquetado ciego A/B y adjudicación del usuario"`;
   - `RULE_FILES` += `gold/ADDITIONS_R3_synthetic.jsonl` y `gold/ADJUDICATION_R3.md`;
   - la nota de `adjudications`, con U3;
   - `sealed_on` y `base_commit` (hoy fijos en `2026-10-01` / `8c1f8c54`): fecha del r3 y base de la rama.
2. `m2a_gold_seal.py build --real-dir $PRIV`. La partición es pegajosa: los ids existentes conservan su lado, y los
   nuevos reciben el suyo por la regla del sello (estrato = slice + disposición adjudicada; orden `sha256(sal:id)`;
   posición par → TEST, impar → DEV).
3. **Antes de mover el pin**, demuestra que ningún id existente cambió de lado:

   ```bash
   git show HEAD:docs/audits/m2a_conversation/gold/M2A_GOLD_SEAL.json > /tmp/seal_r2.json   # o el scratchpad
   python - <<'PY'
   import json
   old = json.load(open("/tmp/seal_r2.json"))["partition"]
   new = json.load(open("docs/audits/m2a_conversation/gold/M2A_GOLD_SEAL.json"))["partition"]
   side = lambda p: {i: s for s in ("DEV", "TEST") for ids in p[s].values() for i in ids}
   o, n = side(old), side(new)
   moved = sorted(i for i in o if n.get(i) != o[i])
   added = {i: n[i] for i in sorted(set(n) - set(o))}
   print("moved", moved, "added", added)
   assert not moved and set(added) == {"SF192", "SF193", "SF194", "SF195"}
   PY
   ```

   El lado de un id nuevo depende de su disposición y de los demás ids nuevos de su estrato. Por eso se calcula aquí,
   con los cuatro dentro, y se copia a `ADJUDICATION_R3.md`.
4. Actualiza `PARTITION_SHA256` en `scripts/audits/m2a_conversation/m2a_gold_seal.py` **y** en
   `tests/test_m2a_gold_seal.py`, con el digest de la partición nueva, y declara en el commit «0 movimientos; 4
   altas».
5. `m2a_gold_seal.py verify` y `m2a_gold_seal.py verify --real-dir $PRIV`: `seal OK`.

### B4.7 Tests y pines

- **`tests/test_m2a_gold_seal.py`:**
  - totales `{"REAL": 49, "SYNTHETIC": 154, "TOTAL": 203}`;
  - `test_gold_is_representable_by_the_contract` → `{"gold": 154, "frontier": 41}`;
  - el pin de la partición;
  - en `test_sealed_gold_reproduces_from_candidate_and_adjudications`, una aserción U3 (los cuatro ids presentes, con
    `adjudicated_by` ⊇ `{"U3"}`).
- **`tests/test_m2a_contract_r3_sweep.py`:** sin cambios en el lado del tipo (los xfail estrictos siguen hasta M2A-3
  1c).
- **Matriz de trazabilidad:** en la columna «Casos Gold» de T14, T15, P11 y M10, sustituye el marcador «se asignan en
  B4» por los ids adjudicados.

### B4.8 Gates (resultado literal en el cierre)

- `build_sealed_gold.py --check`.
- `m2a_gold_seal.py verify`, con y sin `--real-dir`.
- `check_m2a_gold_contract.py --real-dir $PRIV`: 0 violaciones.
- `mutate_check_m2a_gold_contract.py --scratch-dir …`: `ALL_KILLED True`.
- `privacy_scan_public_tree.py $PRIV <copia de los archivos del commit>`: sin bloqueantes.
- `pytest` focal: `tests/test_m2a_gold_seal.py`, `tests/test_m2a_contract_r3_sweep.py` (con sus xfail estrictos),
  `tests/test_foundation_accepted_contract.py`, `tests/test_m2a_boundaries.py`,
  `tests/test_foundation_authority_contract.py` y `tests/test_m1_wave1_parity_pins.py`.
- `ruff` (configuración del repo) sobre los `.py` tocados.
- `olympus-semgrep-delta --base fa0b6de8` sobre los `.py` tocados.

Commit por rutas explícitas y árbol limpio. Después: candidato congelado y ZEUS fresco, sin la narrativa de la
implementación. Lo decide el orquestador.

## 2. Después de B4: secuencia de la ola (usuario, 2026-10-02; `M2A_WAVE1_DECISIONS.md` U17)

R3 → etiquetado ciego A/B → adjudicación → **re-sellado único (B4)** → ZEUS → **M2A-3 fase 1c** → regenerar el
oráculo M2A-8a0 → ZEUS del oráculo (**GO requerido**) → congelar el oráculo → `compose`. `compose` queda bloqueado hasta
ese GO.

- **M2A-3, fase 1c:** el tipo (`CLARIFY_CODES`, `<DIM>` con `UNIVERSE`, `TurnScope.OUT_UNSUPPORTED_PROPOSAL` →
  `INFORM`, `UNSUPPORTED_PROPOSAL:<motivo>`), con el test de A.3 de `tests/test_foundation_accepted_contract.py`. Cada
  ítem convierte su xfail del barrido en XPASS estricto (rojo), y se quita su marca en el mismo commit. El gate
  `CONTRACT_COMPLETE_FOR_M2A3` (INV-M2A-25 satisfecho por el barrido: sin filas sin respuesta y sin xfail) se cumple
  al cerrar 1c.
- **M2A-8a0:** el oráculo se regenera **después** de 1c. Su manifiesto fija el `gold_seal_sha256` r2, y valida el
  vocabulario con los `REASONS` del verificador sellado (B4.4.1).
