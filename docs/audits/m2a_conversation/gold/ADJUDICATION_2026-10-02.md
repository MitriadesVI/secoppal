# M2A_GOLD r2 — adjudicaciones del 2026-10-02 (antes del freeze)

**Origen:** instrucción del usuario del 2026-10-02 sobre el candidato `3aac2a20`, tras la observación ZEUS R2
(P2-1 SF166, P3-1 SF063/SF168). Aplicadas de forma reproducible por
`scripts/audits/m2a_conversation/build_sealed_gold.py` sobre el candidato asesor importado, que **no se modifica**
(`docs/audits/m2a_advisory_2026-10-01/02_gold_followups/gold_candidato/`, evidencia fijada por `IMPORT_MANIFEST.sha256`).
La copia sellada vive en este directorio. Ningún registro REAL cambia. Totales sin cambio: 49 + 150 = 199 en
`M2A_GOLD`, 16 + 41 = 57 en `FOLLOWUP_FRONTIER`.

| Id | Registro | Antes | Después | Regla | Digest antes → después |
|---|---|---|---|---|---|
| U1 | SF166 («y en 2023?» sobre un ancla con 500 SMMLV) | `EXECUTE`, recalcula 500 SMMLV sobre 2023 | `CLARIFY SMMLV_YEAR_AMBIGUOUS`, `resulting_ir = null`, la operación TIME `unresolved = true`, sin efectos ni eco; la nota anterior queda marcada `[SUSTITUIDA por U1]` | ADR-103 D10: el catálogo es 2024–2026; un año no catalogado aclara, nunca usa el salario de otro año (contrato §15.5) | `75c251a4…` → `77aa1c30…` |
| U2 | SF063 (pendiente con `asks = 3`) | preestado inalcanzable (`asks = 3`, dos `PROTOCOL:reask`) | `asks = 2`, una `PROTOCOL:reask`; disposición y etiquetas sin cambio | contrato §8.1: `asks ∈ {1, 2}`; al fallar con 2 se cancela | `42c84f7d…` → `2fdd2688…` |
| D1 | SF168 (ancla ejecutada con SMMLV sobre 2023–2025) | sin declaración | declarado expresamente **prueba de estado persistido inválido** (`invalid_persisted_state`, `adjudicated_by: ["D1"]`); etiquetas sin cambio | contrato §15.5 (ese ancla no es alcanzable) y §6.6 (la re-decisión sobre la efectiva falla cerrado) | `996694e4…` → `4bf00817…` |

## Por qué

- **U1.** SF166 no obliga a ampliar el catálogo SMMLV a 2023: es un **control de no extrapolación**. Añadir 2023 sería
  una decisión de producto separada y futura (D-MONEY / E-VALUE), que exigiría re-adjudicar SF166 y re-sellar.
- **U2.** Un caso del Gold no puede partir de un estado que el contrato no puede producir, salvo que se declare como
  prueba de estado persistido inválido. Para SF063 se normaliza: lo que el caso prueba (rechazo citado + consulta
  autosuficiente cancela la pendiente) no depende del número de re-preguntas.
- **D1.** SF168 sí se conserva como prueba de estado persistido inválido: su valor es justamente que un ancla que el
  contrato vigente no produciría (p. ej. escrita por una versión anterior) se re-decide y falla cerrado. Es la
  excepción que la instrucción del usuario prevé; HERA la declara y la deja visible para el usuario.

## Comprobación

`check_m2a_gold_contract.py` (G11) marca exactamente SF063 (`asks = 3`), SF166 (SMMLV ejecutado fuera del catálogo) y
SF168 (ancla SMMLV plurianual sin declarar) sobre el candidato sin parchear, y 0 violaciones sobre la copia sellada.
`build_sealed_gold.py --check` reproduce la copia byte a byte.

## Partición DEV/TEST

La partición del sello es **fija** desde este release: un id conserva su lado (DEV o TEST) en todo re-sellado posterior
y sólo los ids nuevos se asignan por la regla estratificada. Se conserva la partición del sello anterior (`3aac2a20`):
U1 cambia la disposición de SF166 pero **no mueve ninguna asignación** (SF166 sigue en su lado; 0 movimientos REAL y 0
sintéticos). Una regla posicional habría movido 106 asignaciones sintéticas; con la partición fija, una re-adjudicación
posterior a iterar sobre DEV no puede contaminar TEST.

## Lo que fija el sello

Además de la copia sellada, el sello liga los bytes del candidato asesor (`gold_candidato/*.jsonl`, índice) y comprueba
`IMPORT_MANIFEST.sha256` completo (`gates.import_manifest_ok`).
