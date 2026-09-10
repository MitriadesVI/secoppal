# Integridad de los fixtures corregidos — R5

Dos fixtures se movieron. Ninguna línea de runtime. Este documento declara
exactamente qué cambió y qué quedó intacto, para que la corrección no pueda
esconder un ajuste de producto.

## C03 — `SqlProfileClient` en `tests/test_beta_entity_actor_conversation.py`

### Lo que cambió

| Cambio | Por qué |
|---|---|
| `register_dates(conn)` | El SoQL emitido llama `date_extract_y`, dialecto de Socrata. El transporte SQLite no lo define y la conexión moría con `OperationalError`, que el perfil reporta como «la fuente no respondió». Se reutiliza `tests/contract_universe_sql_dates.py`, la emulación ya acreditada en F2.1, la remediación y las auditorías CU V1.1 — no una función nueva. |
| `row[spec.state] = CELEBRATED_STATE` en filas `jbjy-vk9h` | El predicado de universo celebrated exige evidencia de pertenencia. Las filas nacían con `estado_contrato` NULL, así que ninguna entraba. El estado es `"En ejecución"`, uno cualquiera de `CELEBRATED_STATES`; el perfil nunca lee la etiqueta de vuelta. |
| Comillas en los nombres de columna del `INSERT` | Consecuencia mecánica de la anterior: la lista de columnas se arma por interpolación y el resto del fixture ya citaba los nombres en el `CREATE TABLE`. |

### Lo que NO cambió

- Compradores: `[GALAPA, GALAPA, "OTRO COMPRADOR"]`, en ese orden.
- Cantidad de filas: 3, en ambos datasets.
- Documentos: `900333444` para el señuelo del Actor, `900111222` para los demás.
- Valores (`100`), fechas (`2026-08-01`), objetos (`OBRA PÚBLICA`), modalidad.
- El estado se escribe **solo** en `jbjy-vk9h`. `estado_de_apertura_del_proceso`
  sigue NULL, así que el lado de procesos —que ya pasaba— no se tocó.
- Ninguna aserción del test se relajó ni se eliminó.

### Verificación de que la corrección no es un atajo

`probe_c03.py.txt` corre la misma secuencia con el estado parametrizado:

| Filas contractuales | «Se observan 2 contratos» |
|---|---|
| 2× Galapa `En ejecución` | sí |
| + 1 `OTRO COMPRADOR` `En ejecución` | sí (el comprador excluye, no el estado) |
| + 1 Galapa `Cancelado` | sí — el excluido no suma |
| + 1 Galapa `Borrador` | sí — el excluido no suma |
| sin `estado_contrato` | no — vuelve el cero falso |

Las cinco quedan congeladas en
`tests/test_contract_universe_r5_profile_compatibility.py`. Contract Universe
no se desactivó en ninguna.

## C04 — apertura del perfil contractual

### Lo que cambió

El test construía su «perfil histórico contractual» copiando los
`resolved_params` de un perfil de procesos y sobrescribiendo
`dataset="contratos"`, y luego lo persistía a mano con `append_turn`. Ahora cada
dataset se abre con la consulta que lo produce (`PROFILE_OPENERS`) y el turno se
persiste por la vía normal.

### Lo que NO cambió

- Las cuatro consultas de follow-up, literales.
- Las ocho aserciones del test (ruta, comprador, `PROFILE_ACTIVE_PARAM`, los dos
  rankings, dos llamadas, dataset único, comprador en el SoQL).
- La parametrización `["procesos", "contratos"]`: ambos datasets se siguen
  ejercitando, y ahora el test comprueba además que el abridor aterrizó en el
  dataset parametrizado.

### Por qué el dict fabricado no era un artefacto histórico

`PROFILE_STATE_DIFFS.json` compara los tres estados. `P3_UNREACHABLE_SCAN.json`
recorre 90 turnos de perfil, 63 con `dataset='contratos'`: ninguno sin sello de
universo, ninguno con `sources='BOTH'`. El artefacto V1 real que el repo ya
conserva (`docs/audits/contract_universe_v1_1_independent/`) sí lleva el sello,
con el valor viejo `contract-universe-1`. La forma del fixture no la produjo
ninguna versión; el fail-closed que disparaba es la política adjudicada, y los
perfiles viejos de verdad quedan cubiertos por
`test_an_old_or_unstamped_contractual_profile_is_sent_to_review`.
