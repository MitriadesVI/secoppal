# GAZETTEER-WIDE SOURCE BINDING AUDIT — Schema-3-aware

**Tipo:** auditoría read-only + artefacto reproducible · **Fecha:** 2026-09-23
**Base:** `main` @ `04426d1` · **Rama:** `audit/adr073-gazetteer-wide-schema3`
**Artefacto:** [`artifacts/audits/gazetteer_source_bindings_schema3/`](../../artifacts/audits/gazetteer_source_bindings_schema3/)
**Reproductor:** [`scripts/audits/audit_gazetteer_source_bindings_schema3.py`](../../scripts/audits/audit_gazetteer_source_bindings_schema3.py)
**Tests:** [`tests/test_gazetteer_source_binding_audit_schema3.py`](../../tests/test_gazetteer_source_binding_audit_schema3.py)

## 1. Veredicto

```text
GAZETTEER_WIDE_SCHEMA3: COMPLETE
  1134 / 1134 identidades adjudicadas (1111 BuyerUnits + 23 ecosistemas)
  identity_key usado: NO · NIT usado como identidad: NO
  593 bindings vigentes: 593 BOUND_VERIFIED, 0 review
  518 BuyerUnits sin binding:
      253 SOURCE_BINDING_CANDIDATE   (251 Directorio ADMISSIBLE + 2 catálogo SENA)
      265 en revisión, cada una con su gate
  Directory ADMISSIBLE sin binding: 251 → 251 candidatos (no por ser ADMISSIBLE:
      los 251 pasaron además los 9 gates estructurales)
  re-observación viva de los 253: procesos 253/253 PASS · contratos SOURCE_DEGRADED
RUNTIME MODIFICADO: NO · DATOS DE PRODUCTO MODIFICADOS: NO · PROMOCIÓN: NO
```

`GAZETTEER_WIDE_V1_SCHEMA3_COMPATIBLE = NO` se mantiene: V1 no se tocó. Este
auditor lo **sustituye como autoridad de adjudicación** y **consume** su
observación física.

## 2. Por qué readjudicar sin volver a medir

V1 falló por su modelo de identidad (exigía `identity_key`), no por su
observación de la fuente. V1 guardó, para cada identidad y cada dataset, el
universo completo que alcanza el fallback productivo (`_accent_fold_like`)
agrupado por `(forma física, NIT, filas)`, con `records.sha256` como índice.
El auditor verifica los 1.134 registros contra ese índice **antes** de leerlos:
si uno está alterado, aborta.

El universo exacto de una forma F se **deriva**: toda fila con `campo = 'F'`
tiene esa grafía y, por lo tanto, satisface el LIKE que ya la alcanzó. Con el
universo LIKE completo (sin tope de 200 grupos), `exact(F) = Σ filas de F`.
Si se proponen como binding todas las formas que el LIKE alcanza, entonces
`exact == fallback` por construcción, y la pregunta de seguridad pasa a ser la
que importa: **¿todas esas formas son esta BuyerUnit, y sólo ella?**

Hubo además una razón operativa: el 2026-09-23 `jbjy-vk9h` (contratos) fue
republicado con **1.000 filas** en total (medido: `count(*) = 1000`,
`publicationDate 09:24 UTC`). Una medición en vivo de contratos hoy habría
convertido cada universo en un cero falso.

## 3. Las 13 preguntas → campos de `records.jsonl`

| # | Pregunta | Campo |
|---|---|---|
| 1 | BuyerUnit lógica | `canonical_name`, `kind` |
| 2 | Evidencia fiscal positiva | `fiscal.accreditations`, `fiscal.declared_catalog` |
| 3 | Qué observó el Directorio | `fiscal.directory` (status, admissibility, shared_nit, cuentas) |
| 4-5 | Formas físicas en procesos / contratos | `datasets[ds].physical_forms` |
| 6 | Universo del fallback productivo | `datasets[ds].fallback_like_rows` |
| 7 | Universo de la proyección exacta | `exact_rows` (vigentes) / `exact_rows_derived` (propuestos) |
| 8 | ¿Equivalentes? | `equivalent_to_fallback` (`null` si el universo no está completo) |
| 9 | Identidades ajenas | gates `MULTI_NIT_REVIEW`, `CROSS_UNIT_FORM_COLLISION`, `FISCAL_CONFLICT_REVIEW` |
| 10 | Formas que se perderían | `forms_lost_vs_fallback` (0 por construcción) · `forms_lost_vs_identity` (ver §7) |
| 11 | BuyerGroup declarado | `declared_groups` (sólo de `buyer_groups.json`) |
| 12 | Ecosistema | `ecosystem` |
| 13 | Veredicto | `verdict`, `reasons`, `failed_gates` |

## 4. Gates (orden = precedencia; todos los fallos quedan en `reasons`)

```text
INCOMPLETE_OBSERVATION        universo LIKE truncado, timeout o no medido
NO_SOURCE_PRESENCE            el fallback no alcanza filas en ningún dataset
CANONICAL_COLLISION           dos canónicos del censo normalizan igual
MULTI_NIT_REVIEW              el universo alcanza >1 NIT (dentro o entre datasets)
NO_OBSERVED_NIT               la fuente no publica NIT para esas filas
MULTI_FORM_REVIEW             >1 forma física en un dataset
NAME_EXTENSION_REVIEW         la forma tiene tokens distintos del canónico
CROSS_UNIT_FORM_COLLISION     la forma es canónico o forma reclamada por OTRA BuyerUnit
DECLARED_GROUP_MEMBER_REVIEW  miembro de un BuyerGroup declarado
FISCAL_CONFLICT_REVIEW        Directorio ADMISSIBLE sobre un NIT distinto del de la fuente
STRUCTURAL_SAFE_UNCORROBORATED  pasa todo lo estructural, pero sin corroboración independiente
```

**Reclamar vs. alcanzar.** Que el LIKE de otra unidad alcance una forma por
substring (`ANI` alcanza `ARTESANIAS DE COLOMBIA S.A`, `JARDÍN BOTÁNICO…`) es un
defecto de esa otra unidad, no una segunda identidad para la forma. Sólo
*reclama* una forma la unidad cuyo nombre **es** esa forma (bytes o tokens
iguales). El alcance por substring se reporta en `reached_by_like_of`.

**Corroboración (no es circular).** El Directorio se consultó con el NIT que la
fuente publica junto a la forma, así que la coincidencia de NIT se da por
construcción. Lo que aporta `ADMISSIBILITY_FISCAL_STRICT_V0` es otra cosa: que
ese NIT tiene **exactamente una** cuenta oficial SECOP II y que su nombre es
token-igual al canónico. Es evidencia independiente sobre el **nombre** del
titular del NIT. La otra vía de corroboración es una unidad declarada por
catálogo institucional (`sena_units.json`).

**NIT compartido** es una dimensión, no un gate: las dos unidades SENA
candidatas comparten `899999034` y siguen siendo BuyerUnits distintas.

## 5. Resultados

| Veredicto | N |
|---|---:|
| `BOUND_VERIFIED` | 593 |
| `INSTITUTIONAL_ECOSYSTEM` | 23 |
| `SOURCE_BINDING_CANDIDATE` | **253** |
| `STRUCTURAL_SAFE_UNCORROBORATED` | 238 |
| `MULTI_NIT_REVIEW` | 13 |
| `NO_SOURCE_PRESENCE` | 8 |
| `MULTI_FORM_REVIEW` | 4 |
| `NAME_EXTENSION_REVIEW` | 1 |
| `INCOMPLETE_OBSERVATION` | 1 |
| demás gates | 0 |

**Candidatos (253):** 252 con la forma física idéntica en bytes al canónico y
1 token-igual (`MUNICIPIO DE SAN JOSE DE CUCUTA` → `…CUCUTA.`). Filas que
pasarían de LIKE a igualdad exacta: 2.613.927 en procesos y 1.768.335 en contratos.

- **Retenidos (35, sólo contratos):** cobertura por debajo del 1 % del otro
  dataset (`UNIVERSIDAD DE ANTIOQUIA` 44.658 / 34, `UNIVERSIDAD NACIONAL DE
  COLOMBIA`, `METROPARQUES`, `ECOPETROL`…). Vincular esa forma no cambiaría el
  comportamiento, pero **declararía** como proyección algo que casi seguro no
  lo es: la BuyerUnit publica ahí con otro nombre. Ese dataset queda en el
  fallback LIKE, sin cambios, hasta descubrir la forma real por NIT.
- **Ausentes en contratos (10):** 0 filas el 2026-09-18. Se proyecta sólo procesos.
- **Volumen bajo:** `GOBERNACION DEL GUAVIARE` (16/13) y `GOBERNACION DEL VAUPES`
  (2/1). Comportamiento idéntico al de hoy; es probable que la gobernación
  publique sobre todo con otro nombre. Se reportan, no se bloquean.

**Revisión (265), sin esconder:**

- `MULTI_NIT_REVIEW` (13): `ADRES`, `ANM`, `ESE CENTRO`, `HOSPITAL SAN ANDRES`,
  `UNIVERSIDAD DEL VALLE`, `DEPARTAMENTO DEL META`… Son las mismas colisiones
  de V1 §7, y el producto las sigue ejecutando hoy en el fallback.
- `NO_SOURCE_PRESENCE` (8): `MINSALUD`, `UNIVERSIDAD DE CUNDINAMARCA`,
  `EMPRESA SOCIAL DEL ESTADO METROSALUD`… El canónico no aparece como grafía en
  ninguna fuente.
- `MULTI_FORM_REVIEW` (4) y `NAME_EXTENSION_REVIEW` (1): mismas que V1.
- `INCOMPLETE_OBSERVATION` (1): `ANI` (universo truncado en 200 grupos).
- `STRUCTURAL_SAFE_UNCORROBORATED` (238): 102 con NIT multi-cuenta en el
  Directorio, 90 con cuenta única pero nombre no token-igual y 46 `NOT_FOUND`.
  **Ninguna queda refutada**: el Directorio no las corroboró, nada más. Son el
  próximo lote natural si aparece otra autoridad (catálogo, DIVIPOLA, decisión
  humana por unidad).

## 6. Re-observación viva (`--live-recheck`)

Una guarda de salud previa: `count(*)` debe superar un piso por dataset
(`p6dx-8zbt` ≥ 5 M, `jbjy-vk9h` ≥ 1 M). Un dataset por debajo del piso se
declara `SOURCE_DEGRADED` y no se mide, porque ausencia no es cero.

```text
2026-09-23T17:24Z  p6dx-8zbt count 9.207.780 → sano
                   jbjy-vk9h count 1.000     → SOURCE_DEGRADED
255 llamadas · 0 timeouts · 0 errores
procesos: 253 / 253 PASS  (forma presente, NIT único = NIT del 18-09, sin caída > 10 %)
contratos: no medido; vale la observación sha-verificada del 2026-09-18
```

## 7. Límites declarados

- **Pérdida frente a la identidad** (`forms_lost_vs_identity`): V1 sólo buscó
  formas por NIT cuando el NIT estaba acreditado. Para los candidatos, la
  pérdida frente al **fallback productivo** es 0 por construcción. Si la
  BuyerUnit publica además otra grafía que el LIKE nunca alcanzó, esa pérdida
  ya existe hoy y el binding no la agrava. No está medida.
- **Frescura de contratos:** 2026-09-18. Mientras `jbjy-vk9h` siga degradado,
  no hay forma honesta de re-observarlo.
- **Autoridades:** los sha256 de `aliases_db`, `buyer_groups`, `icbf_regionals`,
  `sena_units` y `soql_builder` coinciden con los del lote V1 (`manifest.json`).

## 8. Reproducción

```bash
.venv/bin/python scripts/audits/audit_gazetteer_source_bindings_schema3.py
```

Sin red. Si `adr073_directory_identity_attestations_v1/` no está en el checkout
(hoy no está trackeado), el auditor lee la proyección congelada
`inputs/directory_attestations_projection.jsonl`. Se verificó que las dos
entradas producen `records`, `candidates`, `review` y `summary` idénticos byte
a byte. `--live-recheck` repite la re-observación viva.

## 9. Deuda registrada (fuera de alcance)

- `SOURCE-HEALTH-GUARD-RUNTIME`: el producto no tiene guarda de salud de fuente;
  hoy muestra "0 contratos" como hecho (incidente del 2026-09-23).
- `JBJY-FORM-DISCOVERY-BY-NIT`: para los 35 retenidos, buscar la grafía real en
  contratos por NIT cuando la fuente se recupere.
- `DIRECTORY-ATTESTATIONS-UNTRACKED`: el sidecar schema 3 cita como `evidence`
  `artifacts/audits/adr073_directory_identity_attestations_v1/…`, que no está
  en git. En un checkout limpio esa referencia no resuelve.
- `UNCORROBORATED-238`: adjudicar por otra autoridad, unidad por unidad.
- Las 13 colisiones de `MULTI_NIT_REVIEW` siguen ejecutándose hoy en el
  fallback LIKE (defecto preexistente, ya declarado en V1 §7).
