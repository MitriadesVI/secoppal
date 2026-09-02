# SOURCE-NAME-PROJECTION-V1 — implementación

**Tipo:** implementación de ADR-073
**Fecha:** 2026-09-02 · **Rama:** `main`
**HEAD antes:** `5674538b5fc460209890977e8139237fe07748f6`
**Diseño:** [`SOURCE_NAME_PROJECTION_V1.md`](SOURCE_NAME_PROJECTION_V1.md) ·
[ADR-073](../decisions/2026-09-02_ADR-073_source-name-projection.md)

## 1. Veredicto

```text
SOURCE_NAME_PROJECTION_V1_IMPLEMENTATION: PASS

COMPARISON_OPERATOR:       EXACT_OR
WILDCARDS_PRESENT:         NO
UPPER_OR_FOLD_IN_BINDING:  NO

DAPRE procesos    0 → 17.128   (control NIT 17.128)
DAPRE contratos   0 →  4.351   (control NIT  4.351)
```

## 2. Lo que se construyó

`app/core/buyer_source_names.py` es la autoridad separada: identidad canónica
+ dataset → nombres físicos exactos. No conoce aliases (lenguaje humano,
autoridad de `EntityResolver`) ni display (autoridad del formatter).

`app/data/buyer_source_names.json` es el sidecar. Contiene **una** identidad:
DAPRE. Los strings se guardan tal como la fuente los publica; puntuación,
acentos y mayúsculas son parte del binding.

`SoQLBuilder._entity_clause` aplica la regla de tres caminos. Sólo el tercero
proyecta.

## 3. Por qué igualdad exacta y no LIKE

Una versión anterior del plan de implementación pedía LIKE. Contradecía a
ADR-073 y al artefacto congelado (`V1_DEFAULT_WITH_EXACT_OR`), y no era un
detalle de estilo.

El filtro previo apilaba **tres** relajaciones: `%…%`, `UPPER()` y accent
folding. Bajo esa forma, Soacha —que publica `MUNICIPIO DE SOACHA..` en
procesos y `MUNICIPIO DE SOACHA.` en contratos— deja que la grafía de un
dataset alcance la del otro por substring. Ese accidente es justamente lo que
hoy hace que Soacha *parezca* funcionar, y es el defecto que esta capa vino a
eliminar. Validar la implementación con la misma relajación habría sido
acreditarla con el error que la motivó.

La consecuencia práctica está en la mutación 2: bajo LIKE, usar el nombre de
procesos para consultar contratos no hace caer ningún test, porque una cadena
contiene a la otra. Bajo igualdad exacta, cae.

## 4. La regla

```text
entidad_like                      → LIKE previo, sin proyección
entidad_resolved sin binding      → LIKE previo, byte-for-byte
entidad_resolved con binding      → igualdad exacta unida por OR
```

Un nombre: `entidad = 'DAPRE'`.
Varios: `(entidad = 'NAME A' OR entidad = 'NAME B')`.

El OR va explícito y no `IN (...)`: es lo que declara el ADR.

El único tratamiento sobre el string es `_escape` (duplicar la comilla), que
no cambia el valor comparado.

## 5. El riesgo que no era obvio

La línea que había que tocar colapsaba los dos campos:

```python
entidad_value = params.get("entidad_like") or params.get("entidad_resolved")
```

`entidad_value` no sólo alimenta el filtro: gobierna los guards
`not entidad_value` de las ramas `entidad_ecosystem` e `entidad_in_candidates`.
Separar los campos ahí habría cambiado **cuándo disparan esas ramas**, y la
regresión no se habría visto en DAPRE sino en consultas sin relación con la
proyección.

La implementación conserva `entidad_value` con su precedencia intacta y pasa
la distinción como un flag al helper. Dos tests fijan que los guards no se
movieron.

## 6. Neutralidad

Catorce configuraciones de parámetros se congelaron antes del cambio y se
compararon después, sobre `build`, `build_count`, `build_top_entities` y
`build_value_stats` en ambos datasets.

Doce quedaron **byte-idénticas**: CGR, AGR, PGN, Defensoría, una CAR, una
Gobernación, SENA regional, Soacha, `entidad_like` solo, `entidad_like` junto a
un `entidad_resolved` con binding, `entidad_ecosystem` e
`entidad_in_candidates`.

Dos cambiaron, ambas con binding DAPRE:

1. `entidad_resolved` = canónico DAPRE → `entidad = 'DAPRE'`. Es el objetivo.
2. Canónico DAPRE + `entidad_contains_all` →
   `entidad = 'DAPRE' AND LIKE '%SENA%' AND LIKE '%MAGDALENA%'`.

El caso 2 se **documenta, no se arregla**. Antes y después devuelve cero filas
—el canónico ya devolvía cero por sí solo—, así que no hay regresión, y
combinar una identidad acreditada con tokens de ecosistema es una pregunta que
V1 no vino a responder.

## 7. Mutaciones

Las cinco muerden:

| # | Mutación | Resultado |
|---|---|---|
| 1 | desactivar el binding DAPRE | 5 tests caen |
| 2 | usar el nombre de procesos para contratos | 1 test cae |
| 3 | proyectar también `entidad_like` | 1 test cae |
| 4 | mutar `params` con el nombre físico | 2 tests caen |
| 5 | reintroducir `%`/`UPPER()`/folding en la rama con binding | 8 tests caen |

La mutación 3 **no mordió en el primer intento** y eso corrigió el test, no la
mutación. El canario original usaba un `entidad_like` sin binding: la
proyección devolvía `None` por ausencia y el guard nunca se ejercía, así que el
test pasaba aunque se hubiera borrado. El canario ahora pone en `entidad_like`
el canónico ligado, que es el único valor con el que el guard es observable.

## 8. Gates

```text
tests nuevos      75 PASS   (tests/test_source_name_projection_v1.py)
suite completa    10.636 PASS · 2 skipped · 15 xfailed · 0 fallos
corpus            89/89 PASS
ruff app/         limpio
make lint-core    app/core/ limpio
smoke Socrata     17.128 / 4.351, coincidentes con el control por NIT
```

`tests/test_truth_layer_public_parity.py` re-fija el SHA de `soql_builder.py`
con su justificación. La divergencia está acotada por el sidecar y no por una
condición de código: sólo diverge una identidad con binding, y V1 liga
únicamente a DAPRE, que ninguna consulta ordinaria menciona. Por eso el oráculo
público siguió byte-idéntico sin intervención.

## 9. V1.1 — el contrato de metadata

```text
ADR_METADATA_CONTRACT: IMPLEMENTED
```

ADR-073 exigía que el loader validara cinco cosas: `identity_key` y
`canonical_name` únicos, datasets conocidos, 1..N nombres no vacíos, nombres
sin duplicados, **y fecha y procedencia de la evidencia**. V1 implementó las
cuatro primeras. El sidecar traía `schema_version`, `evidence` y `observed_at`
desde el primer commit, y el loader no los miraba.

La discrepancia importa porque la metadata que nadie valida es peor que la
metadata ausente: aparenta procedencia sin acreditarla. Un binding sin fecha
verificada es indistinguible de uno que dejó de ser cierto, y esa es justo la
pregunta que `observed_at` existe para responder.

Lo que ahora se valida al cargar:

| Campo | Contrato |
|---|---|
| `schema_version` | requerido · entero · exactamente `1` |
| `evidence` | requerido · string no vacío |
| `authority` | si está, string no vacío |
| `observed_at` | requerido por binding · `YYYY-MM-DD` real |
| `observed_rows` | opcional · datasets conocidos y declarados · enteros ≥ 0 |

`evidence` se valida como metadata declarada, **no** como archivo en disco: un
despliegue puede no empaquetar `artifacts/`, y exigir el fichero convertiría
una afirmación de auditoría en una dependencia de runtime.

Nada de esto entra en la semántica de consulta. `SoQLBuilder` no cambió, el
sidecar no cambió, DAPRE proyecta igual. Es evidencia de auditoría expuesta en
`registry.provenance` y en el binding.

### Dos cosas que las mutaciones corrigieron

**La forma de la fecha la impone la regex, no el parser.**
`date.fromisoformat` acepta `20260902` y `2026-W36-3`: son ISO válidos y no son
`YYYY-MM-DD`. Quitar la comprobación de forma no hacía caer ningún test hasta
que se añadieron esos dos casos. Un `observed_at` en formato de semana ISO es
ilegible como evidencia de auditoría, que es su única razón de existir.

**El caso base de los negativos tenía que ser válido.**
Al añadir la validación de raíz, los doce casos negativos de V1 pasaron a
reventar todos en procedencia —les faltaba `schema_version`— antes de llegar a
su causa declarada: doce tests probando una sola cosa. El helper construye
ahora un sidecar completo y válido, cada caso afirma contra el fragmento de
error que le corresponde, y un control positivo prueba que la línea base
efectivamente carga.

## 10. Deuda

```text
DAPRE_SOURCE_NAME_SPLIT
  CLOSED

CANONICAL_ENTITY_SOURCE_NAMES
  OPEN — architecture implemented, migration incomplete
```

Haber construido la carretera no es haber migrado las ciudades. Siguen sin
binding, con evidencia ya levantada y sin autorización de V1: Soacha, Yopal,
Fondo Financiero Distrital de Salud, UAESP Bogotá y el preview de
universidades.

Intactas y separadas: `CGR_PHYSICAL_NAME_FRAGMENTATION_DEBT`, los candidatos a
Buyer Group (Cundinamarca, Valle) y `MINJUSTICIA_SHORT_FORMS` en REVIEW.

La brecha de admisión que el discovery ya había documentado —`QueryRouter` deja
"Presidencia de la República" en `objeto`, y sólo "DAPRE" resuelve por
`scan_exact`— sigue abierta y fuera de alcance. Esta capa corrige lo que se
proyecta cuando la identidad ya se acreditó; no cambia cuándo se acredita.
