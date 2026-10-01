# M1-D-MONEY — autoridad de VALUE pura (candidato)

Rama `mission/m1-d-money`, base `4ec99bc1`. Sin merge, sin push. No toca el núcleo
(`foundation_admission`, `foundation_projection`, `interpretation_contract`, prompts, producers,
`foundation_messages`).

## 1. Qué entrega

| Archivo | Cambio |
|---|---|
| `app/core/money_comparators.py` (nuevo) | **Léxico único** de comparadores: tabla de frases (familias GT/GTE/LT/LTE/EQ), negación, sufijos («o más», «en adelante»), símbolos. Puro. |
| `app/core/foundation_value_direction.py` | Guarda operador-vs-cita sobre el léxico único: los **4** operadores, negación, inclusivos. Sin `_MARKERS`/`_COMPATIBLE`. Misma firma pública (`operator_reason`, `directions`) y mismos 4 códigos `VALUE_OPERATOR_*`. |
| `app/core/value_authority.py` (nuevo) | `resolve_value_condition(quote, operator, polarity, left=(), right=()) -> ValueResolution`, `MoneyValue`, `TARGET_REASON`. |
| `app/core/money_polarity.py` | `_UP`/`_DOWN`/sufijos salen del léxico único; `NEGATION_FILLER` compartido tras «no». API pública intacta. |
| `tests/test_value_authority_m1d.py` (nuevo) | 645 tests (oráculo escrito a mano, propiedades Hypothesis, paridad entre consumidores, ventana, tipo, razones). |
| `tests/test_foundation_silent_paths.py` | Un test dejó de importar `_MARKERS`; itera `HEADS`. |
| `scripts/audits/foundation_promotion_mutants.py` | M08 apunta a la nueva línea (`if expected is None`). |
| `scripts/audits/m1_d_money_mutants.py` (nuevo) | 20 mutantes sobre una copia en scratch. |

## 2. Léxico y reglas (única fuente: `money_comparators.HEADS`)

| Familia | Frases (plegadas) |
|---|---|
| GT (exclusivo) | más de/que, mayor(es) a/de/que, superior(es) a/de/que, por encima de, supera(n)/supere(n)/superar, excede…/exceder, rebasa…/rebasar, sobrepasa…/sobrepasar, pase/pasa(n)/pasar de, `>` |
| LT (exclusivo) | menos de/que, menor(es)…, inferior(es)…, por debajo de, baje/baja(n)/bajar de, `<` |
| GTE | al menos, por lo menos, mínimo/a, desde, a partir de, igual o mayor/superior, «mayor/superior o igual a», `>=`, `≥`; sufijos «o más», «y más», «o mayores/superiores», «en adelante», «para arriba/más» |
| LTE | hasta, máximo/a, a lo sumo, tope (de), igual o menor/inferior, «menor/inferior o igual a», «sin superar/exceder/rebasar/sobrepasar/pasar de», `<=`, `≤`; sufijos «o menos», «o menores/inferiores», «para abajo/menos» |
| EQ | exactamente, igual a (se lee para **rechazarlo**: ningún operador VALUE lo admite) |

* **Negación** (`que no`, `no`, con relleno «sea(n)/es/son/deba(n)/pueda(n)/…»): `no(>x)=<=x`,
  `no(>=x)=<x`, `no(<x)=>=x`, `no(<=x)=>x`. «que no superen / no mayor a / no exceda / que no pase de /
  no más de» = LTE; «no menos de / no inferior a» = GTE. La negación de una frase léxicamente
  inclusiva («no al menos», «no hasta», «no mayor o igual») **no se adivina**: AMBIGUOUS.
* Un «no/sin/ni» que ninguna lectura consumió → AMBIGUOUS (nunca se ejecuta con negación sin leer).
* Compatibilidad: operador vs familia leída. Mismo límite y distinta inclusividad →
  `VALUE_OPERATOR_BOUNDARY_MISMATCH` (GT ≠ GTE). Límite opuesto o EQ/rango →
  `VALUE_OPERATOR_EVIDENCE_MISMATCH`. Sin comparador → `..._UNGROUNDED`. Dos comparadores →
  `..._AMBIGUOUS`.
* «más»/«mayor» sin enlace (`de/a/que`) no es comparador.

## 3. Autoridad `resolve_value_condition`

`ValueResolution(decision RESOLVED|CLARIFY|BLOCK, bound "min"|"max"|None, inclusive, amount_cop, reason,
authority="value-authority@1")` + `.value -> MoneyValue | None` + `.target_reason`.

Orden (todo fail-closed): polaridad ≠ POSITIVE → BLOCK · operador fuera de los 4 → BLOCK
(`value_without_comparator` → CLARIFY, B5) · rango («entre X y Y», «de X a Y», «desde X hasta Y»,
«más de X y menos de Y») → CLARIFY `VALUE_RANGE_NEEDS_TWO_CONDITIONS`, **sin llamar a `money_to_cop`**
(probado por monkeypatch) · `operator_reason` · segunda cifra → CLARIFY `VALUE_AMOUNT_AMBIGUOUS` · signo
negativo → CLARIFY `VALUE_AMOUNT_NEGATIVE` · ilegible/SMMLV/0 → CLARIFY `VALUE_AMOUNT_UNREADABLE` · bajo
el piso (`money_admission.is_admissible_amount`: «más de 5», «más de 100 mil») → CLARIFY
`VALUE_AMOUNT_UNGROUNDED`.

### Ventana (C0 §5.5)

La **cita** es la autoridad; `left`/`right` son ≤4 tokens plegados (se recorta a los 4 más cercanos).
**El contexto sólo retira**: un RESOLVED con contexto es idéntico al de la cita sola. Única retirada hoy:
«no/sin/ni» pegado a la izquierda de la cita (se saltan «que», «sea(n)», «deben»…) → CLARIFY
`VALUE_OPERATOR_EVIDENCE_MISMATCH`: el productor dejó la negación fuera de la cita y el operador leyó el
límite sin negar. **No se «corrige» a LTE** porque eso crearía un RESOLVED desde el contexto (el mutante
«contexto que crea RESOLVED» del kit de C1.1). `right` se acepta por conformidad y hoy no retira: las dos
mitades de un rango («menos de 300 y más de 100») son átomos legítimos y cada uno ve al otro en su
ventana (por eso tampoco se retira por una segunda cifra en el contexto).

### Mapeo a la interfaz C0 (para C1.1; este carril no crea `host_authorities/`)

| Mío | C0 |
|---|---|
| `resolve_value_condition(quote, operator, polarity, left, right)` | `DimensionAuthority.resolve(evidence, ctx)` para `dimension=VALUE`; `quote=evidence.window.quote`, `left/right=window.left/right` |
| `ValueDecision` | `ResolutionDecision` (mismos 3 nombres; `ValueDecision` es `str, Enum`) |
| `authority="value-authority@1"` | `AuthorityVerdict.authority` (`"<id>@<version>"`) |
| `ValueResolution.value` → `MoneyValue(amount_cop: int, comparator ∈ {">",">=","<","<="})` | `MoneyValue` de `ExecutableValue`. Incluyo `encode() -> ("money@1", cmp, "<int>")` / `decode` con ida y vuelta (Hypothesis). **El formato de la etiqueta es provisional**: C1.1 manda. |
| `reason` (granular, conserva los 4 `VALUE_OPERATOR_*` que `foundation_messages` ya traduce) y `target_reason` | `AUTHORITY_REASONS`: `OPERATOR_EVIDENCE_MISMATCH:VALUE` (EVIDENCE y BOUNDARY), `UNRESOLVED_HOST_DIMENSION:VALUE` (UNGROUNDED, AMOUNT_*, sin comparador, operador ajeno), `AMBIGUOUS_HOST_DIMENSION:VALUE` (AMBIGUOUS, rango, segunda cifra), `NEGATION_UNSUPPORTED:VALUE` (polaridad NEGATIVE). Conjunto cerrado de 4, verificado por test. |
| `effect`, `policies`, `receipt` | no los produce este módulo; el adaptador pone `EQUIVALENT` y un recibo `rules=[código granular]` |

BLOCK sólo en polaridad NEGATIVE/ajena y operador ajeno; todo lo demás es CLARIFY.

## 4. Forma exacta que necesita la proyección (C1; NO hecha aquí)

`SoQLBuilder` **ya** emite inclusivos: `valor_min` → `>=`, `valor_max` → `<=`, `valor_min_exclusive` → `>`,
`valor_max_exclusive` → `<` (`soql_builder.py:602-612`). `foundation_projection.py:310-315` sólo cablea los
exclusivos. La rama que falta (por tipo decodificado):

```python
KEY = {">": "valor_min_exclusive", ">=": "valor_min", "<": "valor_max_exclusive", "<=": "valor_max"}
params[KEY[value.comparator]] = value.amount_cop      # int estricto; _amount() hoy exige str de dígitos
_add_origin(origins, KEY[value.comparator], cid)
```

Cuidados: (a) pasar `int`, no `str` (`valor_*_exclusive` exige `type(v) is int`); (b) el builder lanza
`ValueError("CONFLICTING_VALUE_BOUND")` si coexisten `valor_min` y `valor_min_exclusive` (o los `max`): dos
átomos del mismo lado deben convertirse en CLARIFY antes de proyectar, no en excepción; (c) «entre X y Y» =
`valor_min` + `valor_max` (dos átomos GTE+LTE) no choca; (d) `legacy_shadow.py:368` ya compara los cuatro.
La admisión actual ya deja pasar `greater_or_equal`/`less_or_equal` como READY (la guarda devolvía `None`
antes y devuelve el veredicto correcto ahora): hasta que C1 cablee la proyección, esos operadores terminan
`UNSUPPORTED_CONDITION`, no se ejecutan mal.

Efecto visible en el host con el productor v4.2 (sólo `greater_than`/`less_than`): «que no superen 300
millones» + `less_than` sigue siendo CLARIFY, ahora con el código correcto (`BOUNDARY_MISMATCH`: el límite
es inclusivo). La aclaración falsa del anexo HERA desaparece cuando el productor v5 emita `less_or_equal` y
C1 proyecte `<=`.

## 5. Impacto legacy (medido base `4ec99bc1` vs rama)

**Sondas léxicas** (16.640 combinaciones vocabulario previo × prefijos × cifras × sufijos): **0 diferencias**
en `resolve_money_polarity`. De las 26.288 sondas totales difieren 9.000, todas por vocabulario nuevo.
**Corpus del repo** (2.115 frases con dinero: tests, fixtures, docs, artefactos y `data/` del repo
principal; raw y plegado): sólo cambian las que contienen vocabulario nuevo; `QueryRouter.parse` sobre 1.482
consultas: **0 diferencias**.

Cada cambio legacy es una corrección (base → rama; relación de `resolve_money_polarity`, y `valor_*` del router en los casos que probé de punta a punta: «por lo menos», «no es mayor a», «tope de», «sin superar»):

| Texto | Base | Rama | Porqué |
|---|---|---|---|
| «por lo menos 300 millones» | `valor_max` (`UPPER_BOUND`, «menos» ganaba) | `valor_min` | **inversión silenciosa** (también NAR-027 del corpus) |
| «no es mayor a 300 millones» | `LOWER_BOUND` | `UPPER_BOUND` | «es» escondía el «no»: inversión |
| «que no deben superar 300 millones» | sin filtro | `UPPER_BOUND` | caía sin aviso |
| «sin superar / sin exceder / sin pasar de 300 millones» | sin filtro | `UPPER_BOUND` | idem |
| «tope de 300 millones» | sin filtro | `UPPER_BOUND` | idem |
| «superar/exceder/rebasar/sobrepasar/pase de 300 millones» | sin filtro | `LOWER_BOUND` | infinitivos y «pase de» |
| «300 millones o inferiores» | sin filtro | `UPPER_BOUND` | sufijo nuevo |
| «≥ / ≤ 300 millones», «igual o mayor/menor a» | sin filtro / span parcial | cota con span completo | símbolos y frases nuevos |

(«sin filtro» = la cifra salía `UNKNOWN` y el router no aplicaba límite: el patrón
INV-NO-SILENT-EXPLICIT-MONEY-DROP.) Los spans protegidos (`protected_money_spans`) crecen sólo en esas
frases nuevas. Los consumidores (`query_router`, `negation_binding`, `accreditation/*`,
`contract_activity/filters`) conservan firma y comportamiento sobre el vocabulario previo. Las ramas A2
(`codex/a2-suppliers-*`) no difieren de la base en `money_polarity.py`, `money_admission.py` ni
`utils/money.py`: usan la misma API pública por `query_router` y `negation_binding`, de modo que la
paridad A2 es la misma de arriba.

Gates ejecutados (no la suite global):

* 73 archivos de `tests/` que importan o ejercitan `money_*`, `value_*`, `negation*`, `accreditation*`,
  `contract_activity` filters, `query_router`, `foundation_*`, `legacy_shadow`, `search_sh1`, `natural_*`
  + `tests/product_contract`: **rama 3.429 passed, 8 xfailed**; base 3.420 passed + 9 failed, 8 xfailed.
  Los 9 de la base son `test_procurement_foundation_frozen_delivery` (leen blobs de git) y fallan porque
  la base se exportó con `git archive` sin `.git`; pasan en la rama. Cero regresiones.
* `tests/test_value_authority_m1d.py` + `tests/test_foundation_silent_paths.py`: 694 passed.
* `ruff check` sobre todo lo tocado: limpio. `olympus-semgrep-delta --base 4ec99bc1`: TOTAL=0 BASELINE=0
  NEW_DELTA=0.
* Mutantes: `scripts/audits/m1_d_money_mutants.py` **20/20 muertos** (negación ignorada, negación sin
  inclusividad, guarda ciega a inclusivos, GT aceptado por GTE, rango convertido, sin piso, polaridad
  negativa aceptada, contexto ignorado, negación sin leer, léxico de polaridad que se separa, inclusivo
  invertido, sufijos, símbolo, negación adivinada, ventana sin recorte, «por lo menos» de vuelta,
  «mayor» sin enlace, decode laxo, segunda cifra, signo perdido). El M08 del arnés de promoción
  (`foundation_promotion_mutants.py`) se re-apuntó a la nueva línea; su runner necesita un plugin de red
  externo y no se corrió aquí.

## 6. Riesgos y deuda declarada

* **Límite de léxico, no de gramática**: lenguaje abierto («cuyo precio quede por debajo de», «que no
  llegue a») cae en UNGROUNDED/AMBIGUOUS (pregunta), nunca en un operador adivinado; el riesgo es una
  aclaración de más, no una inversión. Garantía acotada: lo no enumerado se pregunta.
* La guarda lee la **cita**; si el productor recorta la negación fuera de la cita y también propone el
  operador sin negar, sólo la ventana (`resolve_value_condition(left=…)`) lo ve: la guarda de
  `foundation_admission` no recibe contexto hoy. El enchufe SHADOW→ACTIVE (C1/E) cierra eso.
* «entre X y Y» exige dos átomos: cada cita necesita la unidad compartida («entre 100» + «300 millones»
  queda bajo el piso). Falta decidir en el intérprete/prompt v5 si cita la expresión entera en ambos
  átomos (entonces esta autoridad responde RANGE para cada uno) o si C1 añade un divisor de rango
  (`split_range` con `money_admission.shared_currency_marker`). **No lo implementé: es decisión de
  contrato.**
* `300M`/`300 mm` (abreviaturas) caen bajo el piso → CLARIFY (seguro, no convierte).
* `MoneyValue.encode/decode` provisional hasta C1.1.
* Mensajes de usuario para `VALUE_AMOUNT_*`, `VALUE_RANGE_*`, `VALUE_WITHOUT_COMPARATOR`,
  `VALUE_NEGATIVE_POLARITY`, `VALUE_OPERATOR_UNSUPPORTED` no existen en `foundation_messages.py` (núcleo):
  el adaptador debe mapear por `target_reason` o añadirlos en el paso E.
