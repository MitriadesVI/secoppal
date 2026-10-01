# M1-C1.1 — contrato de autoridades, ventana de evidencia, registro y kit de conformidad

Rama `mission/m1-c1-spine` (worktree `secoppal-m1-c1`), base `4ec99bc1`. Autoridad: ADR-103 (con
enmiendas 1–3) y `docs/audits/m1_search_foundation/C0_SPINE_V5_DESIGN.md` §3 (INV-C0-7/8/9), §5.5, §6 C1.1,
§7, §10, **enmendados por `docs/audits/m1_search_foundation/C1_1_METIS_REVIEW.md`** (FREEZE_WITH_CHANGES,
lista cerrada §5). **C1.1 no cambia el runtime**: seis archivos nuevos en `app/`, ninguno modificado, ningún
módulo de `app/` los importa (test + `git diff --name-status 4ec99bc1..HEAD -- app` sólo con `A`).

Candidato = el commit que contiene este archivo (ver `git log`). Cualquier cambio posterior invalida la
certificación. Historia: `af5a979e`…`880e5b73` = candidato original; `0e9aee4e` + `35380cc1` = ronda R1
(aplicación de la revisión METIS); este archivo se actualiza en el commit siguiente.

## R1. Revisión METIS aplicada, punto por punto de su §5

Las secciones 1–8 describen el estado **post-R1** (ya integran los cambios). Esta tabla dice cómo se aplicó cada
punto de la lista cerrada.

| Id | Estado | Cómo |
|---|---|---|
| **W1** | Aplicado | `EvidenceWindow(spans, segments, gaps, left, right)`; `quote` es una propiedad `str \| None` (el primer segmento si todos son la misma frase plegada, `None` si no); `MAX_EVIDENCE_SEGMENTS = 4`; validación `1 ≤ len(segments) ≤ 4`, segmentos NFC no vacíos, `len(gaps) == len(segments) - 1`, cada hueco ≤ `2*WINDOW_TOKENS` (constante `MAX_GAP_TOKENS`) tokens `[a-z0-9]+`; `truncated/stripped` conservan `segments` y `gaps` (test + mutante X05). |
| **W2** | Aplicado | `build_window(raw, spans)` sin `dimension` y sin `material_phrase` (el módulo ya no importa `foundation_evidence`; un test lo fija). Agrupa en segmentos las citas solapadas o separadas sólo por espacios; los huecos salen de `window_tokens(raw[fin_segmento:inicio_siguiente])`. Códigos cerrados `WINDOW_ERROR_CODES` = `INVALID_RAW_TEXT`, `NO_MATERIAL_EVIDENCE`, `UNBOUND_RAW_SPAN`, `SPAN_INSIDE_WORD`, `EVIDENCE_GAP_TOO_WIDE`, `TOO_MANY_EVIDENCE_SEGMENTS`; `EvidenceWindowError` rechaza (ValueError) cualquier otro código. `DISJOINT_EVIDENCE` eliminado. `SPAN_INSIDE_WORD`: el borde de **cada** cita con `isalnum()` a ambos lados. |
| **V2** | Aplicado | `AuthorityContext.datasets` no vacío (`EMPTY_CONTEXT_DATASETS`); `local_date()` = `reference_instant.astimezone(REFERENCE_TZ).date()`, `REFERENCE_TZ = timezone(timedelta(hours=-5))`. |
| **V3** | Aplicado | `AuthorityVerdict.detail: str \| None = None` (último campo): sólo en no-RESOLVED (`RESOLVED_WITH_DETAIL`), forma `[A-Z][A-Z0-9_]{0,63}` (`INVALID_VERDICT_DETAIL`); no entra en `to_resolution` (test: la resolución es idéntica con y sin `detail`). |
| **R1** | Aplicado | `GENERIC_IDENTITY:GEOGRAPHY` en `AUTHORITY_REASONS`. |
| **V5a** | Aplicado | `excluded: bool` como último campo (codificado `excluded=0/1`) en `TopicValue, BuyerValue, ActorValue, GeoValue, TimeValue, StateValue, ContractTypeValue` (ModalityValue ya lo tenía); MONEY, ORDER y CARDINALITY no lo llevan (test sobre `dataclasses.fields`). |
| **V5b** | Aplicado | `BuyerValue(canonical, kind: BuyerKind{UNIT, GROUP}, members, membership, excluded)`. UNIT: `members == (canonical,)`, `membership is None`. GROUP: `len ≥ 2`, ordenados y distintos, `canonical ∉ members`, `membership = "<group_id>@<versión>"` (patrón de id versionado en minúscula). `owners` desaparece. |
| **V5c** | Aplicado | `GeoValue.department: str \| None`: obligatorio con MUNICIPALITY, `None` con DEPARTMENT y REGION; `members` sigue siendo exclusivo de REGION. |
| **V5d** | Aplicado | `StateValue(canonical_class, literals: tuple[(dataset, column, values), ...], excluded)`: triples ordenados y únicos por `(dataset, column)`; `of(class, {dataset: {column: values}}, excluded=False)`; `literals_for(dataset) -> tuple[(column, values)] \| None`; `STATE_CLASSES` pasa a 13 (`suspended`, `ceded`). Semántica documentada: AND entre columnas del dataset, OR dentro de `values`. |
| **V5e** | Aplicado | `CardinalityValue(n, basis: CardinalityBasis{FIGURE, SINGULAR})`; `SINGULAR ⇒ n == 1` (`INVALID_CARDINALITY_SINGULAR`). |
| **K1** | Aplicado | Kit: con evidencia `UNKNOWN` se acepta cualquier nivel concreto; si no, `value.level is evidence.level`. Toy conforme `ToyGeoAnyLevel` (mutante X51). |
| **K2** | Aplicado | Kit: los alias `VALUE_*` sólo valen con la dimensión VALUE; test parametrizado sobre todas las dimensiones (mutantes K5-15, X54). |
| **K3** | Aplicado | Kit: INV-C0-7b — en cada caso RESOLVED con huecos, sustituir cada hueco (y todos a la vez) por `()` y por `("x",)` da el mismo RESOLVED(v) o un no RESOLVED. Toys no conformes: `GapChangesTheValue`, `EmptyGapPicksAnotherValue`, `AnyWordGapPicksAnotherValue`; conforme: `ToyState` (S3). |
| **K4** | Aplicado | Kit: `INV-C1.1-EXCL` (RESOLVED con `excluded` ⇒ `excluded == (polarity is NEGATIVE)`) e `INV-C1.1-DS` (datasets de los literales ⊆ `ctx.datasets`). `default_context()` ya no admite un conjunto vacío (lo rechaza el contrato). |
| **K5** | Aplicado | 15 mutantes de METIS, todos ejecutados y muertos (ver §5; 18 variantes). |
| **D0** | Aplicado | Este archivo. El coordinador sincroniza `C0_SPINE_V5_DESIGN.md` §5.5.1/§5.5.2 (la copia que traigo ya lleva el aviso de enmienda). |

También aplicadas, de las tablas de la revisión (no son filas de §5 pero las exige):

- **§2 #4/#5** — `build_window` lanza `EvidenceWindowError` con códigos propios cerrados; `DISJOINT_EVIDENCE` rechazado.
- **§2 #8 / D6** — alias `VALUE_*` restringidos a VALUE en el kit; `GENERIC_IDENTITY:GEOGRAPHY`.
- **§2 #12a/#12c/#12f/#12i** — rechazos y cambios de BUYER, GEO, STATE, CARDINALITY (V5b–e).
- **§2.5 (sonda)** — S3 («publicados o abiertos», dos citas) y la cita repetida de G1 construyen ventana válida; «sobras» da `SPAN_INSIDE_WORD`. Tests: `test_s3_two_cites_of_one_state_condition_build_a_window_with_the_gap_in_view`, `test_g1_a_phrase_cited_twice_builds_a_window_whose_gap_holds_the_middle_qualifier`, `test_window_refuses_a_cite_that_starts_or_ends_inside_a_word`, y en el kit `ToyState` sobre `S3_CASE` y `test_case_builds_a_window_from_several_cites_and_from_a_repeated_phrase`.
- **Obligaciones de carril/C1.2** (no de C1.1; las recojo para que no se pierdan): D-TIME debe importar `window_tokens`/`WINDOW_TOKENS` del núcleo y dejar de duplicar `fold`/`tokens_of`; D-MONEY usa `MoneyValue` del contrato; D-GEO pasa los catálogos por `AuthorityDeps.territories` y usa digests, no nombres, en `evidence`; el binding de ciudad SECOP vive en los datos de D-GEO; E-BUYER proyecta **todos** los miembros o BLOCK; C1.2/C1.6 debe admitir como evidencia de CARDINALITY el sintagma singular; el núcleo redacta `ActorValue.display` (ADR-060) antes de E-ACTOR.

### Decisiones donde la lista de METIS dejaba margen (ninguna contradice la revisión)

1. `excluded` es **obligatorio** (sin valor por defecto): olvidarlo es `TypeError`, no un `False` silencioso. `GeoValue` queda `level, canonical, department, divipola, members, excluded` (METIS no fijó la posición de `department`; ya no hay valores por defecto en `GeoValue`).
2. `StateValue.literals` vacío **sigue permitido** (no estaba en la lista; apretarlo sería una enmienda más). Si METIS quiere «un STATE resuelto siempre trae literales», es una línea.
3. El contrato no importa `app.utils.money` (arrastra `money_resolution` y datos); `quote` pliega con `_fold`, copia de `normalize_text` fijada por un test Hypothesis (`test_the_contract_fold_is_the_runtime_normalize_text`).
4. Un hueco puede ser `()` cuando las citas sólo las separa puntuación («publicados, abiertos»). La coma no es un token: `gaps` no distingue «publicados o abiertos» de «publicados, o abiertos». Es lo que dice §2.5 (tokens plegados); queda como riesgo (§8).
5. El kit repite por su cuenta la comprobación de `detail` (`RESOLVED_WITH_DETAIL`, `VERDICT_DETAIL`), igual que ya hacía con el resto del veredicto.
6. `IgnoresNegation` (TimeValue) ahora cae en `INV-C1.1-EXCL` antes que en `NEGATION_IGNORED`; esta última sigue alcanzable con VALUE (toy `IgnoresNegationOfAMoneyValue`).

## 1. Archivos

| Archivo | Papel |
|---|---|
| `app/core/foundation_authority_contract.py` | `WINDOW_TOKENS=4`, `MAX_EVIDENCE_SEGMENTS`, `MAX_GAP_TOKENS`, `REFERENCE_TZ`, `EvidenceWindow`, `AuthorityContext`, `DimensionEvidence`, `AuthorityReceipt`, `AuthorityVerdict`, `DimensionAuthority`, `AUTHORITY_REASONS`; reexporta la unión de valores |
| `app/core/foundation_authority_values.py` | `ExecutableValue` (unión cerrada de 11), `encode/decode` etiquetados, `decode_value`, `MAX_TOP_N`, `STATE_CLASSES`, `BuyerKind`, `CardinalityBasis` |
| `app/core/foundation_evidence_window.py` | ÚNICO constructor de `EvidenceWindow`: `build_window`, `window_tokens`, `EvidenceWindowError`, `WINDOW_ERROR_CODES` |
| `app/core/foundation_authority_registry.py` | `Maturity`, `AuthorityDeps`, `AuthorityEntry`, `REGISTRY = ()` (sin enganche) |
| `app/core/foundation_geo_level.py` | `GeoLevel` (módulo mínimo compartido por C1.1 y C1.2) |
| `app/core/host_authorities/__init__.py` | vacío |
| `tests/foundation_authority_kit.py` | `assert_conforms` y utilidades de casos |
| `tests/test_foundation_authority_contract.py`, `tests/test_foundation_authority_kit.py` | pruebas (Hypothesis) y autoprueba del kit |
| `docs/audits/m1_search_foundation/C0_SPINE_V5_DESIGN.md`, `docs/decisions/2026-09-30_ADR-103_...md`, `docs/audits/m1_search_foundation/C1_1_METIS_REVIEW.md` | espejo de la ADR (enmiendas 1–3), del diseño y de la revisión METIS (copias del worktree del coordinador) |

## 2. API final (firmas)

```python
WINDOW_TOKENS = 4; MAX_EVIDENCE_SEGMENTS = 4; MAX_GAP_TOKENS = 2 * WINDOW_TOKENS
REFERENCE_TZ = timezone(timedelta(hours=-5))
MAX_TOP_N = 100                      # P8
class GeoLevel(str, Enum): DEPARTMENT, MUNICIPALITY, REGION, UNKNOWN

@dataclass(frozen=True, slots=True)
class EvidenceWindow:
    spans: tuple[RawSpan, ...]
    segments: tuple[str, ...]                 # NFC; 1..4; un segmento por tramo de citas contiguas
    gaps: tuple[tuple[str, ...], ...]         # len == len(segments) - 1; <= 8 tokens [a-z0-9]+ cada uno
    left: tuple[str, ...]; right: tuple[str, ...]   # <= 4 tokens [a-z0-9]+ fuera de los extremos
    quote -> str | None                       # propiedad: la frase única de todos los segmentos, si no None
    def truncated(self, left: int, right: int) -> EvidenceWindow   # sólo quita contexto exterior
    def stripped(self) -> EvidenceWindow                           # truncated(0, 0); conserva segmentos y huecos

def build_window(raw: str, spans: Iterable[RawSpan]) -> EvidenceWindow   # raises EvidenceWindowError(reason)
def window_tokens(text: str) -> tuple[str, ...]

AuthorityContext(reference_instant: datetime /*tz-aware*/, datasets: frozenset[str] /*no vacío*/)
    .local_date() -> date                                  # hora de Colombia
DimensionEvidence(condition_id: str, dimension: SemanticDimension, operator: str,
                  polarity: Polarity, level: GeoLevel | None, window: EvidenceWindow)
AuthorityReceipt(rules: tuple[str, ...] = (), candidates: int = 0,
                 digests: tuple[tuple[str, str], ...] = ())     # identificadores y sha256, sin texto libre
AuthorityVerdict(decision: ResolutionDecision, authority: str /*"<id>@<v>"*/,
                 value: ExecutableValue | None = None, reason: str | None = None,
                 effect: Effect = EQUIVALENT, policies: tuple[str, ...] = (),
                 receipt: AuthorityReceipt = AuthorityReceipt(), detail: str | None = None)
    .to_resolution(target_id) -> HostResolution          # resolved_ids = value.encode()

class DimensionAuthority(Protocol):                      # runtime_checkable
    authority_id: str; version: str
    dimensions: frozenset[SemanticDimension]; operators: frozenset[str]
    def resolve(self, evidence: DimensionEvidence, ctx: AuthorityContext) -> AuthorityVerdict: ...

ExecutableValue = TopicValue | BuyerValue | ActorValue | GeoValue | TimeValue | MoneyValue
                | StateValue | ModalityValue | ContractTypeValue | OrderValue | CardinalityValue
    # cada uno: .encode() -> tuple[str, ...]; Cls.decode(items); decode_value(items) despacha por etiqueta
```

Campos de los valores (todos con etiqueta `<tipo>@1`; `excluded` es el último campo donde existe):
`TopicValue(phrase_folded, token_mode, excluded)`;
`BuyerValue(canonical, kind: BuyerKind, members, membership, excluded)`;
`ActorValue(document, document_kind, display, excluded)`;
`GeoValue(level, canonical, department, divipola, members, excluded)`;
`TimeValue(start, end_exclusive, precision, excluded)` (`precision` es para el eco y el recibo; la proyección sólo lee `start`/`end_exclusive`);
`MoneyValue(amount_cop, comparator: Comparator)`;
`StateValue(canonical_class, literals: ((dataset, column, values), ...), excluded)` + `StateValue.of(class, {dataset: {column: values}}, excluded=False)` / `.literals_for(dataset)`;
`ModalityValue(procedure_ids, family_policy, excluded)`; `ContractTypeValue(type_ids, excluded)`;
`OrderValue(key: OrderKey, direction: OrderDirection)`; `CardinalityValue(n, basis: CardinalityBasis)`.

Codificación: `("topic@1", "phrase_folded=obras", "token_mode=1", "excluded=0")`. Etiqueta `<tipo>@<versión>` primero; cada
campo `nombre=valor` con porcentaje-escape; `!` = None. Elementos distintos y no vacíos, así que cabe en
`HostResolution.resolved_ids` sin cambiar su forma. `decode` es estricto y canónico: etiqueta/versión
desconocida, campo faltante/extra/reordenado y cualquier grafía no canónica son `ContractError`.

Kit: `assert_conforms(authority, cases, *, monotone_chain=True, require_resolved=True) -> ConformanceReport`;
`ConformanceFailure.code`; `case(raw, needle, *, dimension, operator, polarity=POSITIVE, level=None, ctx=None)` donde
`needle` es una cita o una lista de citas `str | (str, ocurrencia)`; `evidence_from_text`, `default_context`, `pure_environment`.

## 3. Decisiones donde el diseño dejaba margen

1. **Valores en módulo aparte** (`foundation_authority_values.py`, reexportado por el contrato): evita un monolito.
   *(METIS: aceptada.)*
2. **`GeoLevel` en `foundation_geo_level.py`**: C1.2 debe importarlo de aquí, no redefinirlo. *(Aceptada.)*
3. **Tokenizador**: `foundation_time_coverage.py` (M1-H0) no existe en la base `4ec99bc1`; `window_tokens` implementa la
   misma regla (`normalize_text` + `[a-z0-9]+`). D-TIME/H0 deben importar `window_tokens` y `WINDOW_TOKENS` del núcleo.
   *(Aceptada con esa obligación.)*
4. **`build_window` lanza `EvidenceWindowError(reason)`** con códigos propios y cerrados (R1/W2). *(Aceptada con cambio, aplicado.)*
5. **Citas múltiples**: la ventana las representa (segmentos + huecos expuestos enteros, ≤ 2K tokens, si no CLARIFY);
   `DISJOINT_EVIDENCE` queda eliminado y el hueco no puede cambiar el valor (INV-C0-7b). *(Antes: rechazo; METIS lo rechazó, aplicado.)*
6. `EvidenceWindow` se valida a sí misma y expone `truncated/stripped` (sólo retiran contexto exterior). «Único
   constructor» se verifica con un test que escanea `app/`. *(Aceptada; ampliada a segmentos y huecos.)*
7. **`DimensionEvidence`**: dimensión por NOMBRE; `level` obligatorio exactamente con GEOGRAPHY; el operador no se valida
   contra la tabla (no existe hasta C1.2). *(Aceptada.)*
8. **`AUTHORITY_REASONS`**: `frozenset` enumerado (código × dimensión) + `LEVEL_MISMATCH:GEOGRAPHY`,
   `GENERIC_IDENTITY:{BUYER,ACTOR,GEOGRAPHY}`; los cuatro `VALUE_*` existentes son alias fijados por test contra
   `foundation_messages.KNOWN_CODES`, y el kit los acepta sólo con VALUE. *(Aceptada con cambio, aplicado.)*
9. **Invariantes del veredicto** más allá de «RESOLVED ⇔ valor»: RESOLVED no lleva `reason` ni `detail`; no-RESOLVED exige
   `effect = EQUIVALENT`; un `effect` ≠ EQUIVALENT exige `policies`; `AUTHORITY_UNAVAILABLE:*` ⇒ BLOCK; `authority` y cada
   política tienen forma `<id>@<versión>`. El núcleo traduce la *decisión* (CLARIFY→NEEDS_CLARIFICATION,
   BLOCK→UNSUPPORTED), no re-decide por la razón. *(Aceptada.)*
10. `AuthorityReceipt`: `rules` (≤64 sin espacios, ≤32), `candidates`, `digests` `(etiqueta, sha256)`. Límite declarado:
    un identificador puede cargar texto del usuario; el adaptador usa digests. *(Aceptada.)*
11. `AuthorityVerdict.to_resolution(target_id)`. *(Aceptada.)*
12. **Valores** (reglas que el diseño no precisaba): 12a `BuyerValue` unidad|grupo (R1; antes `owners == 1`, rechazada);
    12b `ActorValue.document_kind` identificador abierto distinto de `UNKNOWN` (aceptada); 12c `GeoValue` sin `UNKNOWN`,
    `divipola` de 2 o 5 dígitos, `members` sii REGION, `department` sii MUNICIPALITY (R1); 12d `TimeValue` semiabierto,
    `precision` etiqueta abierta no ejecutable (aceptada); 12e `MoneyValue.amount_cop ≥ 0` (aceptada); 12f `StateValue`
    por triples dataset/columna/valores y 13 clases (R1; antes `by_dataset`, rechazada en la forma); 12g `ModalityValue`
    exige `procedure_ids` o `family_policy` (aceptada); 12h `ContractTypeValue.type_ids` no vacío (aceptada); 12i
    `CardinalityValue.n ∈ 1..100` con `basis` (R1).
13. **Kit, más estricto que la letra** (cada punto con su mutante): monotonía en cadena y de huecos; pureza que registra el
    toque aunque se trague la excepción; operador/dimensión no declarados nunca RESOLVEN; valor del tipo de la dimensión;
    nivel geográfico (UNKNOWN ⇒ cualquier concreto); EXCL y DS; verificación independiente de
    `AuthorityVerdict.__post_init__`; un run sin RESOLVED se rechaza (`NO_RESOLVED_CASE`). *(Aceptada con cambios K1–K5, aplicados.)*
14. **Ownership**: `olympus-state own claim` sobre `docs/audits/m1_search_foundation` devuelve CONFLICT contra el run del
    coordinador `r-20260930-221119-112b` (el coordinador escribe ese directorio **en el worktree `secoppal-m1`**; yo sólo
    escribo copias en `secoppal-m1-c1`). No se forzó; se reclamó el resto del scope y las tres copias de documentos son el
    espejo pedido por el coordinador. *(Aceptada.)*

## 4. Gates (comandos y resultados literales)

Todo con `PYTHONPATH=<worktree>` y `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python` (3.14.7). Código en `35380cc1`.

| Gate | Comando | Resultado |
|---|---|---|
| Ruff | `python -m ruff check` sobre los 8 archivos de código tocados | `All checks passed!` |
| Contrato + kit | `pytest tests/test_foundation_authority_contract.py tests/test_foundation_authority_kit.py` | `238 passed` (180 + 58) |
| Foundation (sin regresión) | `pytest tests/test_foundation_*.py` (incluye los dos anteriores) | `2776 passed in 169.50s` |
| Mutantes | `ares_c1r1_mutants.py all` (ver §5) | ver §5 |
| Semgrep | `olympus-semgrep-delta --base 4ec99bc1 --json <8 archivos>` | `TOTAL 0, BASELINE 0, NEW_DELTA 0, FIXED 0` |
| Sin cambio de runtime | `git diff --name-status 4ec99bc1..HEAD -- app` | 6 líneas `A`, 0 `M`; test `test_c1_1_modules_are_not_imported_by_any_runtime_module` |

Corrí los archivos anteriores, no la suite global (la decide el coordinador).

## 5. Mutantes (aplicar, correr los dos archivos de test con `-x`, restaurar)

Runner fuera del repo (`scratchpad/ares_c1r1_mutants.py`; copia de seguridad y restauración por archivo; `git status`
limpio después). Tres grupos:

- **C1.1 (45 originales, 44 aplicables)**: los 16 del diseño y los 29 extras, con el patrón actualizado al texto R1. El de
  «dueño múltiple de comprador» ya no existe (la regla `owners == 1` se retiró con V5b; su equivalente es K5-08) y el de
  `DISJOINT_EVIDENCE` pasa a «citas con texto entre ellas rechazadas» (misma intención).
- **K5 (los 15 de METIS, 18 variantes)**: hueco ocultado; hueco sin tope; `quote` que une segmentos distintos; repetición
  rechazada; S3 rechazado; cita dentro de palabra aceptada; `excluded` ignorado (valor y kit); grupo con un solo miembro;
  municipio sin `department`; literal STATE sin columna (validación y codificación); singular con `n=2`; `detail` en un
  RESOLVED (contrato y kit); `local_date` en UTC; `datasets` vacío aceptado; alias VALUE en TIME.
- **Extras R1 (54)**: variantes de las mismas familias (tope de hueco en contrato/duplicado, `quote` sin plegado, `truncated`
  que tira los huecos, tope de segmentos, citas contiguas sin fusionar, fin de segmento no máximo, sólo inicio/sólo fin
  dentro de palabra, `isalnum` con `or`, códigos de error abiertos, offset −6, fecha del instante sin zona, `detail`,
  `excluded` sin validar/codificar en cada tipo, reglas de `BuyerValue`, `department`, clases STATE, orden de literales,
  `literals_for`, `basis`, y los chequeos nuevos del kit: DS, huecos, rellenos `()`/`("x",)`, detalle, nivel UNKNOWN, EXCL).

Resultado en el código de `35380cc1`: **116/116 muertos** (44 C1.1 + 18 K5 + 54 extras), 0 sobrevivientes.

Sobrevivientes de la primera pasada de R1: 1 (`X54`, «alias permitido sólo en TIME»: el único toy no conforme estaba en TIME);
reparado con `test_the_value_aliases_are_accepted_by_the_kit_only_for_the_value_dimension` (una comprobación por dimensión) y
re-ejecutado: muerto. Los kills de mutantes que sólo fallan al **importar** el kit (K5-05, el de «texto entre citas») son
válidos pero débiles: los mata también el test de ventana S3 del contrato.

### Resultado de la corrida

`python ares_c1r1_mutants.py all` -> `[all] 116/116 killed; survivors: []`. `git status` limpio en `app/` y `tests/` después.

## 6. Autopruebas del kit

Conformes: año con calificadores del contexto que sólo retiran (`ToyYear`), modalidad con negación honrada por `excluded` +
política, geografía con nivel y con nivel UNKNOWN resuelto, **STATE con literales por (dataset, columna) y el embudo S3**
(`ToyState`: resuelve sólo con el hueco `("o",)`, `RESIDUE_UNREAD:STATE` + `detail` con cualquier otro), VALUE (alias sólo
en VALUE) y una autoridad que usa `ctx.local_date()` dentro del sandbox. No conformes, cada una rechazada con su código
esperado: contexto crea RESOLVED / cambia el valor / rompe la cadena (INV-C0-7); **un hueco cambia el valor** (INV-C0-7b, 3
toys); reloj, socket, `get_settings`, no determinista (INV-C0-8); lanza; razón fuera del conjunto o de otra dimensión
(incluido un alias VALUE_* en TIME); RESOLVED sin valor; valor sin RESOLVED; valor fuera de la unión / de otro tipo / que no
hace ida y vuelta (INV-C0-9); etiqueta ajena; negación ignorada, sin política, retirada por otra razón, o resuelta a un valor
sin `excluded` (INV-C1.1-EXCL, 2 toys); literales STATE de un dataset fuera del contexto (INV-C1.1-DS); `detail` en un
RESOLVED o mal formado; operador o dimensión no declarados resueltos; nivel geográfico equivocado; declaración inválida.

## 7. Rondas de reparación

R1 (esta ronda): aplicada la lista de METIS en un paso; 2 correcciones propias de calidad (clase A/B): (1) un toy del kit comparaba
`gaps == ()` en vez de `((),)` (el fallo lo expuso la autoprueba, no un mutante); (2) el sobreviviente X54. Ningún hallazgo C–F; ningún
punto de la revisión era ambiguo o contradictorio.

## 8. Riesgos y límites declarados

- **Contrato congelado, coste de cambio alto** (§8 del diseño). Tras esta ronda queda para METIS/ZEUS: el tope de 8 tokens por
  hueco (si el Gold tiene citas dispersas legítimas, falla hacia aclarar), K=4, y que `gaps` sólo lleva tokens.
- **La puntuación del hueco es invisible**: «publicados o abiertos» y «publicados, o abiertos» dan el mismo hueco `("o",)`
  (y «publicados, abiertos» da `()`). El host vigente (`composition_reason`) lee el texto entre citas; la autoridad S3 de E-STATE
  deberá decidir si la coma importa. Si importa, hay que añadir un marcador al hueco (cambio de contrato) antes de congelar.
- **K=4**: un calificador a más de 4 tokens fuera de los segmentos no se ve.
- **Pureza del kit**: sólo barre `app.*`, el módulo de la autoridad y sus bases; no ve un reloj capturado en el constructor ni
  uno en una librería externa; `time.time()` no se vigila. Los sockets se vigilan en `connect`/`connect_ex`/`create_connection`/
  `getaddrinfo`/`gethostbyname*`.
- **Polaridad**: el kit exige «valor distinto + política» y `excluded` coherente, o `NEGATION_UNSUPPORTED`; no prueba que el
  complemento sea el correcto: es de la prueba de dominio de cada carril.
- **PII**: `ActorValue.display` viaja en `resolved_ids`; el núcleo debe redactarlo (ADR-060) antes de E-ACTOR.
- La traducción razón → CLARIFY/BLOCK (salvo `AUTHORITY_UNAVAILABLE`) y el mensaje al usuario son del núcleo en los pasos E.
- Sin enganche: `REGISTRY = ()`, `foundation_admission` no importa nada de esto.
- Tras congelar el SHA: avisar a D-TIME, D-MONEY y D-GEO de que rebasen (lo hace el coordinador).
