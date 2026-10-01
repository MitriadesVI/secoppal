# M1-C1.1 — contrato de autoridades, ventana de evidencia, registro y kit de conformidad

Rama `mission/m1-c1-spine` (worktree `secoppal-m1-c1`), base `4ec99bc1`. Autoridad: ADR-103 (con
enmienda 1) y `docs/audits/m1_search_foundation/C0_SPINE_V5_DESIGN.md` §3 (INV-C0-7/8/9), §5.5, §6 C1.1,
§7, §10. **C1.1 no cambia el runtime**: seis archivos nuevos en `app/`, ninguno modificado, ningún
módulo de `app/` los importa (test + `git diff --name-status` sólo con `A`).

Candidato = el commit que contiene este archivo (ver `git log`). Cualquier cambio posterior invalida la
certificación.

## 1. Archivos

| Archivo | Papel |
|---|---|
| `app/core/foundation_authority_contract.py` | `WINDOW_TOKENS=4`, `EvidenceWindow`, `AuthorityContext`, `DimensionEvidence`, `AuthorityReceipt`, `AuthorityVerdict`, `DimensionAuthority`, `AUTHORITY_REASONS`; reexporta la unión de valores |
| `app/core/foundation_authority_values.py` | `ExecutableValue` (unión cerrada de 11), `encode/decode` etiquetados, `decode_value`, `MAX_TOP_N`, `STATE_CLASSES` |
| `app/core/foundation_evidence_window.py` | ÚNICO constructor de `EvidenceWindow`: `build_window`, `window_tokens`, `EvidenceWindowError` |
| `app/core/foundation_authority_registry.py` | `Maturity`, `AuthorityDeps`, `AuthorityEntry`, `REGISTRY = ()` (sin enganche) |
| `app/core/foundation_geo_level.py` | `GeoLevel` (módulo mínimo compartido por C1.1 y C1.2) |
| `app/core/host_authorities/__init__.py` | vacío |
| `tests/foundation_authority_kit.py` | `assert_conforms` y utilidades de casos |
| `tests/test_foundation_authority_contract.py`, `tests/test_foundation_authority_kit.py` | pruebas (Hypothesis) y autoprueba del kit |
| `docs/audits/m1_search_foundation/C0_SPINE_V5_DESIGN.md`, `docs/decisions/2026-09-30_ADR-103_...md` | espejo de la ADR y el diseño (versión con enmienda 1/P3) |

## 2. API final (firmas)

```python
WINDOW_TOKENS = 4
MAX_TOP_N = 100                      # P8
class GeoLevel(str, Enum): DEPARTMENT, MUNICIPALITY, REGION, UNKNOWN

@dataclass(frozen=True, slots=True)
class EvidenceWindow:
    spans: tuple[RawSpan, ...]; quote: str            # NFC
    left: tuple[str, ...]; right: tuple[str, ...]      # <= 4 tokens [a-z0-9]+ cada lado
    def truncated(self, left: int, right: int) -> EvidenceWindow   # sólo quita contexto
    def stripped(self) -> EvidenceWindow                           # truncated(0, 0)

def build_window(raw: str, spans: Iterable[RawSpan],
                 dimension: SemanticDimension | str) -> EvidenceWindow   # raises EvidenceWindowError(reason)
def window_tokens(text: str) -> tuple[str, ...]

AuthorityContext(reference_instant: datetime /*tz-aware*/, datasets: frozenset[str])
DimensionEvidence(condition_id: str, dimension: SemanticDimension, operator: str,
                  polarity: Polarity, level: GeoLevel | None, window: EvidenceWindow)
AuthorityReceipt(rules: tuple[str, ...] = (), candidates: int = 0,
                 digests: tuple[tuple[str, str], ...] = ())     # identificadores y sha256, sin texto libre
AuthorityVerdict(decision: ResolutionDecision, authority: str /*"<id>@<v>"*/,
                 value: ExecutableValue | None = None, reason: str | None = None,
                 effect: Effect = EQUIVALENT, policies: tuple[str, ...] = (),
                 receipt: AuthorityReceipt = AuthorityReceipt())
    .to_resolution(target_id) -> HostResolution          # resolved_ids = value.encode()

class DimensionAuthority(Protocol):                      # runtime_checkable
    authority_id: str; version: str
    dimensions: frozenset[SemanticDimension]; operators: frozenset[str]
    def resolve(self, evidence: DimensionEvidence, ctx: AuthorityContext) -> AuthorityVerdict: ...

ExecutableValue = TopicValue | BuyerValue | ActorValue | GeoValue | TimeValue | MoneyValue
                | StateValue | ModalityValue | ContractTypeValue | OrderValue | CardinalityValue
    # cada uno: .encode() -> tuple[str, ...]; Cls.decode(items); decode_value(items) despacha por etiqueta

Maturity = DECLARED | SHADOW | ACTIVE
AuthorityDeps(entity_resolver, actor_name_resolver, territories, catalogs)   # object | None
AuthorityEntry(dimension: SemanticDimension, factory: Callable[[AuthorityDeps], DimensionAuthority],
               maturity: Maturity)
REGISTRY: tuple[AuthorityEntry, ...] = ()
```

Campos de los valores: `TopicValue(phrase_folded, token_mode)`, `BuyerValue(canonical, owners)` (exactamente 1
dueño), `ActorValue(document, document_kind, display)`, `GeoValue(level, canonical, divipola, members)`,
`TimeValue(start, end_exclusive, precision)`, `MoneyValue(amount_cop, comparator: Comparator)`,
`StateValue(canonical_class, by_dataset)` + `StateValue.of(class, mapping)` / `.literals(dataset)`,
`ModalityValue(procedure_ids, family_policy, excluded)`, `ContractTypeValue(type_ids)`,
`OrderValue(key: OrderKey, direction: OrderDirection)`, `CardinalityValue(n)`.

Codificación: `("topic@1", "phrase_folded=obras", "token_mode=1")`. Etiqueta `<tipo>@<versión>` primero; cada campo
`nombre=valor` con porcentaje-escape; `!` = None. Elementos distintos y no vacíos, así que cabe en
`HostResolution.resolved_ids` sin cambiar su forma. `decode` es estricto y canónico: etiqueta/versión
desconocida, campo faltante/extra/reordenado y cualquier grafía no canónica son `ContractError`.

Kit: `assert_conforms(authority, cases, *, monotone_chain=True, require_resolved=True) -> ConformanceReport`;
`ConformanceFailure.code`; `case(raw, needle, *, dimension, operator, polarity=POSITIVE, level=None, ctx=None)`;
`evidence_from_text`, `default_context`, `pure_environment`.

## 3. Decisiones donde el diseño dejaba margen

1. **Valores en módulo aparte** (`foundation_authority_values.py`, reexportado por el contrato): evita un monolito de
   ~700 líneas; el contrato público sigue siendo `foundation_authority_contract`.
2. **`GeoLevel` en `foundation_geo_level.py`.** El diseño lo lista bajo `foundation_operations.py` (C1.2): C1.2 debe
   importarlo de aquí, no redefinirlo.
3. **Tokenizador**: `foundation_time_coverage.py` (M1-H0) no existe en la base `4ec99bc1`; se implementó la misma regla
   (`normalize_text` + `[a-z0-9]+`) en `window_tokens`. D-TIME debe reutilizar `window_tokens` al migrar.
4. **`build_window` lanza `EvidenceWindowError(reason)`** (códigos cerrados, nunca texto crudo) en vez de devolver
   `(valor, razón)`: no se puede olvidar comprobar. Reutiliza `material_phrase` (frase única, citas contiguas,
   duplicadas).
5. **`DISJOINT_EVIDENCE`**: la ventana sólo ve a la izquierda de la primera cita y a la derecha de la última; para que
   un calificador del medio no quede oculto, citas no contiguas se rechazan, **también la misma frase repetida en dos
   sitios** (hoy G1 la acepta). Falla hacia aclarar. Relajable sin cambiar el contrato (sólo el constructor).
6. `EvidenceWindow` se valida a sí misma (≤K, `[a-z0-9]+`, NFC, no vacía) y expone `truncated/stripped` (sólo retiran
   contexto). «Único constructor» se verifica con un test que escanea `app/`.
7. **`DimensionEvidence`**: la dimensión debe tener autoridad, comparada por NOMBRE (`ORDER`/`CARDINALITY` entran en
   `SemanticDimension` en C1.2 sin tocar este contrato); `level` obligatorio exactamente con GEOGRAPHY; el operador no se
   valida contra la tabla (no existe hasta C1.2).
8. **`AUTHORITY_REASONS`** es un `frozenset` enumerado (código × dimensión). `LEVEL_MISMATCH` sólo con GEOGRAPHY,
   `GENERIC_IDENTITY` sólo BUYER/ACTOR. Los cuatro `VALUE_*` existentes se copian como alias y un test los fija contra
   `foundation_messages.KNOWN_CODES` (deriva = rojo). El contrato no importa módulos de runtime.
9. **Invariantes del veredicto** más allá de «RESOLVED ⇔ valor»: RESOLVED no lleva `reason`; no-RESOLVED exige `effect =
   EQUIVALENT`; un `effect` ≠ EQUIVALENT exige `policies`; `AUTHORITY_UNAVAILABLE:*` ⇒ BLOCK; `authority` y cada política
   tienen forma `<id>@<versión>`. La traducción CLARIFY/BLOCK del resto de razones queda al núcleo.
10. `AuthorityReceipt` (el diseño no fija campos): `rules` (identificadores ≤64 sin espacios, ≤32), `candidates`, `digests`
    `(etiqueta, sha256)`. Es estructura, no una garantía contra que un identificador cargue texto del usuario.
11. `AuthorityVerdict.to_resolution(target_id)` añadido: ata el valor codificado a `HostResolution.resolved_ids`.
12. **Valores** (reglas que el diseño no precisaba): `BuyerValue.owners` exactamente 1; `ActorValue.document_kind` es un
    identificador mayúsculo abierto distinto de `UNKNOWN` (`actor_identity` sólo tiene NIT/CC/UNKNOWN; el diseño dice
    «NIT/CC/…»); `GeoValue` sin `UNKNOWN`, `divipola` de 2 dígitos (depto) o 5 (municipio), `members` no vacío sii REGION;
    `TimeValue` intervalo semiabierto, al menos una cota, `precision` etiqueta abierta `[a-z][a-z0-9_]*`; `MoneyValue`
    `amount_cop ≥ 0`; `StateValue.by_dataset` es una tupla ordenada de pares (un dict sería mutable/inhashable) con
    literales no vacíos y las 11 clases del diseño; `ModalityValue` exige `procedure_ids` o `family_policy` (el segundo
    cubre «licitación coloquial: no filtra, con política»); `ContractTypeValue.type_ids` no vacío («residuo vacío
    obligatorio» se interpreta como deber de la autoridad: `RESIDUE_UNREAD`, no un campo); `CardinalityValue.n ∈ 1..100`
    (compatible con la enmienda 1 de la ADR: singular ⇒ 1).
13. **Kit, más estricto que la letra** (cada punto con su mutante): monotonía en cadena (todo truncamiento del contexto de
    una resolución RESOLVED sigue RESOLVED con el mismo valor; `monotone_chain=False` da la INV-C0-7 literal); la pureza
    registra el toque aunque la autoridad se trague la excepción; operador/dimensión no declarados nunca RESOLVEN; el
    valor es del tipo de la dimensión y `GeoValue.level` es el de la evidencia; verificación independiente de
    `AuthorityVerdict.__post_init__` (un contrato mutado no ciega al kit); un run sin ningún caso RESOLVED se rechaza
    (`NO_RESOLVED_CASE`, si no INV-C0-7/9 son vacuas).
14. **Ownership**: `olympus-state own claim` sobre `docs/audits/m1_search_foundation` devolvió CONFLICT (run del
    coordinador `r-20260930-221119-112b`); no se forzó. Se reclamó el resto del scope y la copia de ADR/diseño es un espejo
    pedido por el coordinador, no una edición propia.

## 4. Gates (comandos y resultados literales)

Todo con `PYTHONPATH=<worktree>` y `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python` (3.14.7).

| Gate | Comando | Resultado |
|---|---|---|
| Ruff | `python -m ruff check` sobre los 9 archivos de código tocados | `All checks passed!` |
| Tests nuevos | `pytest tests/test_foundation_authority_contract.py` / `.../test_foundation_authority_kit.py` | `117 passed` / `36 passed` |
| Foundation (sin regresión) | `pytest tests/test_foundation_*.py` (incluye los nuevos) | `2561 passed in 116.88s` |
| Semgrep | `olympus-semgrep-delta --base 4ec99bc1 --json <archivos nuevos>` | `TOTAL 0, BASELINE 0, NEW_DELTA 0, FIXED 0` |
| Sin cambio de runtime | `git diff --name-status 4ec99bc1..HEAD -- app` | 6 líneas `A`, 0 `M`; test `test_c1_1_modules_are_not_imported_by_any_runtime_module` |

Corrí los archivos anteriores, no la suite global (la decide el coordinador).

## 5. Mutantes (aplicar, correr los dos archivos de test, restaurar)

Runner fuera del repo (`scratchpad/mutants.py`; copia de seguridad y restauración por archivo; `git status` limpio
después). **Requeridos por el diseño: 16/16 muertos**; total **45/45 muertos** (la primera pasada dejó 5 vivos, cada uno
reparado con un test o reemplazado por ser equivalente; ver abajo).

| Familia del diseño | Variantes (todas muertas) |
|---|---|
| K+1 | constante `WINDOW_TOKENS=5`; rebanada izquierda +1; rebanada derecha +1 |
| ventana que filtra texto crudo | `quote=raw`; `left` tomado del turno completo; `right` tomado del turno completo |
| RESOLVED sin valor aceptado | validación del contrato retirada; chequeo del kit retirado |
| razón fuera del conjunto | validación del contrato retirada; chequeo del kit retirado; conjunto ampliado con `MY_OWN_REASON` |
| contexto que crea un RESOLVED | monotonía del kit apagada; variantes vacías; el kit ignora la decisión; `stripped()` conserva el contexto; `truncated()` conserva la izquierda |

Extras muertos (29): pureza (reloj vía módulo, vía barrido, sockets, `connect`, `get_settings`, excepción tragada),
determinismo, polaridad (ignorada / sin política), alcance declarado, ida y vuelta del kit, tipo de valor, nivel
geográfico, dimensión de la razón, campos perdidos en `encode`, etiqueta aceptada de cualquier tipo, grafía no canónica,
orden de `procedure_ids`, cota de fecha, dueño de comprador múltiple, tope top-N, nivel UNKNOWN, no-BLOCK de
`AUTHORITY_UNAVAILABLE`, efecto sin política, nivel exigido sólo en GEOGRAPHY, instante sin zona, contexto sin tope,
evidencia disjunta, cita sin ligar.

Sobrevivientes de la primera pasada y su resolución: reloj vía `dt.date` y sockets a nivel de módulo (tests nuevos del
sandbox por punto de entrada y un toy con `dt.date.today()`); ida y vuelta del kit en la rama positiva (toy cuyo valor sólo
falla ahí); etiqueta aceptada (assert de código `UNSUPPORTED_VALUE_TAG`); «ordenar `owners`» era **equivalente** (siempre
1 elemento) y se reemplazó por «ordenar `procedure_ids`», muerto.

## 6. Autopruebas del kit

3 autoridades conformes (año con calificadores del contexto que sólo retiran, modalidad con negación honrada por
`excluded` + política, geografía con nivel) y 27 NO conformes, cada una rechazada con su código esperado: contexto crea
RESOLVED / cambia el valor / rompe la cadena (INV-C0-7); reloj (`date.today`, `datetime.now`, vía módulo, tragado), socket
(`create_connection`, `connect`), `get_settings`, no determinista (INV-C0-8); lanza; razón fuera del conjunto o de otra
dimensión; RESOLVED sin valor; valor sin RESOLVED; valor fuera de la unión / de otro tipo / que no hace ida y vuelta
(INV-C0-9); etiqueta de autoridad ajena; negación ignorada, sin política o retirada por otra razón; operador o dimensión
no declarados resueltos; nivel geográfico equivocado; declaración inválida.

## 7. Rondas de reparación

2 rondas de calidad sobre mi propio código (clase A/B, dentro de presupuesto): (1) autopruebas con expectativas de código
mal calibradas (toys sin guarda de alcance; `ChainBreaker` sin guarda); (2) sobrevivientes de mutación (arriba). Ningún
hallazgo C–F.

## 8. Riesgos y límites declarados (para la revisión de METIS antes de integrar)

- **Contrato congelado, coste de cambio alto** (§8 del diseño): conviene revisar sobre todo (a) la forma de
  `AuthorityVerdict` (reglas del punto 9), (b) las restricciones de campos del punto 12, (c) `DISJOINT_EVIDENCE` (punto 5),
  (d) `ActorValue.document_kind` abierto y `TimeValue.precision` abierto.
- **K=4**: un calificador a más de 4 tokens no se ve (falla hacia aclarar si la autoridad es conservadora; la ventana no
  puede corregirlo). Un fragmento de palabra pegado a la cita (`«s»` en `«sobras»`) ocupa un hueco.
- **Pureza del kit**: sólo barre `app.*`, el módulo de la autoridad y sus bases; no ve un reloj que se capture en el
  constructor ni uno en una librería externa; `time.time()` no se vigila (también lo usaría el logging). Los sockets se
  vigilan en `connect`/`connect_ex`/`create_connection`/`getaddrinfo`/`gethostbyname*`.
- **Polaridad**: el kit exige «valor distinto + política» o `NEGATION_UNSUPPORTED`; no puede probar que el complemento sea
  el correcto: eso es de la prueba de dominio de cada carril.
- La traducción razón → CLARIFY/BLOCK (salvo `AUTHORITY_UNAVAILABLE`) y el mensaje al usuario son del núcleo en los pasos E.
- Sin enganche: `REGISTRY = ()`, `foundation_admission` no importa nada de esto; el primer comportamiento observable llega
  con el primer paso E.
- STOP §10.1: no encontré contradicción en el diseño que fije el contrato; las únicas divergencias de nombre/lugar
  (GeoLevel, tokenizador) están en los puntos 2 y 3.
