# M2A — Preparación de la ola 1: decisiones del usuario (2026-10-02)

**Rama:** `mission/m2a-wave1-prep` (desde `6be95aaf`). Sólo tests y docs; nada bajo `app/`. Este documento registra
decisiones; no edita el contrato (`docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md`) ni el Gold sellado. El plan
(`M2A_IMPLEMENTATION_PLAN.md`) quedó enmendado en §0.6–§0.8, filas M2A-2, M2A-3, M2A-6 y M2A-8, y §2.

## 1. Registros por paso

Los módulos M2a se registran en `tests/m2a_steps/m2a_<n>.py`, uno por paso (`STEP`, `MODULES`, `DEPENDS_ON`,
`CLOSURE`). Cada carril (M2A-2, M2A-3) escribe **sólo su archivo**: ninguna lista central compartida. `CLOSURE` es el
cierre transitivo exacto de `MODULES` **menos** el de los pasos de los que depende (su incremento propio). Un cambio en
M2A-1 no toca el registro de M2A-2, **salvo que ambos crezcan hacia el mismo módulo**: entonces M2A-2 falla de forma
segura (`closure lost X`, nunca en silencio) y su dueño quita X de su propio `CLOSURE`.

El registro se valida (`registry_problems`): módulos bajo `app.core.`, existentes, **nuevos** (no figuran en el manifiesto
de `main` ni en `POST_MAIN_EXISTING`, los 9 módulos que ya existían en la base de esta preparación y no están en él), y
registrados una sola vez; pasos únicos; `DEPENDS_ON` conocido, no propio, sin ciclos. Un archivo de registro es **datos**:
un docstring y las cuatro asignaciones literales, sin imports ni lógica (`file_problems`).

El **DAG de la ola es una constante revisada** (`PLANNED_DEPENDENCIES` en `tests/test_m2a_boundaries.py`: M2A-2 → {M2A-1},
M2A-3 → {M2A-1}). Cada carril declara su propio `DEPENDS_ON`, así que éste no puede ser la autoridad de lo que el paso
puede importar: ampliarlo (p. ej. que el reducer importe el store) es editar esa constante, una decisión explícita.

## 2. Invariante definitiva de consumo (sustituye `test_m2a_has_no_consumer_yet`)

El consumo interno de M2a está permitido **únicamente entre módulos explícitamente registrados** y a lo largo de
`DEPENDS_ON`. Ningún módulo externo (`app/`, `scripts/`, `streamlit_app`), el cierre legacy ni el runtime Foundation
alcanza M2a. Registrar un módulo no abre ninguna puerta: un módulo legacy registrado sigue prohibido en el cierre y sigue
siendo consumido por legacy. Pin: `tests/test_m2a_boundaries.py`.

**Garantías limitadas** (decisión previa del usuario: no ampliar la lista negra): `consumer_problems` no ve
`getattr(app.core, "foundation_accepted")` (acceso por atributo a un objeto paquete) ni un import dinámico no resoluble
(`importlib.import_module(nombre)` con variable) en un módulo **no registrado**; dentro del cierre registrado este último
sí falla. La regla «ya existía» del registro lee el manifiesto de `main` y una lista congelada: un módulo creado después
por otra persona y registrado por un carril no se ve como preexistente.

## 3. Enmienda C1.x (`tests/test_foundation_authority_contract.py`)

Antes: nada bajo `app/` importa `foundation_accepted`. Ahora: **los importadores bajo `app/` ⊆ módulos M2a registrados**.
`CLOSED_TYPE_CONSUMERS == {"foundation_accepted"}` se mantiene: los demás módulos M2a **nunca** importan
`foundation_authority_values` ni `foundation_geo_level` directamente, decodifican vía `foundation_accepted`; registrarse no
concede el consumo de tipos cerrados. Se mantienen el resto de comprobaciones (sin `resolve`, sin módulos de autoridad).

## 4. `foundation_accepted.py`: adyacente exclusivo de M2A-3

`app/core/foundation_accepted.py` es adyacente **exclusivo** de M2A-3, sólo para invariantes y canonicidad compartidas
con el futuro M2A-1b. M2A-2 lo importa y **no** lo modifica. Nada se ordena a ciegas: primero se demuestra qué
colecciones son set-like y cuáles tienen orden observable. Las policies duplicadas sí se rechazan.

## 5. Carril M2A-8a0 (oráculo del reducer)

Autor distinto del de M2A-3. El oráculo no importa ni llama al reducer. Se congela **antes** de implementar `compose`.
M2A-3 itera sólo contra **DEV**; **TEST** se usa al congelar.

Decisión del orquestador (R1): `scripts/` es externo a M2a y no puede importarlo. El oráculo vive en
`scripts/audits/m2a_conversation/` y trabaja **sólo sobre dicts**; el enlace con los tipos M2a y con `compose` (el
adaptador) vive en `tests/`. El plan (filas M2A-3 y M2A-8, carril M2A-8a0, §0.8) quedó reconciliado con eso.

## 6. El contrato §6.6 no autoriza `foundation_admission`

§6.6 del contrato (las políticas de petición se re-deciden sobre la efectiva compuesta) **no** autoriza importar ni copiar
`foundation_admission`. Toda política que dependa de E-\*/F queda **fail-closed con dueño declarado** hasta que exista su
autoridad.

## 7. Regla del manifiesto

`tests/fixtures/legacy_closure_manifest_88653752.json` **no** se regenera ni se edita para legitimar un módulo legacy
tocado. El módulo se declara como divergencia explícita (`ALLOWED_DIVERGENCE` de `tests/test_m1_wave1_parity_pins.py`) y se
protege con un pin que sólo permita el delta declarado (para M2A-2, el de `ConversationStore.clear()`; patrón del pin 6 de
`foundation_guard`: fuente de `main` como fixture + comparación AST).

Razón verificable: `test_the_allowed_divergences_are_real_and_inside_the_closure`
(`tests/test_m1_wave1_parity_pins.py:956-963`) exige que toda `ALLOWED_DIVERGENCE` difiera del manifiesto. Regenerar el
manifiesto **entero** rompe los pines 4–6; editar **una sola entrada** (la de `conversation_store.py`) dejaba todo en verde
porque los demás pines recalculan contra lo que diga el archivo, y lo impide ahora `tests/test_legacy_manifest_frozen.py`,
que fija el sha256 de los bytes del archivo. Cambiar el manifiesto exige editar esa constante: una decisión explícita.

**Supersesión (decisión del usuario, 2026-10-02).** La frase procedimental «exige regenerar el manifiesto con motivo
declarado» del contrato (`M2A_CONVERSATION_CONTRACT_V1.md:202-203`; el contrato **no** se edita, por el sello), de ADR-103
§4.6 y del plan queda sustituida por esta decisión: divergencia explícita + pin del delta. Ampliar `ALLOWED_DIVERGENCE` con
`conversation_store` para M2A-2 **está autorizado** por ella, y precisa ADR-103 D1(b) («lista explícita y mínima»): la lista
sigue siendo explícita y mínima, y cada entrada nueva trae su pin de delta.

## 8. Deuda separada

`DEBT_CLEAR_GENERATIONS_RETENTION.md`: `ConversationStore.clear()` no borra `.generations/<chat>/`. Preexistente; M2A-2 la
caracteriza y no la repara sin autorización del usuario.

## 9. Evidencia de los pines de esta preparación

Cada pin nuevo o cambiado trae mutantes que caza y controles positivos (registro malformado o con lógica, módulo
preexistente, DAG ampliado, cierre con un elemento de menos o de más, cada prefijo prohibido con un `CLOSURE` exacto, paso
que importa a otro sin `DEPENDS_ON`, módulo no registrado que importa M2a, legacy y runtime que lo alcanzan, módulo
registrado que importa `foundation_authority_values`, módulo con el mismo nombre en otro directorio, manifiesto editado).
Los mutantes usan módulos **virtuales** (fuentes en memoria): ningún mutante crea archivos.

**Meta-mutación (corrección de R1).** La primera versión de esta sección decía que se neutralizó cada comprobación; no era
cierto: la cláusula `name.startswith(FORBIDDEN_PREFIXES)` y la condición `relative == f"app/core/{stem}.py"` sobrevivían.
Ahora la meta-mutación es **sistemática** y se corre sobre el candidato:

- **Por función** (354 mutantes: manifiesto 4, paquete de registro 101, contrato de autoridad 45, fronteras 204): de a
  uno, cada `if` e `if` en expresión (a `False` y a `True`), cada operando de `and`/`or`, cada filtro de comprensión,
  cada comparación invertida, cada resta o intersección de conjuntos, cada `not`, cada `append`/`extend`/`add` y el cuerpo
  entero. Los mutantes sobre anotaciones de tipo se excluyen (equivalentes). **3 supervivientes, los tres equivalentes:**
  `_docstring_nodes` (segundo operando: sólo añade a un conjunto el id de un nodo que no es `Constant`, y el conjunto sólo
  se consulta con `Constant`); `source_problems` (`node.module or ""`: la lista que le sigue ya contiene el módulo, porque
  un `from … import` tiene al menos un alias); `steps_from_namespaces` (`if step is not None` siempre verdadero: el
  `ValueError` posterior descarta la lista).
- **Por constante** (74 mutantes, 0 supervivientes): quitar de a uno cada elemento de `FORBIDDEN_MODULES`,
  `FORBIDDEN_PREFIXES`, `FORBIDDEN_SUFFIXES`, `FORBIDDEN_IDENTIFIERS`, `REGEX_MODULES`, `LEXICON_MARKERS`,
  `DYNAMIC_IMPORT_*`, `POST_MAIN_EXISTING`, `PLANNED_DEPENDENCIES` (también cambiando sus valores), las dos mitades de
  `PREEXISTING`, `NEW_RUNTIME_MODULES` y `GEO_LEVEL_IMPORTERS`. Las listas de los mutantes están escritas a mano, no
  derivadas de la constante: si no, quitar un elemento también lo quitaba de su propio test.
- **Scripts de ZEUS** (`mut.py`, `mut2.py`): 48 de 50 caen; los dos que sobrevivían (`load_steps` no levantaba error y el
  escaneo de `app/` limitado a `app/core`) tienen ahora su test y caen.

Lo que la primera pasada sistemática destapó (y ya tiene mutante): argumentos, keywords, alias, `async def`, clases y
`resolve()` sin atributo en `source_problems`; `**kwargs`, import relativo y fuente vacía sin romper el escáner; el
indicador de import dinámico de un override; la resolución de imports relativos de paquetes; un paso que depende de uno
inexistente; el autoimporte `foundation_geo_level`; `_module_name` de un `__init__.py`; primer enunciado que no es un
docstring en un archivo de registro.
