# M2A — Preparación de la ola 1: decisiones del usuario (2026-10-02)

**Rama:** `mission/m2a-wave1-prep` (desde `6be95aaf`). Sólo tests y docs; nada bajo `app/`. Este documento registra
decisiones; no edita el contrato (`docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md`) ni el Gold sellado. El plan
(`M2A_IMPLEMENTATION_PLAN.md`) quedó enmendado en §0.6–§0.8, filas M2A-2, M2A-3, M2A-6 y M2A-8, y §2.

## 1. Registros por paso

Los módulos M2a se registran en `tests/m2a_steps/m2a_<n>.py`, uno por paso (`STEP`, `MODULES`, `DEPENDS_ON`,
`CLOSURE`). Cada carril (M2A-2, M2A-3) escribe **sólo su archivo**: ninguna lista central compartida. `CLOSURE` es el
cierre transitivo exacto de `MODULES` **menos** el de los pasos de los que depende (su incremento propio). Un cambio en
M2A-1 no toca el registro de M2A-2, **salvo que M2A-1 y M2A-2 alcancen el mismo módulo X, y entonces rige en los dos
sentidos, no sólo cuando ambos crecen**:

- Si M2A-1 **crece** hacia X (X deja de ser incremento de M2A-2), M2A-2 falla de forma segura con `closure lost X`.
- Si M2A-1 **encoge** y deja de alcanzar X (X pasa a ser incremento propio de M2A-2), M2A-2 falla de forma segura con
  `closure reaches X`. Es la dirección que la primera redacción de esta sección no decía (P3 del ZEUS del Paso 0).

En ambos casos el fallo no es silencioso y el arreglo es local: el dueño de M2A-2 quita (o añade) X en su propio
`CLOSURE`; el registro de M2A-1 no se toca. Ambos sentidos tienen pin
(`test_when_two_steps_grow_towards_the_same_module_…` y `test_when_the_foundation_step_shrinks_…`).

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

**Tres evasiones por ofuscación deliberada, también garantía limitada** (la lista negra **no** se amplía; cada grafía se
corrió contra `source_problems` y contra el grafo). Qué se caza y qué no, verificado:

| Evasión | Forma llana (se caza) | Forma que evade |
|---|---|---|
| `importlib.util.spec_from_file_location` | `import importlib.util` y `from importlib.util import spec_from_file_location` fallan como maquinaria de importlib | `sys.modules['importlib.util'].spec_from_file_location(nombre, ruta)`: carga por **ruta de archivo**, sin sentencia de import ni nombre con puntos; ni el escáner ni el grafo lo ven |
| `getattr(importlib, 'import_module')` | `import importlib` + `getattr(importlib, 'import_module')` falla (maquinaria y literal `import_module`) | `getattr(sys.modules['importlib'], 'imp' + 'ort_module')` o el nombre en una variable: no escribe el nombre que el escáner busca. El grafo sigue resolviendo como arista un destino **literal** con puntos; sólo un destino construido o variable queda invisible |
| `runpy.run_module` (y `runpy.run_path`) | no hay | `runpy` no está en ninguna lista: `import runpy` no se marca. Con destino literal con puntos el grafo resuelve la arista; con destino construido o variable, nada lo ve |

El cierre estático es completo frente a código honesto y a las grafías llanas, **no** frente a un módulo escrito para
esconder sus imports: eso es asunto de revisión, no de un pin. Los tres están dichos también en el docstring de
`tests/test_m2a_boundaries.py`.

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

- **Por función** (363 mutantes: manifiesto 4, paquete de registro 101, contrato de autoridad 45, fronteras 213): de a
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

**Reproducibilidad de estas cifras (P3 del ZEUS del Paso 0).** Los números de esta sección (363 mutantes por función
con sus 3 equivalentes, 74 por constante, 48 de 50 de los scripts de ZEUS) son el resultado de una corrida con arneses
que **no están en el árbol**: ni el generador de mutantes por función ni `mut.py`/`mut2.py` están versionados (se buscó
en `tests/`, `scripts/` y `docs/`; sólo aparecen citados aquí). **No se pueden reproducir desde el commit.** Lo que el
árbol sí conserva y puede ejecutarse es lo que está escrito como tests: las baterías de mutantes de
`tests/test_m2a_boundaries.py` y `tests/test_foundation_authority_contract.py`, y los pines de
`tests/test_legacy_manifest_frozen.py`. Léase la meta-mutación como evidencia citada de una corrida, no como una
propiedad verificable hoy.

Lo que la primera pasada sistemática destapó (y ya tiene mutante): argumentos, keywords, alias, `async def`, clases y
`resolve()` sin atributo en `source_problems`; `**kwargs`, import relativo y fuente vacía sin romper el escáner; el
indicador de import dinámico de un override; la resolución de imports relativos de paquetes; un paso que depende de uno
inexistente; el autoimporte `foundation_geo_level`; `_module_name` de un `__init__.py`; primer enunciado que no es un
docstring en un archivo de registro.

## 10. Los carriles llenan sus registros (R2)

Los registros de M2A-2 y M2A-3 están hechos para que los carriles los llenen; **ningún test de esta preparación puede
depender de lo que un carril tenga escrito**. La composición real (R1 + M2A-2 + M2A-3) dejó dos tests míos en rojo porque
derivaban sus pasos sintéticos de los registros reales y suponían que estaban vacíos. Lo que sí puede asumirse es
`m2a_1.py`: es la fundación. Reparación:

- **Mundo sintético** (`WORLD` en `tests/test_m2a_boundaries.py`): M2A-1 real y dos pasos de carril **vacíos** con el DAG
  revisado. Todo mutante se construye sobre él; ninguno reutiliza un paso real de carril. `_graph(..., steps)` pone en
  blanco los archivos reales que el registro real registra y el mundo no, de modo que ni lo que un carril importa ni lo que
  deja de importar cambia el veredicto de un mutante. Con el registro real no se blanquea nada.
- **Mutantes de contrato de autoridad** (`tests/test_foundation_authority_contract.py`): leen **sólo** sus fuentes
  virtuales (`_only`), nunca el árbol real; los pines del árbol real siguen leyéndolo.
- **Registros simulados llenos**: un M2A-2 con un módulo que importa `foundation_accepted` y `store_lock`, y un M2A-3 con
  dos módulos sobre el vocabulario del IR v2. Todos los pines quedan en verde (también los archivos de registro, que se
  generan y se validan), y 25 mutantes (`len(SIM_MUTANTS)` en `tests/test_m2a_boundaries.py`; una versión anterior de este
  documento decía 24) siguen cazando lo suyo sobre esos registros.
- **Composición real** (merge desacoplado de R2 + `c6ca1ac6` + `f30e91ff`, sin commitear nada ajeno): todos los pines de
  esta preparación pasan; los fallos que quedan son los fixtures de `tests/test_foundation_accepted_store.py` frente al tipo
  canónico de M2A-3 y se resuelven en la integración (ver el reporte de R2).

## 11. Escáner de privacidad: falso positivo preexistente (F6, integración de la ola 1)

`privacy_scan_public_tree.py <GOLD_CANDIDATE privado> <copia de git diff --name-only 6be95aaf HEAD>` (74 archivos de la
integración, copiados fuera del repo) sale con **1**, con **dos** bloqueantes y ninguna otra clase (ni `ACTOR`, ni
`DOCUMENT`, ni `EMAIL`):

| Archivo | Hallazgo |
|---|---|
| `app/core/conversation_store.py:11` | `FULL_UTTERANCE:6tok` |
| `tests/fixtures/conversation_store_main_88653752.py.txt:11` | `FULL_UTTERANCE:6tok` |

Es el argumento `user_query` del ejemplo de uso del docstring de `ConversationStore` (líneas 7–14), **no texto REAL**. El
texto no se cita aquí: como el escáner, este documento sólo da archivo, línea y número de tokens.

**Evidencia (ejecutada).**

1. **Preexistente.** `git log -S` sitúa la primera aparición del literal en `2479d853` (2026-05-15,
   «wip: snapshot pre-audit P0»). La línea 11 es idéntica en `2479d853`, en `88653752` (el manifiesto de `main`), en la
   base `6be95aaf` y en HEAD; la integración no la cambia.
2. **Anterior al Gold.** Los archivos privados `*_real.jsonl` y el commit que sella el Gold (`88da4e22`) son del 2026-10-01: el texto
   público es unos cuatro meses y medio anterior, así que no puede derivar del Gold. Lo que el escáner detecta es que un
   enunciado REAL de 6 tokens coincide palabra a palabra con esa frase del ejemplo, no que M2a la haya sacado del Gold.
3. **La fixture es una copia.** `git show 88653752:app/core/conversation_store.py | cmp - tests/fixtures/conversation_store_main_88653752.py.txt`:
   idénticos byte a byte; el hallazgo de la fixture es el mismo texto.
4. **Por qué sale ahora.** Los cierres anteriores escaneaban «los archivos del commit» (limpio). Aquí el diff contra la
   base incluye `app/core/conversation_store.py` (M2A-2 sólo cambió el delta de `clear()`) y la fixture (la añadió M2A-2),
   y la línea 11 viene con ellos.

**Por qué no hay lista de permitidos.** El escáner no admite ninguna: su interfaz es `<privado> <público> [--json]`,
recorre el directorio entero con `rglob`, sale con 1 ante cualquier bloqueante y sólo informa archivo y número de tokens
(ni siquiera la línea). No hay permiso por ruta, por línea ni por hash. No se modifica el escáner (sería un cambio de
herramienta fuera de este alcance) y **no se tocan** `conversation_store.py` ni la fixture: el delta de `clear()` se fija
por AST contra esa fixture (`tests/test_conversation_store_clear_delta.py`) y el manifiesto de `main` fija los bytes del
módulo (`tests/test_legacy_manifest_frozen.py`).

**Regla para quien corra el escáner sobre este diff.** El resultado esperado es exactamente esos dos bloqueantes con
`FULL_UTTERANCE:6tok` y nada más. Cualquier tercer archivo, o una clase `ACTOR`/`DOCUMENT`/`EMAIL`, es un bloqueo real. Los
dos `FULL_UTTERANCE:4tok` de `docs/audits/m2a_conversation/M2A_TRACEABILITY_MATRIX.md` y
`docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` se informan y no bloquean por diseño (≤ 4 tokens, enunciados genéricos);
ambos archivos ya estaban en la base.

**Deuda (propuesta, sin decidir).** Si este escaneo va a ser un gate recurrente sobre diffs que incluyan archivos
heredados, el escáner necesitaría una lista explícita y estrecha (ruta + sha256 del archivo, con motivo). Es un cambio de
herramienta y lo decide quien la mantiene.
