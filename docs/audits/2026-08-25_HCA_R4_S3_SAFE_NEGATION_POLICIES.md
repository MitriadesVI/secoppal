# HCA-R4-S3 — Safe negation policies

**Estado:** PASS

**Base:** `7ae03e9b01e3a93f67bdbebab37e2afa174eb0c8`

**Rama:** `feature/hca-r4-s3-safe-negation-policies`

## Dictamen

```text
HCA_R4_S3_SAFE_NEGATION_POLICIES: PASS

SUPPORTED:
  OBJECT_EXECUTION: PASS
  MODALITY_EXECUTION: PASS

CITY:
  BINDING: PASS
  POLICY: CLARIFY
  POSITIVE_LEAKS: 0
  CITY_TO_ENTITY_LEAKS: 0
  SOURCE_CALLS: 0

TIME:
  BINDING: PASS
  POLICY: CLARIFY
  POSITIVE_LEAKS: 0
  SOURCE_CALLS: 0

BUYER:
  POLICY: CLARIFY
  POSITIVE_LEAKS: 0
  SOURCE_CALLS: 0

ACTOR:
  POLICY: CLARIFY
  ACTOR_PENDING_CREATED: 0
  SOURCE_CALLS: 0

DEPARTMENT:
  POLICY: SET_SUBTRACTION_IF_SAFE_ELSE_CLARIFY
  UNSAFE_NEGATIVE_PREDICATES: 0

TRANSACTIONALITY:
  CLARIFY_MUTATES_CONFIRMED_CONTEXT: NO
```

## Inventario as-built de S2

La tabla se reconstruyó con `QueryRouter`, acciones Burr, persistencia y runner
real; no sólo desde ADR-064.

| dimensión | binding S2 | ejecución S2 | policy S2 | riesgo observado antes de S3 | target y resultado S3 |
|---|---:|---:|---|---|---|
| OBJECT | sí | sí | `EXECUTE` | ninguno material | `EXECUTE`, intacto |
| MODALITY | sí | sí | autoridad especializada | ninguno material | `EXECUTE`, intacto |
| CITY | sí | no | `DEFER` + bloqueo | el post-proceso podía abrir ecosistema sobre el scope positivo; follow-up persistía `{}` | `CLARIFY`, sin ecosistema ni fuente |
| DEPARTMENT | sí | no | `DEFER` | la resta segura existente no se alcanzaba | resta sólo sobre `departamentos_in`; en otro caso `CLARIFY` |
| TIME | sí | no | `DEFER` + bloqueo | follow-up persistía `{}`; contradicción no se distinguía | `CLARIFY` o `CONTRADICTORY` |
| BUYER | sí | no | `DEFER` + bloqueo | sin ejecución, wording genérico | `CLARIFY`, sin resolución positiva |
| ACTOR | sí | no | `DEFER` + bloqueo | resolvedor de actor estaba antes del plan | `CLARIFY`, resolvedor/directorio inactivos |
| CONTRACT TYPE | sí | no | `DEFER` + bloqueo | sin fuga positiva | `CLARIFY` |
| STATE | sí | no | `DEFER` + bloqueo | sin fuga positiva | `CLARIFY` |

El bloqueo de S1/S2 ya evitaba llamadas SECOP en los casos simples, pero no era
transaccional extremo a extremo: `apply_context` restauraba `parsed_params` y
dejaba `resolved_params` vacío; `normal_flow` persistía este último y el turno
siguiente podía perder el frame confirmado. Además, la expansión de ecosistema
post-query no estaba condicionada por aclaración. S3 cierra ambas costuras.

## Policy mínima

`NegationRequest.verdict` sigue siendo la única decisión efímera:

```text
bound request
  → OBJECT / MODALITY                         → EXECUTE
  → DEPARTMENT + departamentos_in canónico   → SET SUBTRACTION → EXECUTE
  → contradicción directa                    → CONTRADICTORY → CLARIFY
  → resto                                    → CLARIFY
```

`app/core/negation_policy.py` evalúa el conjunto completo como una transacción.
Si una petición bloquea, revierte cualquier resta departamental tentativa. No
crea AST, campos `*_exclude`, predicados `NOT LIKE`/`NOT IN` ni un segundo plan.

DEPARTMENT alcanzó la capacidad segura porque el runtime ya materializa regiones
como listas finitas de valores oficiales. Por ejemplo:

```text
Caribe = [Atlántico, Bolívar, Cesar, Córdoba, La Guajira, Magdalena, Sucre]
menos Atlántico
→ departamento IN (Bolívar, Cesar, Córdoba, La Guajira, Magdalena, Sucre)
```

Una exclusión sin `departamentos_in`, un set corrupto/no canónico o una resta
que lo vacíe aclara. `SoQLBuilder` no cambió.

## Fail-closed, fuente y side effects

Las dimensiones no soportadas llegan al plan como `CLARIFY`; la respuesta dice
que la exclusión fue entendida y por qué no se puede ejecutar con precisión. No
se presenta como consulta incomprendida, cero resultados ni error de SECOP.

Antes del plan se suprimen el fallback LLM y la resolución de actor para estas
peticiones. La expansión `_CITY_TO_ENTITY`/ecosistema no corre en aclaraciones.
Spies end-to-end sobre CITY, TIME, BUYER y ACTOR verifican:

```text
SecopClient.query = 0
SecopClient.count = 0
LLM.parse = 0
ActorNameResolver.resolve = 0
ActorNameResolver.resolve_exact = 0
actor_pending = absent
```

El adversarial detectó y cerró una fuga adicional: `proveedor pero no X` se
clasificaba como OBJECT y dejaba `actor_scope=pero`. El conector contrastivo se
enmascara ahora junto al span ACTOR negativo; ciudades y actores positivos no
cambian.

## Transaccionalidad conversacional

En follow-up, la decisión se reevalúa contra el frame confirmado porque un
`departamentos_in` seguro puede provenir del turno anterior. Si la petición
aclara, `parsed_params`, `resolved_params` y el snapshot persistido son copias
idénticas del frame previo. Tampoco se invalida su buffer de paginación.

Se verificaron las secuencias:

```text
T1 contratos de aseo en Atlántico
T2 menos Barranquilla

T1 contratos desde 2024
T2 menos los de 2025
```

En ambos casos: contexto antes = contexto después, llamadas SECOP de T2 = 0.
La resta departamental sí funciona como follow-up sobre una región confirmada.

## HCA-NEG-005

Clasificación: **C — el runner no observa la nueva semántica completa**.

El oráculo holístico sólo exige `objeto_not: [alimentacion]`. Desde R4-S1 el
enmascaramiento ya impedía que el término apareciera como positivo, por lo que
el caso era PASS antes de R4-S2. S2 añadió la semántica material
`objeto_exclude=[alimentacion]` y el SoQL NULL-safe, pero el caso no exige el
campo ni el predicado; por eso su status y el score global no podían cambiar.
No es un bug downstream de OBJECT.

## Corpus y regresión

Los dos fixtures conservaron bytes y SHA-256:

- `negation_focal_v1.yaml`:
  `f4f64a33ea7009eb1da9fa7d3c910e67010e6dafc6b4c5b6932901c27fad9a1d`.
- `conversation_holistic_v1.yaml`:
  `262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba`.

Resultados:

- preregistrado: **19 PASS / 5 FAIL / 0 ERROR → 21 PASS / 3 FAIL / 0 ERROR**;
  cambian a PASS `NEG-N3B` (resta regional) y `NEG-N9` (contradicción temporal);
- HCA holístico: **164 PASS / 31 FAIL / 0 ERROR → 164/31/0**; no cambia ningún
  status, pero `HCA-NEG-003` y `HCA-NEG-006` reciben aclaración veraz;
- fallos preregistrados restantes:
  - `NEG-N11B`: retirar una exclusión, lifecycle reservado a R4-S4;
  - `NEG-N15`: la modalidad se resuelve correctamente, pero el guard de scope
    general bloquea una búsqueda sólo por modalidad; deuda preexistente ajena;
  - `NEG-ANTI-002`: el runner compara `parsed_params.entidad_resolved`, aunque
    esa entidad positiva se proyecta después; limitación observacional existente.

No se modificó ningún oráculo para maquillar el score.

El runner holístico emitió cuatro avisos `402 Insufficient Balance` tanto antes
como después y aplicó su fallback regex-only previsto en casos ajenos; terminó
con `ERROR=0`. Se conserva esta limitación ambiental como evidencia, sin
ocultarla ni atribuirle cambios de status.

## Pruebas

- policy/binding/OBJECT focal: PASS;
- modalidad: **438 passed**;
- money: **218 passed**;
- geografía/entidad: **186 passed**;
- actor/follow-up: **160 passed**;
- adversarial: 30 formas de CITY/TIME/BUYER/ACTOR, combinaciones ortogonales y
  spies de fuente/contexto: PASS;
- changed-file Ruff: PASS;
- `make lint-core`: PASS;
- `git diff --check`: PASS;
- suite amplia final: **8 412 passed, 2 skipped, 23 xfailed**.

## Invariantes cerrados

- `INV-NEGATION-PRESERVES-DIMENSION`: PASS.
- `INV-NO-SILENT-NEGATION-DROP`: PASS.
- `INV-UNSUPPORTED-NEGATION-DOES-NOT-MUTATE-QUERY`: PASS.
- `INV-NEGATION-CLARIFY-IS-TRANSACTIONAL`: PASS.
- `NEGATIVE_CITY_CAN_TRIGGER_ENTITY_ECOSYSTEM = FALSE`: PASS.
- `NEGATION_CLARIFICATION_EXECUTES_SECOP = FALSE`: PASS.
- `NEGATIVE_ACTOR_CAN_CREATE_PENDING_CANDIDATES = FALSE`: PASS.

## Diferido a R4-S4

- lifecycle de exclusiones: retirar/reincluir (`quita la exclusión`, `ahora
  incluye X`);
- limpieza explícita de una exclusión heredada;
- cualquier ampliación futura de ejecución CITY/TIME/BUYER/ACTOR/STATE/
  CONTRACT TYPE requiere una decisión separada; S3 no la presupone.
