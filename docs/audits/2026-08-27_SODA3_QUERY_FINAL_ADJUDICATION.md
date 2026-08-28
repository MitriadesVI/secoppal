# SECOPPAL — SODA3 `/query`: adjudicación final

**Fecha local:** 2026-08-27
**Rama:** `experiment/soda3-discovery-v1`
**HEAD de evidencia H4:** `6841aad` (`docs(audit): validate SODA3 H4 ordering policy`)
**Alcance:** cierre del discovery de SODA3 `/query`; no es integración, shadow ni cutover.
**Veredicto final:** `GO_INTEGRATION`

## 1. Executive decision

Sí. Existe evidencia suficiente para cerrar el discovery de SODA3 `/query` y autorizar una rama
separada de integración productiva reversible.

```text
FINAL_VERDICT: GO_INTEGRATION
```

El veredicto autoriza implementar SODA3 como **transporte candidato**, inicialmente apagado y con
reversibilidad explícita. No afirma que SODA3 deba ser el transporte por defecto, no autoriza
cutover y no autoriza retirar `sodapy`.

### Intento explícito de falsación

Se intentó falsar la proposición: «SODA3 `/query` está suficientemente validado para comenzar una
integración reversible», buscando contradicciones entre V0, V1, V1.1, el microprobe y el replay H4.

- V1 contradice cualquier lectura de paridad estricta universal y falló correctamente sus gates
  originales: 18 pares exitosos no estrictos y 6 timeouts exclusivos de SODA3 impedían integrar
  bajo la configuración uniforme `orderingSpecifier=total`.
- V1.1 no borró ese resultado: adjudicó los 18 pares contra el contrato productivo y dejó un único
  bloqueador realmente alcanzable, `RT-TAB-002__call_002`.
- El microprobe reprodujo ese bloqueador y aisló una variable causal: el mismo SODA3 pasó 5/5 con
  default, `no_preserve` y `discard`, pero 0/5 con `total`.
- El replay H4 extendió el candidato conservador `no_preserve` a las 7 operaciones de la clase
  capturada: no dejó regresiones exclusivas de SODA3 alcanzables ni errores de contrato, y cerró el
  recuento de bloqueadores en cero.

No queda una contradicción material sin resolver para **iniciar una integración apagada y
reversible**. Sí quedan límites y deudas que impiden interpretar este GO como autorización de
default o cutover.

## 2. Cadena de evidencia

Los veredictos históricos se preservan; la evidencia posterior explica o acota sus hallazgos, pero
no los reescribe.

| Fase | Evidencia y resultado | Veredicto preservado |
|---|---|---|
| V0 | 16 casos controlados; señal fuerte de rendimiento y paridad estricta en 78/78 pares comparables | `GO_SODA3_QUERY` |
| V1 runtime | 26 consultas humanas, 83 operaciones y 249 pares; mantuvo la mejora, pero detectó divergencias y fallos asimétricos | `NO_GO_DIVERGENCE` |
| V1.1 | Adjudicó cuáles diferencias violaban realmente el contrato y confirmó un único blocker alcanzable | `V1_1_BLOCKERS_CONFIRMED` |
| Microprobe | Identificó `orderingSpecifier=total` como causa observada del único fallback bloqueante | `ORDERING_SPECIFIER_CAUSAL_SIGNAL` |
| Replay H4 | Validó `no_preserve` sobre las 7 operaciones H4 capturadas y dejó cero bloqueadores verdaderos | `H4_NO_PRESERVE_POLICY: PASS` |

## 3. Gates finales

| Gate | Resultado adjudicado | Estado |
|---|---|:---:|
| Compatibilidad | Ambos datasets; `query`, `count` y `aggregate`; mismo SoQL; sin errores de serialización/tipos ni HTTP contract errors | **PASS** |
| Paridad semántica | 0 divergencias verdaderas de contenido y 0 divergencias de orden semántico después de adjudicar el contrato | **PASS** |
| Rendimiento | Mejora material y repetida en V0 y V1, en contratos, procesos, `query`, `count` y `aggregate` | **PASS** |
| Estabilidad | V1: 49 timeouts clásicos frente a 28 SODA3; H4: 3 frente a 2; 0 regresiones SODA3-only alcanzables | **PASS** |
| Bloqueadores | `REMAINING_TRUE_MIGRATION_BLOCKERS = 0` | **PASS** |

V1 incluyó 27 pares `classic timeout → SODA3 ok` y 6 pares
`classic ok → SODA3 timeout`. Este balance no oculta el fallo del gate histórico: los seis fallos
desfavorables exigían adjudicación causal aunque el total favoreciera a SODA3.

## 4. Rendimiento

V0 midió 16 casos, 80 pares A/B y 78 pares comparables, todos con paridad estricta. La mejora
pareada mediana de SODA3 fue aproximadamente **59,543%**; el p90 global fue **79,509%** menor y el
p95 global **86,353%** menor. OR-100 mejoró aproximadamente **60,395%**.

V1 trasladó la señal al runtime real: 26 consultas humanas, 83 operaciones Socrata congeladas y
249 pares con SoQL idéntico 249/249. El corpus incluyó 34 `query`, 12 `count` y 37 `aggregate`. El
p90 fue **83,37%** mejor en contratos y **91,97%** mejor en procesos. Por operación, la mejora
pareada mediana fue **36,697%** para `query`, **84,064%** para `count` y **41,139%** para
`aggregate`.

La mejora es material, repetida y transversal. No se reduce a un único número ni compensa por sí
sola fallos semánticos: los gates de paridad y estabilidad se evaluaron de forma independiente.

## 5. Compatibilidad

La evidencia cubre los dos datasets productivos evaluados (`jbjy-vk9h` y `p6dx-8zbt`) y las tres
formas remotas usadas por el runtime: `query`, `count` y `aggregate`. V1 conservó el SoQL exacto en
249/249 pares. No se observaron divergencias de serialización o conversión de tipos, respuestas con
forma inválida ni HTTP 4xx/5xx atribuibles al contrato probado.

El replay H4 cubrió 7 operaciones y 42 requests. El clásico obtuvo 18/21 éxitos y 3 timeouts;
SODA3 con `orderingSpecifier=no_preserve`, 19/21 éxitos y 2 timeouts, con 0 HTTP contract errors.
Los dos timeouts SODA3 restantes pertenecen a
`RT-PHRASE-002__call_002`; el clásico agotó 3/3 en esa misma operación y SODA3 respondió 1/3, por
lo que no existe regresión exclusiva de SODA3 allí.

## 6. Adjudicación de divergencias

Los 18 pares exitosos no estrictos de V1 quedaron clasificados contra el contrato productivo:

| Clasificación | Pares | Adjudicación |
|---|---:|---|
| `ORDER_TIE_NON_SEMANTIC` | 9 | Empates que no violan el orden semántico prometido |
| `UNORDERED_SAMPLE_EXPECTED_VARIANCE` | 3 | Varianza válida de una muestra H4 sin orden garantizado |
| `TOPN_BOUNDARY_TIE_DEBT_POST_MIGRATION` | 6 | Deuda conocida, post-migración y no bloqueante |
| `PRIMARY_TRUE_CONTENT_DIVERGENCE` | 0 | Ninguna |
| `SEMANTIC_ORDER_DIVERGENCE` | 0 | Ninguna |

El TOP-N determinístico queda expresamente como `KNOWN_DEBT`, `POST_MIGRATION` y `NON_BLOCKING`;
no debe arreglarse como precondición de integración.

Las regresiones de fallback también quedaron adjudicadas:

- `RT-NEG-002__call_002`: V1 observó timeout SODA3 3/3, pero su primary SODA3 respondió 3/3. El
  fallback sería inalcanzable en un runtime SODA3 real:
  `FALLBACK_UNREACHABLE_WITH_SODA3_PRIMARY`. No bloquea.
- `RT-TAB-002__call_002`: era el único blocker alcanzable. El microprobe obtuvo clásico 5/5,
  SODA3 `total` 0/5, SODA3 default 5/5, `no_preserve` 5/5 y `discard` 5/5. La variable causal
  observada fue `orderingSpecifier=total`, no una incapacidad intrínseca de SODA3. En el replay H4,
  SODA3 `no_preserve` pasó 3/3.

Resultado contractual final:

```text
TRUE_CONTENT_DIVERGENCES: 0
SEMANTIC_ORDER_DIVERGENCES: 0
SODA3_ONLY_REACHABLE_REGRESSIONS: 0
REMAINING_TRUE_MIGRATION_BLOCKERS: 0
```

## 7. Política de ordering derivada

La futura integración deberá conservar la política ya probada por V1 para las operaciones normales
y usar `orderingSpecifier=no_preserve` en el fallback H4 donde el runtime retira deliberadamente
el `ORDER BY`.

La selección de política debe recibir la **intención semántica de ejecución** desde la ruta o el
contrato correspondiente. Queda prohibido inferir H4 mediante una regla como:

```text
if "ORDER BY" not in soql:
    no_preserve
```

La ausencia de `ORDER BY` también aparece legítimamente en `count`, `aggregate`, direct lookup y
otras operaciones que no son H4. Este dictamen no diseña todavía cómo se representará esa señal en
código.

## 8. Riesgos y deuda aceptada

Ninguno de estos puntos bloquea el inicio de una integración reversible, pero todos limitan su
alcance:

1. TOP-N determinístico queda pendiente para después de la migración.
2. SODA3 no elimina todos los timeouts: V1 registró 28 y el replay H4 registró 2.
3. `/export` no fue probado ni queda autorizado por esta evidencia.
4. La paginación SODA3 no ha sido validada suficientemente como proyecto independiente.
5. Aún no existen cutover, shadow ni dogfood productivo SODA3.
6. `sodapy` debe permanecer disponible.
7. La integración debe ser reversible y comenzar apagada.
8. La política de ordering debe respetar el contrato H4 y no depender de inspección heurística del
   texto SoQL.

## 9. Qué autoriza el veredicto

`GO_INTEGRATION` cierra el discovery de `/query` y autoriza abrir una rama separada para implementar
SODA3 como transporte candidato, con activación inicialmente apagada, reversibilidad y observación
explícitas. Autoriza trasladar a código la política adjudicada, preservando la intención semántica
de la ruta H4.

## 10. Qué NO autoriza

Este veredicto no autoriza:

- hacer cutover o convertir SODA3 en default;
- retirar `sodapy` ni el transporte clásico;
- activar SODA3 en producción, shadow o dogfood;
- modificar TOP-N determinístico como requisito previo;
- asumir soporte de `/export` o paginación no demostrada;
- diseñar la integración dentro de esta rama de discovery;
- seleccionar `no_preserve` solo porque el SoQL carece de `ORDER BY`.

## 11. Próximo bloque exacto

Abrir una rama separada de integración para implementar SODA3 `/query` como transporte candidato
inicialmente apagado y reversible, sin cutover ni retiro de `sodapy`.
