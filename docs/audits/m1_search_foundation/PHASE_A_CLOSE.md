# M1_SEARCH_FOUNDATION_V1 — cierre de la Fase A

Fecha: 2026-09-30. Rama `mission/m1-search-foundation-phase-a` (base `main` @ `4ec99bc1`).
Orquestador (HERA): Opus. Nada de esta fase toca `app/`.

## Entregables

| Gate §9 | Estado | Evidencia |
|---|---|---|
| 1. Gold v1 etiquetado y validado | ✅ 999 registros (620 REAL privados + 111 NEIGHBOR + 176 MINIMAL_PAIR + 92 ADVERSARIAL) | `gold/GOLD_SEAL.json` |
| 2. Segundo etiquetado ciego, acuerdo, cola | ✅ REAL 365 dobles (κ op 0,979); sintéticos 379 dobles (κ 0,959); 109 desacuerdos → contrato v1.3/v1.4 + decisiones | `M1_B_ADJUDICATION.md`, `gold/agreement_*.json` |
| 3. Authority Health | ✅ 7 FIX_REQUIRED, 1 HEALTHY, con verificación de punta a punta | `A2_AUTHORITY_HEALTH.md` (+ anexo HERA) |
| 4. Gap matrix GOLD/LEGACY/FOUNDATION | ✅ | `A3_GAP_MATRIX.md/.json` |
| 5. Fronteras de capacidad | ✅ | `A4_CAPABILITY_BOUNDARIES.md` |
| 6. Spec de failover | ✅ | `A5_PROVIDER_FAILOVER_SPEC.md` |

Contrato vigente: `M1_0_SEARCH_CONTRACT.md` v1.4 (§1–§10 + enmiendas §11–§14). Product views del
Gold verificadas al 100 % contra la función PV1–PV6. **[v1.8: el Gold sellado vigente tiene 1005 registros (+6 pares
ACTOR × STATE) y P034 deja de seguir PV2 por decisión de producto; ver contrato §18.]**

## Lo que dice la evidencia

SEARCH REAL (449 consultas): legacy acierta 194 (43 %), Foundation 101 (22 %). *(Corregido el
2026-10-01: la primera cifra, 238/53 %, daba por buena a legacy en ACTOR con sólo detectar un actor;
con evaluación simétrica legacy acierta 24/79 condiciones ACTOR, no 68 — sobre todo ofrece candidatos.)*
Foundation **no pierde por entender mal**: el productor da 0 errores en 620 llamadas (p90 2 s), y
de sus fallos 192 son `IR_CANNOT_EXPRESS` (ACTOR 79, universo omitido 50, mes/rango 31,
inclusivos 26, orden 23). Le siguen la proyección de compradores (51
`BUYER_SOURCE_PROJECTION_REQUIRED`) y el host (municipio 18, región 5, estado 17).
Legacy pierde donde Foundation ya gana: oportunidades (41/46 mal), universo omitido (43/50),
estados inventados (34), «licitaciones»→modalidad (12).

Estabilidad del productor: 3/40 propuestas canónicas cambian al repetir (7,5 %); la evaluación de
v5 debe correr ≥2 pasadas.

## Hallazgos que cambian el plan

1. **Fallos silenciosos en dogfood hoy** (`foundation_v3` activo): comprador equivocado por alias
   multi-dueño (ICBF → «ICBF REGIONAL TOLIMA»; 136/137 aliases) y TIME colapsado a año (mes,
   «desde», «antes de»; 7 REAL). → microfix **M1-H0** fail-closed (rama
   `fix/m1-h0-dogfood-fail-closed`), antes que nada.
2. **La puerta es la única barrera de varias capacidades.** Sin la regex, SEARCH secuestraría 25/58
   vecinos (Suppliers 7/8, Radar natural 6/8, Export 3/3). Borrar la puerta en M1-I exige que el
   intérprete **proponga la operación** y el host decline toda operación no-SEARCH hacia su dueño
   actual. Se adelanta a M1-C un trozo de M3: vocabulario de operaciones para *declinar*, no para
   ejecutar.
3. **El cambio de vocabulario obliga a un productor v5.** El prompt v4.2 está congelado y su Y1
   («cite only the year») es la causa del ensanche temporal. v5 se evalúa sobre el Gold sellado
   (≥2 pasadas) y sobre el held-out DA-6 (no regresión).
4. **ACTOR es el bloque de mayor tráfico** (73 consultas reales piden registros de un actor;
   79 condiciones ACTOR). `resolve_actor_params` y `contract_universe_intent.request` leen texto
   crudo: no sirven como autoridad de evidencia estructurada. El directorio resuelve cabezas
   genéricas («Consorcio», «Hospital», «Huila») a identidades concretas: hay que cerrarlo antes de
   hospedar ACTOR.
5. **MONEY no invierte con el productor real**, pero la guarda rechaza propuestas correctas
   («no superen» → `less_than` → CLARIFY falso) y es ciega con inclusivos (`:59`).

## Plan de las olas siguientes

Orden y paralelismo (un escritor por archivo; el núcleo Foundation tiene un único dueño):

| Ola | Misión | Escritor | Archivos (ownership) | Depende de |
|---|---|---|---|---|
| H0 | Fail-closed BUYER multi-dueño + cobertura TIME | ARES (sonnet) | `entity_resolver.py` (aditivo), módulo nuevo de cobertura TIME, 1 enganche en `foundation_admission.py` | — (en curso) |
| C0 | Diseño del spine v5 (IR + operaciones de declinación + vocabulario) | METIS (opus) | sólo doc | Gold sellado |
| C1 | Spine v5: contrato IR, decoder, prompt v5 nuevo (v4.2 intacto), host acepta/declina por operación | ARES (sonnet) único dueño de `foundation_*` + `interpretation_contract.py` | núcleo | C0, H0 merged |
| D-BUYER | binding de nombres físicos (30/35) + desambiguación nacional/regional | ARES | `entity_resolver.py`, binding | H0 |
| D-GEO | autoridad pura de municipio (DIVIPOLA) y región | ARES | módulos geo nuevos | — |
| D-TIME | `during_month`, `between_dates` (cotas abiertas), bug «septiembre del 2025 → 2026» | ARES | `temporal_semantics.py` + módulo puro | — |
| D-MONEY | guarda con negación + inclusivos, un léxico único de comparadores | ARES | `foundation_value_direction.py`, `money_polarity` | — |
| D-ACTOR | autoridad pura sobre evidencia + cierre de cabezas genéricas | METIS → ARES | `actor_*` | C0 |
| D-STATE/MOD | «adjudicados» ensancha; «terminados anticipadamente»; guarda de host para «licitaciones» y negación | ARES | `estado_families.py`, guarda nueva | — |
| F | Product View Policy (módulo puro nuevo) + eco y botones | ARES | módulo nuevo | C1 |
| G | Failover V1 según A5 (taxonomía PROVIDER_ERROR incl. vacío/truncado) | ARES | `foundation_producers.py` (tras C1) | C1 |
| E0–E10 | Integración dimensión por dimensión con su slice Gold | HERA + ARES | núcleo | C1 + D-* |
| H | Composición 2/3-vías (slice ADVERSARIAL ya sellado) | HERA | — | E |
| I | Cutover (gate abajo) | — | — | todo |

D-GEO, D-TIME, D-MONEY y D-STATE/MOD pueden correr en paralelo con C0/C1: tocan autoridades, no el
núcleo, y su contrato es «evidencia estructurada X → resolución Y».

## Gate de cutover M1-I (añadidos a la lista del plan)

- Regresiones exclusivas Foundation vs legacy contra Gold = 0 por clase SEARCH (columna
  `L-only-OK` de A3 a cero, sin dispensas; una diferencia deliberada corrige el Gold por
  adjudicación). Gate completo: ADR-103 enmienda 2.
- Secuestro de vecinos con la puerta **retirada** = 0 sobre NEIGHBOR (hoy 25/58 en simulación).
- Ensanches silenciosos = 0 (TIME, BUYER, VALUE): todo lo no hospedable aclara.
- v5 ≥ v4.2 en el held-out DA-6 y estabilidad ≥ 95 % en 2 pasadas.
- Suite global pareada, semgrep delta con `--base`, freeze y ZEUS fresco (opus, que no implementó).

## Política de verificación global (2026-09-30)

Los carriles cierran con ruff, focales, Hypothesis, mutantes, semgrep delta con `--base` y tests
legacy específicos; **no** con suite global. La global pareada (por nodeid, red bloqueada,
`PYTHONHASHSEED=0`, Hypothesis seed 19) se corre sólo sobre el candidato que de verdad se integra:

| Candidato | ¿Global? |
|---|---|
| H0 (va directo a `main`, cambia dogfood) | sí |
| Autoridades D-* aisladas | no |
| Composición `main+H0+C1.1` + D-TIME + D-MONEY + D-GEO (una rama de integración, commits separados para bisecar) | sí, una |
| Candidato C1/E (spine v5 + autoridades + PV + failover) | sí |
| CUTOVER (borra puerta y SEARCH legacy) | sí |
