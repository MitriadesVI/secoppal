# Municipal M1A RETRY — BLOCKED por captura de un comprador fuera del lote

**MUNICIPAL_M1A_RETRY: BLOCKED. Promoción retenida: 0. PUSH: NO.**

El mismo promotor pasó el dry-run y aplicó las 503 propuestas. La segunda
aplicación fue byte-idéntica. Los gates de los 1.888 aliases, 3.071 interacciones
y geografía pasaron contra producción real. Sin embargo, el test existente de
captura compuesta detectó un nuevo wrong owner: **San Andrés de Sotavento,
Córdoba, resuelve al ejecutivo de San Andrés, Santander**. Se activó STOP GLOBAL,
se interrumpió la suite y se revirtió todo el lote. No se cambió el parser ni
se aceptó el defecto mediante xfail o exclusión de una propuesta.

## Procedencia y secuencia de escritura

- Rama: `main`; tracked tree limpio al inicio. Archivos locales sin seguimiento conservados.
- `RETRY_HEAD_BEFORE`: `7bcad4bb55b2d09b7071ce4fca4f1b8bb1540573`.
- `BASELINE_HEAD`: `9fb6375a3d23158b44ca9ad14bf1624c2495d1fc`.
- `HEAD_BEFORE` del promotor permanece intacto: identifica los `before_*` históricos.
- Código/evidencia del retry: `d3a14f4`; el siguiente commit documental contiene este cierre.
- M1A original continúa **BLOCKED histórico**. M1A.1 continúa **PASS histórico**;
  su declaración de readiness no cubría esta interacción fuera del lote.

Se ejecutó `promote_municipal_m1a.py` sin cambios, primero sin argumentos y
después dos veces con `--apply`. Los JSON de producción fueron los únicos
archivos de producción modificados. Antes de la escritura se preservaron los
artefactos históricos que el contrato del promotor sobrescribe. Tras el STOP,
los datos y los artefactos M1A originales se restauraron desde `7bcad4b`.
Los resultados aplicados están congelados en `promoted_snapshot/`, separados
del manifiesto histórico restaurado.

| Métrica | Primera aplicación real | Segunda aplicación incremental | Retenida al cierre |
|---|---:|---:|---:|
| Propuestas de entrada | 503 | 503 | 503 diferidas |
| BuyerUnits nuevas | 498 | 0 | 0 |
| Unidades existentes ampliadas | 5 | 0 | 0 |
| Aliases añadidos | 1.883 | 0 | 0 |
| Bindings añadidos | 503 | 0 | 0 |
| Rechazos individuales por evidencia | 0 | 0 | 0 |
| Gazetteer total | 1.097 | 1.097 | 599 |
| Aliases totales | 12.685 | 12.685 | 10.802 |
| Bindings totales | 588 | 588 | 85 |

El promotor sigue informando 498/1.883/503 en la segunda ejecución porque sus
métricas comparan contra el baseline congelado. `second_apply.json` compara
las dos escrituras reales y demuestra **SECOND_APPLY_PRODUCTION_DELTA = 0**.
La inspección byte por byte precisa la explicación del delta: se incorporaron
1.888 strings propuestos y `dict.fromkeys` eliminó cinco ocurrencias duplicadas
preexistentes, una por ampliación; el incremento neto es 1.883. Ningún string
distinto anterior se perdió. No eran cinco propuestas ya almacenadas con los
mismos bytes: `alias_accounting.json` conserva las intersecciones vacías y el
conteo de cada ampliación. El total esperado era correcto; esta es su causa real.

## Bloqueo determinante

Consulta exacta del test ya existente:

```text
contratos del municipio de San Andrés de Sotavento
```

| Estado | Resultado |
|---|---|
| Baseline con runtime corregido | Sin BuyerUnit; captura completa y `clarification_needed=true`; el builder rechaza la consulta sin WHERE |
| Producción promovida | `ALCALDIA SAN ANDRES`, DIVIPOLA 68669, Santander; resolución exacta del fragmento `municipio de san andres` |
| Cola del nombre | `sotavento` se convierte en objeto contractual |
| Territorio solicitado | DIVIPOLA 23670, San Andrés de Sotavento, Córdoba; fuera de las 503 propuestas |

El SoQL realmente construido en producción incluye:

```sql
nombre_entidad = 'ALCALDIA SAN ANDRES'
AND (... LIKE ... '%sotavento%' ...)
```

La consulta completa, parámetros y procedencia de la propuesta incorrecta
están en [blocking_canary.json](../../artifacts/audits/municipal_m1a_retry/blocking_canary.json).
No es un problema de NIT ni de proyección: el proyector recupera exactamente
los nombres de **la identidad equivocada** que seleccionó el parser.

La escalera R2A.1 permite un fragmento si tiene evidencia exacta. La protección
M1A.1 evita partir nombres de compradores más largos ya acreditados, pero el
comprador de Sotavento no está acreditado en el catálogo. Incorporar San Andrés
convierte un fragmento antes irresuelto en una coincidencia exacta. La prueba
de captura compuesta lo detecta aunque los aliases propios del lote pasen.

Los 3.071 casos cubren 599 canonicals existentes, 584 aliases cercanos y 1.888
aliases propuestos. No prueban todos los nombres compuestos de compradores
ausentes. Su PASS se conserva con ese alcance; **no se extrapola a seguridad
global**. La reproducción posterior usa snapshots, sin volver a aplicar datos.

## Gates medidos antes del rollback

| Gate | Resultado real |
|---|---|
| Dry-run, 503 propuestas | PASS; 498 nuevas + 5 ampliaciones |
| Datos / NO_NIT_MERGE | PASS; unidades y bindings existentes preservados |
| Replay en producción, aliases M0 | 1.888/1.888 PASS; 0 wrong owners dentro del lote |
| Nuevas claves multi-owner | 0; permanecen las 141 históricas |
| Nuevas identidades canónicas duplicadas | 0 |
| Nuevos cruces de scan | 0 sobre 42.790 superficies congeladas |
| Cross-interaction | 3.071 casos; 0 nuevos wrong owners y 0 owners correctos perdidos en ese conjunto |
| Proyección ADR-073 | 503 bindings completos; 1.006 comprobaciones por dataset; igualdad exacta, sin fallback substring |
| Matriz municipal estratificada | 62 municipios, 444 consultas de comprador; PASS |
| Geografía DIVIPOLA | 2.206 consultas; 0 fallos; sin cambio por promoción |
| DIVIPOLA sin departamento | 953 exactos + 150 aclaraciones legítimas |
| DIVIPOLA con departamento | 1.103 exactos |
| Canario R2A.1 fuera del lote | **FAIL: 1 nuevo wrong owner observado; universo no barrido exhaustivamente** |

La matriz incluye Baranoa, Sabaneta, Puerto Gaitán, Magangué, Los Patios,
Tocancipá, Aguazul, Ipiales, Susacón, Susa, Nuevo Colón, Nemocón, Belén,
Valparaíso y los El Carmen acreditados, además de los estratos M0 por actividad,
acentos y homónimos. Susacón nunca pasa a Susa en estos casos; Susa mantiene
su identidad. Los homónimos usan los aliases cualificados acreditados; la
geografía desnuda ambigua exige departamento.

**Baranoa durante la aplicación:** las formas `contratos de la alcaldia de
baranoa` y `procesos del municipio de baranoa` resuelven a
`ALCALDIA MUNICIPAL DE BARANOA`. Ambos datasets proyectan exactamente ese
nombre. La paridad offline es 381 filas / **380 procesos únicos** y **374
contratos únicos**. `contratos en baranoa` devuelve territorio Baranoa,
Atlántico, sin BuyerUnit. **Tras el rollback:** se conserva la geografía M1A.1;
la cobertura de comprador acreditada vuelve a 0/380 y 0/374. No hubo consulta live.

## Cobertura: estado aplicado y estado final separados

Se reejecutó M0 offline dos veces: sobre producción expandida y después de
restaurarla. La segunda conciliación es idéntica al análisis M0 original.

| Métrica y denominador | Aplicada y luego revertida | Producción final |
|---|---:|---:|
| Territorios reconocidos / 607 acreditados activos | 571/607 = 94,0692% | 70/607 = 11,5321% |
| Territorios reconocidos / 1.103 DIVIPOLA | 571/1.103 = 51,7679% | 70/1.103 = 6,3463% |
| Procesos únicos / cohorte acreditada | 323.960/339.745 = 95,3539% | 197.352/339.745 = 58,0883% |
| Contratos únicos / cohorte acreditada | 318.904/331.185 = 96,2918% | 199.963/331.185 = 60,3780% |

La medición aplicada coincide con la expectativa M0. Sus 503 propuestas añaden
501 territorios: Manizales (17001) y El Cerrito (76248) ya tenían otra unidad
reconocida. Las unidades municipales con alcance acreditado pasarían de 119 a
622; las cinco ampliaciones también ganan alcance comprobable. No hubo
reclamaciones físicas compartidas ni territorios perdidos durante la aplicación.

Los porcentajes de volumen describen la cohorte acreditada. No son porcentajes
de municipios de Colombia. El denominador global completo de BuyerUnits
municipales continúa pendiente de adjudicación M2.

## Rendimiento real durante la promoción

**ACCEPTABLE**, medido antes de lanzar tests concurrentes, con las mismas
listas, calentamiento y repeticiones de M1A.1. Ambos catálogos usan el mismo
runtime corregido; el catálogo expandido se lee directamente de producción.

| Operación | Baseline p50/p95 (ms) | Producción aplicada p50/p95 (ms) |
|---|---:|---:|
| Carga | 78,323 / 80,602 | 96,499 / 97,878 |
| Exacto | 0,0061 / 0,1098 | 0,0054 / 0,1100 |
| Scan | 5,340 / 23,812 | 5,489 / 36,857 |
| Fuzzy | 11,614 / 13,092 | 13,392 / 15,235 |
| Pipeline de canarios | 11,081 / 91,749 | 0,594 / 0,719 |

Incrementos absolutos: carga p50 **18,176 ms**, scan p95 **13,045 ms**, fuzzy
p95 **2,143 ms**. Frente al candidato M1A.1, scan p95 es 36,857 vs 36,896 ms;
fuzzy p95, 15,235 vs 15,587 ms; pipeline p95, 0,719 vs 0,663 ms. No hay una
regresión material. No se optimizaron regexes. La mejora de pipeline corresponde
a estos canarios que evitan resoluciones fallidas, no a todo el tráfico.

## Validación, adjudicación y restauración

- Focales en producción promovida: **866 passed, 4 failed, 1 xfailed**, 45,36 s.
- Suite completa iniciada: **2.719 passed, 4 failed, 2 skipped, 11 xfailed**,
  interrumpida a los 165,05 s por STOP. **No es una suite completa aprobada.**
- Focales tras rollback (R2A.1, geografía y control nacional): **173 passed**.
- Corpus: **89/89** tanto aplicado como restaurado.
- Ruff y `make lint-core`: **PASS**. Registro DIVIPOLA reproducible.
- Validador aplicado: 0 errores, 141 claves multi-owner, 129 warnings. Los 62
  warnings adicionales corresponden a propuestas cualificadas con sólo dos aliases.
- Archivos de tests modificados: **0**. Nuevos xfails: **0**. Los 14 xfails de
  la suite M1A.1 son históricos; los 11 observados aquí corresponden al tramo ejecutado.
- `LLM_EXECUTION_MODE: DETERMINISTIC_OFFLINE_NO_LLM`.
- `LLM_PROVIDER_LIVE_COVERAGE: NOT_VERIFIED`.

Los fallos de ausencia para Baranoa/Nuevo Colón y Villa de Leyva contienen
expectativas anteriores a la promoción. El fallo de control nacional exige que
`scan_disabled` sea subconjunto sólo de aliases, aunque M1A ya permite el
canonical propio; Belmira revela ese desacople. Se documentan en
`adjudication.json`, sin modificar esos tests ni confundirlos con el wrong owner
de Sotavento, que por sí solo determina BLOCKED.

Hashes finales restaurados:

```text
aliases_db.json:
1bf21c5c4224d5097f6ad03dab67bea1341e2e665e9ebacb42e9a9fe0733eacc
buyer_source_names.json:
ec49aac1959c49565ae4374d1f18408c81e1bb179c1cf95498b7a2a75363b7d2
proposals.json:
b88ff18e75f837edaf2ceb3c3abd8fb4fc1b65319a8ea9c7aed5f0ffef678c3b
```

Producción, propuestas/capturas M0, DIVIPOLA, BuyerGroups, `result_identity`,
`result_grouping`, parser y tests permanecen sin diferencias respecto al inicio.
Los únicos cambios retenidos son el verificador, evidencia y documentación.

## Reproducción del bloqueo sin escribir producción

```bash
.venv/bin/python scripts/audits/verify_municipal_m1a_retry.py reproduce-blocker
.venv/bin/python scripts/audits/verify_municipal_m1a_retry.py rollback-check
```

El primer comando compara los snapshots preservados y verifica el mismo cruce;
no vuelve a ejecutar `--apply`. El segundo comprueba hashes originales y la
conciliación M0 restaurada. Los comandos de dry-run, aplicación, benchmark,
replay de producción y M0 ya ejecutados quedan identificados por fase en
`validation.json`, con sus logs íntegros bajo `validation/`.

## Deuda y commits

**Verificado:** aplicación real del promotor, idempotencia entre escrituras,
proyección y cobertura post reproducidas, rendimiento aceptable y rollback
íntegro. **Abierto:** resolución de nombres compuestos de compradores ajenos al
lote ante nuevos aliases cortos; ampliación del gate de interacción; promoción
de las 503 propuestas; adjudicación residual M0/M2 y proveedor LLM vivo.

Commit de evidencia: `d3a14f4 test(audit): capture municipal retry cross-owner regression`.
El cierre usa `docs(audit): close municipal M1A retry as blocked`. No se creó
`feat(gazetteer): promote...` porque no queda ninguna promoción segura aplicada.
El `HEAD_AFTER` final es el commit documental que contiene este archivo,
resoluble con `git log -1 --format=%H -- docs/gazetteer/MUNICIPAL_M1A_RETRY.md`.
**PUSH: NO.**
