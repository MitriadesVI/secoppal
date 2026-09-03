# INSTITUTIONAL BUYER INTEGRITY & GROUPS V1

Objetivo y orden de trabajo: checkpoint C → veracidad/fail-closed → intención
institucional → capacidad BuyerGroup → Bogotá si GO → Municipal M0.
Base recuperada: `main @ adf9703`, 2026-09-03. Sin reset ni push.

## C — INSTITUTIONAL_FAMILY_COMPATIBILITY

Estado: **PASS**. Cambios pendientes originales recuperados en
`entity_resolver.py` y `test_institutional_family_compatibility.py`.

La compatibilidad de familia se aplica a alias exactos, escaneo y candidatos
fuzzy de las reescrituras estrictas. Alcaldía Local y Fondo de Desarrollo Local
son compatibles; CENAC, Personería y otras cabezas institucionales no lo son.
El territorio que contiene un nombre no acredita su familia.

La recuperación encontró otro contraejemplo: `PERSONERIA ALCALDIA LOCAL DE
CHAPINERO` pasaba la comprobación original por contener “alcaldía local”. Se
exige ahora cabeza afirmativa, después de un posible prefijo departamental
reconocido por `DEPARTAMENTOS`. No se mantiene una lista de compradores vetados.
Se reutilizan `name_tokens`, las autoridades de alias y la autoridad especial
de evidencia raíz departamental; no se crea un clasificador paralelo.

Los alias exactos conservan su evidencia territorial (Pereira, Risaralda;
Mosquera, Cundinamarca), aunque el nombre físico no contenga toda esa cola.
La inferencia fuzzy exige además preservar el territorio escrito.

Validación focal: 99 passed, 2 xfailed (deuda geográfica de ecosistemas).
Relacionadas: 131 passed. Query corpus: 89/89. Ruff y `make lint-core`: PASS.
Suite completa: **10797 passed, 2 skipped, 16 xfailed**, 568,24 segundos.
La corrida inicial se interrumpió al reforzar la comprobación de cabeza;
estos números corresponden a una corrida nueva completa de la versión final.
Evidencia: `artifacts/audits/institutional_family_compatibility_c.json`.

Deuda cerrada por C: `MUNICIPAL_REWRITE_HEAD_COMPATIBILITY` y compatibilidad
Alcaldía Local ↔ FDL sin hijack militar. No modifica NIT, bindings, membresías
ni `result_identity`.

## Deudas y próximos checkpoints

| Deuda / etapa | Estado al recuperar C | Criterio de cierre |
|---|---|---|
| Puente Aranda; cinco unidades con grafías por dataset | Implementados antes de C | Mantener canarios de `test_buyer_unit_integrity_p02.py` |
| Conjuntos `entidad_in_candidates` >15 | Fail-closed previo, capacidad pendiente | Ningún colapso y estrategia medida |
| `ECOSYSTEM_SOQL_DISPLAY_DIVERGENCE` | Abierta | SoQL mostrado igual al ejecutado; sin subset silencioso |
| Genéricos con puntuación | Abierta, reproducida | `ALCALDIA*`, `GOBERNACION+`, `E.S.E.` sin fuzzy |
| Intención SDIS | Abierta | Comprador conservado; `integracion social` fuera del objeto |
| Estrategia de grupos | Pendiente | Latencia, timeout, orden, paginación, counts y completitud |
| Piloto Bogotá | Condicionado a GO | Membresía acreditada, declarada y versionada |
| Municipal M0 | Pendiente | DIVIPOLA + directorio + actividad, denominadores explícitos |
| NIT fantasma / canonical sintético | Censo pendiente | Informar defectos sin merge ni ampliación de filtros |

Cada etapa cierra con PASS/PARTIAL/BLOCKED, commit propio, focales, corpus,
ruff, `make lint-core`, y suite completa si cambia runtime o datos. Cualquier
categoría nueva que ADR-074 no pueda representar detiene esa etapa para reporte.

## D — FAIL_CLOSED_AND_EXECUTION_TRUTH

Estado: **PASS**. La autoridad existente `generic_buyer` reconoce el ruido
de puntuación mediante `name_tokens`; `ALCALDIA*`, `GOBERNACION+`, `MUNICIPIO..`
y las formas societarias ESE fallan cerrado. La captura de ESE exige fin de
frase o una cláusula nueva para conservar el demostrativo de “ese año”.

Se elimina la inyección geográfica posterior al fetch en `normal_flow`: el
contexto del advisor, la huella y la traza deben describir los filtros que
efectivamente produjeron las filas. El test instrumenta la llamada real y el
contexto entregado al advisor; reprodujo el defecto antes del cambio.
La rama legacy `entidad_ecosystem` también rechaza >15, con la misma excepción
de capacidad existente, en vez de truncar silenciosamente.

Focales: 63 passed. Corpus: 89/89. Ruff y `make lint-core`: PASS.
Suite completa: **10814 passed, 2 skipped, 14 xfailed**, 567,85 segundos.
La primera corrida señaló dos hashes de archivos autorizados y una expectativa
del ecosistema posterior al fetch; se actualizaron esos tres oráculos y se
repitió toda la suite. Focales de esos oráculos: 28 passed.
Evidencia: `artifacts/audits/buyer_execution_truth_d.json`.
Deudas cerradas: genéricos puntuados,
`ECOSYSTEM_SOQL_DISPLAY_DIVERGENCE`, truncamiento geográfico silencioso.

## E — INSTITUTIONAL_INTENT_SDIS

Estado: **PASS**. Se acredita el alias territorial explícito «integración
social de Bogotá» sobre la BuyerUnit SDIS existente. Los tres prefijos del
canario fallaron antes del cambio; ahora preservan comprador, adulto mayor y
2026. «Contratos en Bogotá» sigue siendo únicamente geografía. No cambia fuzzy.

Focales: 64 passed. Corpus: 89/89. Ruff y `make lint-core`: PASS.
Suite completa: **10818 passed, 2 skipped, 14 xfailed**, 656,23 segundos.
Deuda cerrada: `SDIS_INSTITUTIONAL_INTENT_LOSS`.

## F — BUYER_GROUP_EXECUTION_STRATEGY

Estado: **PASS / GO acotado**. Una consulta OR de nombres físicos exactos,
proyectados por dataset, conserva un orden global con fecha, valor, identificador
lógico y `:id` físico como último desempate. El grupo selecciona unidades;
`result_identity` continúa siendo la única autoridad de identidad de resultados.

Se midieron ICBF 33, central 19 nombres, locales 20 y ejecutivo 39 nombres,
en ambos datasets: ocho casos PASS. Cada caso contiene 12 llamadas adicionales
(conteo y página OFFSET 500; histórico completo y 2026; tres repeticiones).
Todas terminaron dentro del presupuesto de 15 segundos. No se afirma caché fría.
Se usaron las columnas y el orden de producción, además del desempate físico.

La completitud se materializó para `[2026-08-01, 2026-08-08)`, comparando conteos
antes/después, suma de lotes exactos de diez nombres, páginas de 500, replay de
la frontera de 1.000 filas, identidad física e identidad lógica. No se confunde
esa comprobación semanal con una materialización de todo el histórico.

| Caso | Procesos: filas / únicos | Contratos: filas / únicos |
|---|---:|---:|
| ICBF 33 | 652 / 650 | 739 / 739 |
| Central 19 nombres | 910 / 903 | 948 / 948 |
| Locales 20 | 91 / 90 | 98 / 98 |
| Ejecutivo 39 nombres | 1.001 / 993 | 1.046 / 1.046 |

La política de capacidad aprobada es hasta **40 nombres físicos acreditados**;
superarla requiere otra medición. Los candidatos legacy mantienen su límite 15.
Los timeouts no autorizan quitar miembros, filtros ni el orden global del grupo.

**Corrección de cardinalidad:** Salud publica una grafía por dataset, acreditada
como una sola unidad. La [estructura oficial distrital](https://serviciocivil.gov.co/tablero-de-control/info-importante-tablero-de-control/faqs-tablero-de-control)
declara 18 entidades centrales y 20 fondos locales. El registro del piloto debe
representar 18/20/38 BuyerUnits; 19/39 siguen identificando los nombres físicos
del inventario histórico. ADR-074 ya representa esta distinción; no aparece una
categoría arquitectónica nueva.

Evidencia: `artifacts/audits/buyer_group_execution_v1/`; reproducción:
`scripts/audits/audit_buyer_group_execution_v1.py`. Focales: 9 passed.
Corpus: 89/89. Ruff y `make lint-core`: PASS. Sin mutación de runtime/datos de
producción en F; la suite completa de E permanece como base. Deuda de capacidad
medida cerrada; activación y membresía pertenecen a G.
