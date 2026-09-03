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
