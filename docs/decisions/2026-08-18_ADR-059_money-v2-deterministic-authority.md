# ADR-059 — Autoridad monetaria determinística y vigencia SMMLV

**Estado:** Accepted
**Fecha:** 2026-08-18
**Rama:** `feature/money-v2`

```text
AS_BUILT_AUTHORITY:
  docs/audits/2026-08-18_MONEY_V2_CLOSEOUT.md
```

## Contexto

SECOPPAL necesitaba interpretar montos colombianos sin confundirlos con NIT,
años o referencias, y proyectarlos al campo económico correcto de procesos o
contratos. SMMLV agrega una restricción adicional: el COP equivalente depende
de una vigencia y no puede sobrevivir sin revisión cuando un follow-up cambia
el alcance temporal.

La auditoría posterior al arco inicial encontró tres fronteras que debían quedar
explícitas en la misma autoridad: un umbral SMMLV heredado frente a un año nuevo,
el cardinal verbal `mil` ya escalado por la gramática y un monto como `2000
millones` que tiene forma de año.

## Decisión

1. `app/core/money_resolution.py` resuelve la representación numérica con
   `Decimal`; `app/utils/money.py` aplica unidades y escalas COP.
2. En cardinales verbales, `mil` forma parte del número resuelto (`dos mil` =
   2.000). Solo un literal numérico seguido de la unidad `mil` aplica un factor
   adicional (`500 mil` = 500 × 1.000).
3. Un entero `20xx` seguido de unidad monetaria no es señal temporal para el
   clasificador de follow-up. `2000 millones` refina valor; `solo 2025` cambia
   la vigencia.
4. `money_metadata` conserva la cantidad SMMLV fuente. Si un follow-up cambia
   fechas, el merge vuelve a validar la metadata heredada antes de ejecutar:

   - una sola vigencia catalogada recalcula `valor_min` / `valor_max` desde la
     cantidad fuente;
   - un alcance multianual elimina el umbral COP heredado y produce una
     aclaración bloqueante;
   - una vigencia no catalogada también bloquea, sin extrapolar.

5. `QueryPlan` P16 es la última compuerta: una aclaración SMMLV impide construir
   o enviar una consulta SECOP.
6. El contrato público sigue usando enteros COP en `valor_min` / `valor_max`.
   La polaridad no cambia y `DatasetSpec` conserva la proyección a
   `precio_base` o `valor_del_contrato`.
7. La metadata continúa como diccionario mínimo y opcional. No se introduce un
   motor monetario paralelo, LLM, dependencia lingüística externa ni semántica
   SMMLV multianual aproximada.

## Consecuencias

- El umbral SMMLV siempre corresponde al año efectivo del frame final.
- Los rangos de varias vigencias fallan cerrados y hacen cero llamadas SECOP.
- Las cantidades cardinales no monetarias inferiores al umbral del router ya no
  se inflan 1.000 veces hasta convertirse en filtros económicos falsos.
- El invariante `1 billón = 1000 × mil millones` permanece intacto.
- El catálogo anual sigue requiriendo una actualización explícita y respaldada
  por fuente oficial para nuevas vigencias.

## Alcance diferido

Este ADR no autoriza limpieza general del parser. Permanecen diferidos
`MV2-06`, `MV2-07`, `MV2-08`, `MV2-09`, `MV2-11`, `MV2-12` y `MV2-13`, según el
re-audit. El índice canónico `docs/decisions_log.md` no se modificó porque ya
tenía cambios locales ajenos al patch; este archivo autónomo es el registro
versionado de la decisión.
