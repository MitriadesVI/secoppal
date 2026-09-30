# ADR-101 — Foundation V3: el host decide el universo de la cita (H7)

**Fecha:** 2026-09-29.
**Estado:** candidato experimental; flag `SECOPPAL_FOUNDATION_UNIVERSE_POLICY` en `off` por defecto; sin merge a `main`, push fuera de la rama asignada, activación ni despliegue. Foundation sigue `NO_GO`.
**Misión:** `FOUNDATION_HOST_UNIVERSE_POLICY_V1`.
**Rama:** `claude/foundation-bc-v4-eval` (repo `secoppal-fundation`), sobre `69cf3a3d`.
**Numeración:** el máximo global en los worktrees y ramas era ADR-100; 101 verificado libre.

## Contexto

En Foundation V3 un productor LLM propone el significado lingüístico y el host lo
admite, lo proyecta y lo ejecuta. La convención P4 decía que el host no lee el texto
de la evidencia de universo, así que O3 (oportunidades e intención de participar),
O6 («licitación» coloquial) y O7 («convocatorias») quedaron en el prompt.

El paso B intentó enseñárselas al productor en tres versiones (v4, v4.1, v4.2) y
ninguna pasó el filtro. Una sonda sobre las capturas mostró que el productor cita el
núcleo del universo de forma estable en todas las pasadas y que lo que oscila es el
bit de oportunidad: cada versión del prompt movía el error de un caso a otro. El
trabajo lingüístico está bien hecho; el normativo, no, y esas reglas son
convenciones de SECOPPAL sobre un vocabulario cerrado.

## Decisión

1. **Cambia P4.** El productor dice qué fragmento nombra el universo y declara
   `universe_kind`. El host decide qué significa esa cita con un léxico cerrado (O3,
   O3-INTENT, O6A/B/C, O7) y deja una `RequestUniverseDecision` por petición, con la
   afirmación del productor, la decisión efectiva, la autoridad (`PRODUCER` o
   `HOST_CLOSED_POLICY`), las reglas y el efecto. El host lee solo datos ya
   estructurados (sujeto, cita de universo, condiciones y peticiones del mismo
   alcance), nunca el turno crudo.
2. **Precedencia.** Regla cerrada conocida > afirmación del productor. Dos reglas
   cerradas que discrepan piden aclaración. `universe_kind` es obligatorio: su
   ausencia es `INVALID_PROPOSAL`, no una búsqueda normal.
3. **La expansión es otra política.** `HISTORICAL_TENDER_COMPLEMENT` añade la vista
   de contratos a una búsqueda histórica de «licitaciones» con año y comprador. Es un
   `WIDENING` con recibo propio, y la respuesta dice que esos contratos no se
   presentan como derivados de los procesos listados.
4. **La historia experimental se conserva.** El paso B de solo prompt queda
   `CLOSED / NO_GO`. v4.2 + H7 es un candidato distinto, evaluado sobre las mismas
   capturas.

## Garantía y límites

- Una palabra cerrada solo decide cuando es la cita de universo. Si otra condición
  la cita, es contenido de esa condición.
- Ninguna palabra crea oportunidades en contratos.
- Un estado que el host no resuelve no altera O6.
- La intención comercial («quiero vender») queda con el productor, porque la familia
  canónica exportada solo cubre los verbos de participación.
- La expansión no cubre VALUE, STATE, modalidad ni peticiones dobles.

## Evidencia

Ver `docs/audits/foundation_v3_bc_v4/H7_POLITICA_UNIVERSO.md`: gate offline fijado
antes del replay, mutaciones (11/11 muertas) y resultado. En desarrollo es una cota,
porque el acta decidió O6 con A1-N7 y A1-N8 como ejemplos; la evidencia será el
held-out, con custodio independiente.

## Consecuencias

- `CONVENCIONES.md`: P4 y O6 remiten a este ADR.
- Llevar H7 al producto depende del paso D y del sello DA-6 del held-out.
