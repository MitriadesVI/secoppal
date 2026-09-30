# ADR-102 — Foundation V3: promoción selectiva detrás de un solo flag

**Fecha:** 2026-09-29.
**Estado:** candidato de promoción congelado para revisión adversarial independiente.
Sin merge, push, activación ni despliegue. `SECOPPAL_FOUNDATION_MODE=legacy` por defecto.
**Misión:** `FOUNDATION_V3_SELECTIVE_PROMOTION_V1`.
**Rama:** `feature/foundation-v3-promotion-v1` desde `main` `68090765` (no avanzó).
**Numeración:** máximo global en ramas y worktrees = ADR-101 (sólo en ramas Foundation); 102 libre.
**Supera:** el «Estado» de ADR-101 (el flag `SECOPPAL_FOUNDATION_UNIVERSE_POLICY` y el complemento
histórico no llegan al producto); el resto de ADR-101 sigue vigente.

## Contexto

La auditoría final (`audit/foundation-v3-heldout-da6-zeus` = `f703504b`) dejó
`FOUNDATION_PROMOTION = GO_PROMOTION_ONLY`: held-out DA-6 sellado (digest `bc0381e4…`,
acta `908eb552…`), prompt B v4.2 GO, H7 sin interferencia, valor incremental de H7 no
demostrado, complemento histórico y B_GUARDAS no ejercitados, extremo a extremo no
verificable. Esa rama es un laboratorio de 102 commits y 312 k líneas; no se mergea.

## Decisión

1. **Trasplante selectivo por archivo**, desde `main`, con un inventario commiteado antes de
   copiar (`docs/audits/foundation_v3_promotion/SELECTIVE_INVENTORY.md`).
2. **Productor único**: brazo B, prompt v4.2 byte-congelado (SHA-256 `a5867d40…`, fijado en
   test). v3, v4, v4.1 y el brazo C dejan de ser alcanzables; no hay selector de productor.
3. **`universe_kind` obligatorio** (sin default); ausencia = `INVALID_PROPOSAL`.
4. **H7 inseparable**: un único perfil `foundation_v3` = B v4.2 + `universe_kind` + H7. No
   existe `foundation_v3` con H7 apagado; el rollback soportado es `legacy`.
5. **Propiedad de SEARCH**: Foundation entra en `SecopalWorkflow._dispatch()` después de los
   handlers estructurados y de la costura Natural Alpha, y sólo reclama un primer turno que
   todos los dueños vigentes declinan según sus propios detectores (mismo
   `QueryRouter.parse` que `admit()`). No es un router de producto; convive con Natural Alpha,
   Capability Binding (SUPPLIERS, Contract Activity, follows), Entity/Actor Profile, Radar,
   direct lookup y las continuaciones (`ROUTE_OWNERSHIP_MATRIX.md`). La compuerta
   `eligible_first_search` no se amplía.
6. **Excluido**: `HISTORICAL_TENDER_COMPLEMENT` (no copiado, no importable; ni siquiera
   representable en el resultado de H7). B_GUARDAS no son autoridad: G1/G2 pasan a invariantes
   de admisión, G3 la absorbe H7, G4 queda como telemetría futura.
7. **Rutas silenciosas cerradas** (cada una con commit, propiedad, mutante y cero llamadas):
   varias citas de un TOPIC/BUYER/GEOGRAPHY (G1/T4), la ocurrencia del universo como tema
   (G2, por ocurrencia), composición de estados (sólo el embudo ADR-100/S3 bajo «o» desnuda),
   y dirección/limite del operador monetario frente a su propia evidencia.
8. **Mensajes humanos**: una frontera única código interno → mensaje determinista; el código
   queda en `foundation_internal_reason` y en la traza.
9. **Degradación por caída del proveedor**: búsqueda básica rotulada, sólo por
   `PROVIDER_ERROR`, sólo con el parser determinista vigente y una lista blanca cerrada
   (`BASIC_FALLBACK_CONTRACT.md`); si no es segura, cero llamadas y reintento.
10. **Flag OFF** = comportamiento de `main` (paridad observable medida; ver `FINAL_REPORT.md`).
11. **DA-6**: el paquete sellado queda como evidencia externa por digest (opción B), no se
    copia a CI porque sólo es ejercitable con propuestas capturadas del proveedor; se crea un
    corpus de regresión separado que no se llama held-out.

## Hallazgos del trasplante

- `f703504b` relajaba para todos los llamadores la guarda anti-consulta-global con O1B; ahora
  es un opt-in explícito que sólo pasa la proyección Foundation.
- `f703504b` configuraba por defecto el brazo C, no el medido.
- Tras un turno Foundation, un fragmento legado habría perdido el tema en silencio; ahora se
  aclara. Las frases de comprador que el parser legado lee como actor quedan con ese dueño
  (pérdida de cobertura a medir en `FOUNDATION_ENTRY_ADMISSION_V1`).

## Activación (fuera de esta decisión)

1. Trasplante selectivo + suite global (esta rama). 2. Dogfood Rodrigo/allowlist.
3. Canary pequeño. 4. Default general. Cada etapa requiere autorización humana expresa.

## Límites heredados de DA-6

GOLD_ONLY; sin aprobación de producción; sin certificación de filas vivas; IGUAL_A_D es
equivalencia de obligaciones; O6 extremo a extremo no verificado; paráfrasis cercanas y
vocabulario próximo al prompt declarados; tres pasadas a temperatura 0 no son muestras
independientes; 5 READY esperados no pasan `eligible_first_search`.

## Rollback

`SECOPPAL_FOUNDATION_MODE=legacy` (default). Límite: un turno Foundation ya guardado deja un
contexto vacío para un follow-up legado posterior.
