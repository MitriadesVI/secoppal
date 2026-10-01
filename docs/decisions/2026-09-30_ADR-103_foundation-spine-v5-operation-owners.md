# ADR-103 — Foundation spine v5: perfiles cerrados, enrutamiento por operación y contrato de autoridades

**Fecha:** 2026-09-30.
**Estado:** aceptada por el orquestador (HERA) con delegación del usuario; sin efecto en runtime
hasta que cada paso C1 se fusione con autorización expresa. Enmienda ADR-102 §2/§4 y resuelve la
tensión ADR-102 §9 vs D1.
**Misión:** `M1_SEARCH_FOUNDATION_V1`, ola C. Diseño completo:
`docs/audits/m1_search_foundation/C0_SPINE_V5_DESIGN.md` (METIS C0).
**Numeración:** máximo global en ramas y worktrees = ADR-102; 103 verificado libre.

## Contexto

La Fase A de M1 (Gold sellado de 999 consultas) mostró que Foundation pierde frente a legacy por
lo que su IR no puede expresar (ACTOR, universo omitido, mes/rango, inclusivos, orden), no por
entender mal; que el prompt v4.2 contiene convenciones normativas dañinas (Y1 «cita sólo el año»);
y que la puerta léxica es la única barrera que impide a SEARCH secuestrar capacidades vecinas
(25/58 sin ella).

## Decisión

1. **Perfiles cerrados.** `SECOPPAL_FOUNDATION_MODE ∈ {legacy, foundation_v3, foundation_v5}`.
   Un perfil es un paquete fijo productor+wire+H7+host; no hay selector suelto de productor. El
   prompt v4.2 queda byte-congelado y seleccionable como rollback.
2. **IR v2 única en memoria** (`SCHEMA_VERSION = 2`), con `lift_v1` sin pérdida para el wire v4.2.
   El perfil fija el wire; ningún decoder infiere la versión.
3. **La operación gobierna el enrutamiento**, con frontera acotada: el LLM sólo decide a qué dueño
   de una tabla cerrada se *ofrece* el turno; el dueño debe reclamarlo con su propia admisión; sin
   segundo salto; a legacy sólo cruza la operación (nunca valores); la conformidad del ejecutor se
   mide y es 0 para el cutover.
4. **Autoridades por dimensión** como funciones puras sobre una ventana de evidencia (cita + ≤4
   tokens), contexto monótono (sólo retira autoridad), valor tipado con ida y vuelta, razones
   cerradas, registro con madurez `SHADOW → ACTIVE` y borrado de la rama en línea al activar.
5. **Lo normativo se cierra en el host** antes de escribir el prompt v5; Y1 se elimina.
6. **Product View Policy** como módulo puro tras la admisión; toda vista por defecto se anuncia.
7. **Caída del proveedor en `foundation_v5`: rige D1** («no pude interpretar ahora», sin legacy);
   ADR-102 §9 sigue rigiendo sólo `foundation_v3`.

## Decisiones de producto (P1–P9 del diseño)

Tomadas según la recomendación de METIS: P1 mensaje explícito con guía para operaciones sin dueño;
P2 MULTI con SEARCH → «por pasos» con un botón por job; P3 (enmendada, ver abajo) superlativo singular → ORDER + CARDINALITY 1; P4 activación por etapas con autorización expresa en cada una; P5 sí a perfiles
cerrados; P6 D1; P7 en M1 sólo botones de vista (período y orden en M2), opciones numeradas en
texto; P8 tope de top-N = 100 anunciado; P9 SEARCH por persona natural con la política de Actor
Profile (ADR-060).

## Consecuencias

- Cambian `interpretation_hash` y `plan_hash` de todo turno Foundation; `load_acceptance`/`lift_v1`
  permanecen hasta M4.
- El test que fija «un solo perfil» se reemplaza por uno de conjunto cerrado (cambio declarado).
- Las reclamaciones de dueños legacy siguen leyendo texto: deuda declarada que muere en M3.
- Activar v5 exige los criterios ACTIVATE_V5 (diseño §5.4.3) sobre la partición TEST del Gold
  (≥2 pasadas) y no regresión en el held-out DA-6 con custodio.

## Rollback

`foundation_v3` (v4.2 bajo el host v2) o `legacy` (paridad con `main`). Cada paso C1.x es un commit
revertible; C1.1 no cambia el runtime.

## Enmienda 1 (2026-09-30) — P3 vetada y corregida

La adopción inicial de P3 («superlativo singular → lista ordenada, sin límite») contradecía el Gold
sellado: N012 («el contrato más grande de la Alcaldía de Cali», doble etiqueta sin desacuerdo) y
R0507 (consulta REAL, «dame el contrato … de mayor cuantía») llevan `ORDER value_desc` con
cardinalidad 1. Veto del asesor aceptado; el Gold no se modifica.

Regla vigente:
- Superlativo **singular** («el/la <sustantivo> más …», «el de mayor valor») → `ORDER` +
  `CARDINALITY 1`.
- Superlativo **plural sin cifra** («los más caros») → lista ordenada, sin `CARDINALITY`.
- **Cifra explícita** («los 10 más caros») → `CARDINALITY N` (tope P8 = 100, anunciado).
La cardinalidad la decide el host a partir de la cita (número gramatical o cifra); el LLM no
entrega el valor (invariante 3).
