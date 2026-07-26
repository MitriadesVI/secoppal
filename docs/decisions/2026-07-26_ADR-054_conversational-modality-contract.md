# ADR-054 — Contrato conversacional de modalidad con autoridad determinística

**Estado:** Accepted
**Fecha:** 2026-07-26
**Rama de integración:** `integration/modality-contract-v1-main`
**Bloque:** M3 (M2 catálogo/resolver · M3A/M3B integración · M3C-B · R3 · M3C-C · ER1A · M3C-E)

```text
AS_BUILT_AUTHORITY:
  docs/planes/2026-07-25_M3C_modality_conversation_closeout.md
```

Este ADR resume la decisión aceptada. El closeout conserva el detalle as-built,
las matrices completas de cobertura, la matriz adversarial y las tablas de
paridad. Ante cualquier contradicción sobre *lo construido*, manda el closeout;
sobre *lo decidido*, manda este ADR.

> **Nota de trazabilidad y numeración.** El trabajo se desarrolló en
> `search/modality-contract-v1`, una rama cuya ascendencia incluía trabajo ajeno
> a modalidad. Sobre `main` se integró únicamente el conjunto de modalidad; el
> contenido de la decisión es el mismo en ambas líneas.
>
> La numeración ADR es global entre ramas concurrentes. El salto desde el último
> ADR registrado en `main` es intencional para preservar la identidad `ADR-054`
> ya asignada a esta decisión y evitar colisiones con decisiones reservadas en
> otras líneas de desarrollo.

---

## Contexto

El resolutor de modalidad (M2) resuelve el turno inicial: un texto como
`licitaciones públicas de alimentación` produce un filtro canónico de
`procedure_ids` con polaridad (include / exclude) y un status jurídico. Lo que
no existía era una semántica para el **segundo turno**. Cuatro problemas
concretos lo bloqueaban:

1. **Los follow-ups no tenían álgebra explícita.** `además contratación
   directa`, `quita contratación directa` y `permite contratación directa` son
   tres operaciones distintas sobre el mismo filtro previo, y ninguna estaba
   nombrada. Sin un enum cerrado, cada frase nueva habría empujado a un
   if-else que nadie podía auditar ni probar por casos.

2. **Las señales de control se confundían con lenguaje descriptivo y
   jurídico.** `contratación directa que incluye suministro` usa `incluye` como
   verbo descriptivo del objeto, no como orden de agregar una modalidad.
   `contratación directa sin ofertas` lleva un `sin` que pertenece al nombre
   jurídico de la modalidad, no a una exclusión. Un clasificador que leyera
   tokens sueltos habría mutado el filtro en ambos casos.

3. **El LLM tenía demasiada autoridad sobre parámetros estructurales.** La
   ruta de fallback podía reescribir dataset, estados, montos y fechas. Una
   dimensión que el pipeline determinístico ya había resuelto podía ser
   sobrescrita por una propuesta del modelo, y cada campo nuevo del sistema
   nacía *abierto* al LLM hasta que alguien recordara protegerlo.

4. **Una mutación parcial podía contaminar el estado.** Si un turno pedía
   operar sobre dos modalidades y solo una era ejecutable, aplicar la mitad
   habría dejado dataset, estados, topic y persistencia describiendo una
   búsqueda que el usuario nunca pidió — y peor, habría ejecutado SoQL contra
   SECOP con ese estado a medias.

---

## Decisión

Se congela el siguiente contrato:

1. **M2 resuelve modalidad exactamente una vez por turno.** No hay segunda
   llamada después del merge conversacional, ni reejecución tras aplicar la
   operación.

2. **`ModalityInterpretation.consumed_spans` vincula el control con targets
   reales.** Un candidato de control que caiga dentro de un span consumido por
   M2 —o que se solape con él— nunca es control: pertenece al nombre jurídico.

3. **Las operaciones permitidas forman un enum cerrado:**

   ```text
   REPLACE
   ADD_INCLUDE
   ADD_EXCLUDE
   REMOVE_MEMBER
   REMOVE_EXCLUDE
   RESTORE_INCLUDE
   CLEAR
   ```

   No hay operación implícita fuera de este conjunto, y una frase que no
   corresponde a ninguna no muta nada.

4. **El motor puro aplica la mutación sobre IDs canónicos.** El álgebra opera
   sobre `procedure_ids` resueltos, nunca sobre texto, aliases ni raws del
   dataset.

5. **`RESTORE_INCLUDE` en universo abierto elimina la exclusión, pero no crea
   whitelist.** Solo agrega al include si ya existía una whitelist previa.
   `alimentación excepto contratación directa` → `vuelve a incluir contratación
   directa` devuelve el universo abierto, no una búsqueda restringida a
   contratación directa.

6. **Status M2 bloqueado, target ausente o conflicto de control produce
   rollback completo, cero SoQL y cero ejecución.** No existe la aplicación
   parcial: o la operación entera es ejecutable, o el filtro previo queda
   intacto y el turno se responde pidiendo aclaración.

7. **Solo el builder proyecta procedure IDs a raws del dataset final.** Las
   capas anteriores conservan los IDs canónicos, lo que permite pivotar de
   procesos a contratos sin reparsear la modalidad.

8. **El LLM usa allowlist monotónica y solo completa dimensiones permitidas aún
   vacías.** Parte de los params determinísticos y únicamente puede llenar un
   hueco autorizado (una sola representación de topic, `entidad`, `ciudad` o
   `departamento`, siempre en forma canónica). Un campo ya resuelto no se
   sobrescribe.

9. **Dataset, modalidad, estados, monto, fecha, orden, intent y campos futuros
   son cerrados para el LLM.** La política es allowlist, no lista negra: un
   campo nuevo nace cerrado y solo se abre por decisión explícita.

10. **La metadata de control es efímera.** Instrucción, spans, operación y trace
    no entran a `parsed_params`, `QueryFrame`, fingerprint, persistencia ni
    SoQL. Solo el filtro canónico final cruza esa frontera.

11. **Las construcciones cerradas pueden producir una vista enmascarada para
    M2** sin alterar el texto original ni sus offsets. Se enmascaran únicamente
    spans de control exactos; el topic sustantivo y las posiciones se conservan.

12. **La telemetría es privada, agregada, opt-in y fail-open.** No contiene
    `user_query`, `chat_id`, procedure IDs, params, SoQL, resultados ni URL;
    está desactivada por defecto; corre después de la persistencia; y un error
    de escritura no altera la respuesta al usuario.

13. **El sidecar de aclaración (M3C-D) queda `DEFERRED`** hasta cumplir la
    compuerta de producto real descrita abajo. No se implementó
    `PendingModalityClarification`, sidecar, TTL, consumo single-use ni override
    contextual de `QueryRouter`.

### Compuerta M3C-D

Condición necesaria:

```text
sample_is_real = true
turns_total >= 100
```

Y además al menos una de:

```text
blocked_total >= 10
```

o:

```text
blocked_by_m2_status_total >= 5
blocked_by_m2_status_total / turns_total >= 0.05
```

`target_missing` y `control_conflict` **no** cuentan como bloqueo jurídico M2:
tienen buckets propios en el analizador y no entran a
`blocked_by_m2_status_total`.

---

## Evidencia

### Commits integrados en `main`

En orden de aplicación. Los hashes de origen corresponden a
`search/modality-contract-v1`, donde el trabajo se desarrolló.

| # | hash en `main` | hash de origen | mensaje |
| --: | --- | --- | --- |
| 1 | `af55772c23ddfd418080bb264c084a9c29c9c8c9` | `e2e3db5` | `feat(modality): add canonical live modality catalog` |
| 2 | `53812d1a502639057894f5b89ffb9efebd611ea1` | `82b17c8` | `feat(modality): add compositional modality resolver` |
| 3 | `361fd6d49813d93e85b57f1b1c3b332daaa570a3` | `2bae6cb` | `feat(modality): add coordinated modality alternatives` |
| 4 | `8e23e43737dfa8aa5bf7366dea8fb12a6a59838f` | `e1009df` | `feat(modality): add query integration contracts` |
| 5 | `5b75e675c6fa2c56720dea5ee92499c9fcf02f3e` | `f94ffbb` | `feat(modality): integrate canonical query pipeline` |
| 6 | `d397cdfb101ac1a7f04e9858927f75cbb8e75a4a` | `33bbdd3` | `feat(modality): add pure conversational mutation algebra` |
| 7 | `2e3c8895467706de953c58f6131c5c42866aa3a7` | `16da02e` | `fix(modality): bind follow-up controls to resolved target spans` |
| 8 | `eaa051fbc4532b0eb00f36d1b83b30b9fe396e87` | `4096a2c` | `feat(modality): wire conversational modality mutations` |
| 9 | `63d22d28c7a59679d087036cbbf305928f743ecb` | `ec55a81` | `feat(modality): add follow-up product gate telemetry` |
| 10 | `84a95181e71cee58d78f8711cdf0d6be0eebbe63` | `3ea5dc6` | `fix(modality): preserve closed replacement target polarity` |
| 11 | `c5f857e6234bdea1d4bc539aad257e0b35ffe2c6` | `3cc81cb` | `test(modality): close conversational follow-up contract` |

Los once se aplicaron sin conflictos. No se reescribió historia en la rama de
origen.

### Validación medida sobre `main`

```text
lint core           app/core/ limpio
focal modalidad     526 passed, 1 xfailed
suite completa      5381 passed, 2 skipped, 24 xfailed
query corpus        89/89 PASS
dialogue corpus     60/61 PASS
dialogue E2E        29/29 secuencias PASS
```

Fallo basal aceptado, ajeno a modalidad y con firma idéntica al baseline:

```text
GOB-DEPTO-001
entidad_resolved: expected 'GOBERNACIÓN DE BOYACÁ', got None
```

> El closeout registra `6592 passed` para la suite completa. Esa cifra se midió
> en la rama de origen, cuyo árbol contiene además pruebas de otros bloques que
> `main` no tiene. La diferencia es de cobertura del árbol, no una regresión:
> aquí no falla ninguna prueba.

### Dogfood manual

Registrado como evidencia de **paridad ortogonal**, no como muestra para la
compuerta M3C-D:

```text
T1: mínimas cuantías de mantenimiento abiertas
T2: en la costa atlántica
T3: entre 30 y 50 millones
```

Resultado observado:

- modalidad preservada;
- estado preservado;
- topic preservado;
- región Caribe añadida;
- rango completo añadido;
- ruta `heuristic_only`;
- LLM no utilizado.

> Estos turnos no son operaciones de follow-up de modalidad y no cuentan para la
> compuerta M3C-D.

---

## Alternativas descartadas

- **Permitir que el LLM reescriba todos los params.** Convierte cada campo del
  sistema en superficie de ataque y hace que la reproducibilidad del turno
  dependa del modelo.
- **Lista negra creciente de campos protegidos.** Cada campo nuevo nacería
  abierto y la protección llegaría siempre tarde, después del primer incidente.
- **Reejecutar M2 después del merge.** Duplica la autoridad jurídica, permite
  que dos resoluciones del mismo turno discrepen, y cuesta una segunda pasada.
- **Inferir el target jurídico desde el lenguaje de control.** El clasificador
  habría tenido que conocer aliases de modalidad, replicando la autoridad de M2
  en una capa que no puede validarla.
- **Construir inmediatamente un sidecar de aclaración.** Sin muestra real no hay
  evidencia de que los bloqueos ocurran con frecuencia relevante; el sidecar
  queda condicionado a la compuerta.
- **Aplicar mutaciones parciales cuando un target queda bloqueado.** Produce
  búsquedas que el usuario no pidió y ejecuta SoQL sobre un estado a medias.

---

## Consecuencias

**Positivas**

- Comportamiento determinístico y auditable: la misma frase produce la misma
  operación y el mismo filtro final.
- Composición segura con topic, scope, monto, fecha y dataset; el follow-up
  ortogonal no destruye la modalidad y viceversa.
- Los campos nuevos quedan cerrados por defecto para el LLM.
- Cobertura por pruebas algebraicas (motor puro) y E2E (corpus multi-turn
  congelado de 29 secuencias / 64 turnos).
- El sidecar de aclaración queda condicionado por evidencia de producto en vez
  de por intuición.

**Costos**

- Mayor superficie de tests: el álgebra, el clasificador, el wiring, la
  precedencia y el corpus se prueban por separado.
- El vocabulario de control queda estrictamente gobernado: ampliar una frase es
  una decisión, no un parche.
- Ciertos casos ambiguos (`sin modalidad`, `agrega y quita X`) requieren
  aclaración explícita del usuario en vez de resolverse por heurística.
- La telemetría debe acumular uso real antes de que M3C-D pueda evaluarse.

---

## Enlaces

- As-built canónico: `docs/planes/2026-07-25_M3C_modality_conversation_closeout.md`
- Diseño aprobado (baseline histórico): `docs/planes/2026-07-23_M3C_modality_conversation_contract.md`
- Mapa de integración previo: `docs/planes/2026-07-23_M3A_modality_integration_contract_design.md`
- Enmiendas control↔target: `docs/planes/2026-07-24_M3C_B_R3_target_span_control.md`
- Diseño del product gate: `docs/planes/2026-07-25_M3C_CG_product_gate.md`
- Guía operativa: `docs/guides/MODALITY_CONVERSATION_GUIDE.md`
