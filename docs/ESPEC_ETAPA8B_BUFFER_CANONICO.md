# Etapa 8B — Buffer canónico de paginación

> **Estado:** **E8A ✅ · E8B.1 ✅ · E8B.2 ✅** (2026-06-29). Etapa 8 cerrada con
> ruta canónica única, buffer persistido por grupo, refill, TTL y tres totales.

## Problema histórico que E8A no resolvía por sí sola

El modelo actual re-pesca la ventana SECOP en cada "más" y la rebanada es determinista
(E8A añadió el desempate por id → sin empates no deterministas; el fingerprint guard evita
continuar un cursor de otra consulta/contrato). PERO:

1. **Re-consulta en cada página** (no hay buffer): coste y exposición a la variabilidad de
   Socrata en cada "más".
2. **Validación de frases INCONSISTENTE entre página 1 y siguientes** (el defecto real):
   la página 1 (normal_flow) pasa por `rank_results` + validador de frases (descarta `none`);
   "más" usa `rank_and_dedupe` (formatter), que NO aplica la misma validación → una fila
   filtrada en la página 1 puede reaparecer en la 2, o el orden diferir. Peligroso cuando
   entren PhraseSpec complejas / nodos semánticos.
3. **Un solo "total"** ambiguo (universo SECOP) — no se distingue de validados/mostrados.

## Precisión de implementación — UNIFICAR antes de CACHEAR (crítico)

> Añadido 2026-06-29. "Buffer canónico" se puede malinterpretar como "añadir caché". **El
> buffer NO es el arreglo; es la consecuencia.** El defecto raíz son DOS rutas de ranking
> (página 1 = `rank_results`/`rank_candidate_pool` + validador de frases; "más" =
> `rank_and_dedupe` del formatter, sin validación). Cachear sin unificar solo CACHEA la
> inconsistencia: la página 2 seguiría con distinta calidad, ahora persistida.

**Orden obligatorio dentro de E8B:**
```
1. UNIFICAR: handle_pagination usa la MISMA ruta de validación+ranking que normal_flow
   (rank_candidate_pool / rank_results), NO rank_and_dedupe. Verificable de inmediato:
   la concatenación de páginas == la secuencia validada+rankeada completa de la fuente.
2. CACHEAR: recién entonces, guardar esa salida única en validated_ranked_buffer + cursores
   (lo demás de esta spec). El buffer es caché de la ruta unificada, no un segundo camino.
```
Si se hace al revés (buffer primero), el invariante "P1 ≡ PN" no se cumple aunque las
páginas no se repitan.

## Riesgo de primer orden — dual-dataset (diseñar, no descubrir)

El buffer es **por grupo** (procesos y contratos tienen rankings independientes; no hay
ranking global — ADR-038). Un buffer único global mezclaría universos y totales. El guard
dual actual ("más" tras dual pregunta cuál grupo) debe integrarse: el buffer se asocia a
`(fingerprint, grupo)`, y "más procesos"/"más contratos" avanzan cursores distintos.

## Modelo de estado (a persistir por conversación)

```
PaginationBuffer:
  query_fingerprint: str          # sha256(consulta + contrato), de query_fingerprint.py
  candidate_buffer: list[Row]      # candidatos crudos traídos de la fuente (dedup por id)
  validated_ranked_buffer: list[Row]  # tras validación+ranking UNA sola vez
  display_cursor: int              # cuántos validados ya se mostraron
  source_cursor: int              # OFFSET fuente del próximo lote a traer
  source_candidate_total: int      # count del universo fuente (puede ser X+)
  validated_total_known: int       # validados conocidos hasta ahora (crece al traer más)
  created_at / ttl                 # expiración
```

## Flujo

```
1. Buscar: fuente → candidate_buffer (≤200) → validar+rankear UNA vez →
   validated_ranked_buffer. Mostrar [0:25]. display_cursor=25.
2. "más": mostrar validated_ranked_buffer[display_cursor : +25] del MISMO buffer
   (sin re-consultar). display_cursor += 25.
3. Al agotar el buffer validado: traer siguiente lote fuente (source_cursor),
   validar con la MISMA versión, deduplicar por id estable, fusionar al buffer,
   continuar. source_cursor += lote.
```

## Invariantes

- **Misma validación/ranking en TODAS las páginas** (página 1 ≡ página N). Versión de
  PhraseSpec/ranking/validador en el fingerprint (`extra`); si cambia → buffer inválido.
- **Sin duplicados**: dedup por identificador estable (`id_del_proceso`/`id_contrato`) al
  fusionar lotes.
- **Sin skip / sin overlap**: el orden fuente es estable (E8A: `... , id DESC`).
- **Tres totales separados** (nunca llamar "total" a universos distintos):
  `source_candidate_total` · `validated_total_known` · `displayed_so_far`.
- **Reanudación tras reinicio**: el buffer vive en el store; si expiró (TTL) o el
  fingerprint no coincide → re-pescar desde la página 1 (E8A ya hace el guard).

## Límites de memoria

- `candidate_buffer` acotado (p.ej. ≤ 600 filas o N lotes), TTL corto (minutos), payload
  recortado a los campos que el display/validador necesitan (no guardar filas crudas
  enormes indefinidamente). Evicción por TTL y por tamaño.

## Implementación cerrada (E8B.2)

- Snapshot JSON lateral y atómico por conversación; una entrada independiente por grupo.
- TTL: 15 minutos. Un buffer vencido o con fingerprint incompatible se elimina y reinicia
  desde la página 1.
- Límite: 600 candidatos y 600 validados; al alcanzarlo se informa capacidad agotada, no
  se afirma falsamente que se terminó el universo.
- `source_total_approximate` conserva la semántica `X+`: un count fallido no bloquea refill.
- `source_candidate_total`, `validated_total_known` y `displayed_so_far` viajan separados.
- Reset elimina historial y buffers; una nueva instancia del workflow reanuda el snapshot.
- Dual mantiene cursores separados para procesos y contratos; “más” pregunta el grupo y
  “más procesos/contratos” avanza únicamente el buffer seleccionado.

## Plan de migración

- Reusar `query_fingerprint` (ya versiona vía `extra`) y el orden estable de E8A.
- Añadir campos al store con **ciclo de vida definido** (no campos huérfanos): creación en
  la búsqueda, lectura/avance en "más", invalidación por fingerprint/TTL.
- `handle_pagination` deja de re-pescar+rebanar y pasa a leer del buffer; el refetch solo
  ocurre al agotar el buffer validado.
- Tests: validación idéntica P1≡PN, dedup al fusionar lotes, 3 totales coherentes,
  expiración por TTL, reanudación, consultas duales (buffer por grupo).

## Gate de aceptación (no "aprobar en falso")

No basta con "las páginas no se repiten". El gate clave —el más fácil de aprobar en
falso— es **P1 ≡ PN**: la **concatenación de todas las páginas** mostradas debe ser
**idéntica** (mismas filas, mismo orden) al prefijo de la **secuencia validada+rankeada
completa** que produce la ruta única sobre la fuente. El test debe construir ambas y
comparar la lista, no solo verificar disjunción. Perfil dirigido de paginación que cierra
E8B: literal simple · PhraseSpec · exclusiones · empates fecha+valor · agotamiento y
ampliación del buffer · dual. Solo con eso se declara **Etapa 8 cerrada**.

## Qué NO hacer

- No implementar el buffer a medias ni persistir campos sin ciclo de vida.
- No declarar Etapa 8 completa hasta que E8B pase sus invariantes en corrida real.
