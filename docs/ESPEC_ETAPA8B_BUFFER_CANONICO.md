# Etapa 8B — Buffer canónico de paginación (SPEC, pendiente de implementar)

> **Estado:** Etapa 8 dividida (asesor, 2026-06-29). **E8A HECHA** (orden estable por id +
> fingerprint guard); **E8B PENDIENTE** — esta spec. **Etapa 8 NO está cerrada** hasta E8B.
> E8B es stateful (estado conversacional, memoria, TTL) → merece sesión fresca y enfocada.

## Problema que E8A NO resuelve

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

## Plan de migración

- Reusar `query_fingerprint` (ya versiona vía `extra`) y el orden estable de E8A.
- Añadir campos al store con **ciclo de vida definido** (no campos huérfanos): creación en
  la búsqueda, lectura/avance en "más", invalidación por fingerprint/TTL.
- `handle_pagination` deja de re-pescar+rebanar y pasa a leer del buffer; el refetch solo
  ocurre al agotar el buffer validado.
- Tests: validación idéntica P1≡PN, dedup al fusionar lotes, 3 totales coherentes,
  expiración por TTL, reanudación, consultas duales (buffer por grupo).

## Qué NO hacer

- No implementar el buffer a medias ni persistir campos sin ciclo de vida.
- No declarar Etapa 8 completa hasta que E8B pase sus invariantes en corrida real.
