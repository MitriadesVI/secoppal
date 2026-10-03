# Deuda de retención — `ConversationStore.clear()` no borra `.generations/<chat>/`

**Registrada:** 2026-10-02 (preparación de la ola 1 de M2a). **PRE-EXISTENTE** (no la introduce M2a). **Dueño:** Núcleo.
**Vence:** sin fecha; la fija el usuario. **No bloquea.** Esta preparación **no** cambia ningún comportamiento.

## Hecho

- `ConversationStore.clear()` (`app/core/conversation_store.py:277-287`) avanza la época y borra el sobre (envelope), el
  historial plano (`<chat>.jsonl`) y la paginación (`<chat>.pagination.json`). No borra `.generations/<chat>/`.
- `generation_path` (`app/core/workflow/continuity_staging.py:18-23`) ubica las copias en
  `<base_path>/.generations/<chat saneado>/<generación>/{history.jsonl,pagination.json}`. En producción las crea
  `stage_continuity` (`continuity_staging.py:38-56`), que `coordinate_turn`
  (`app/core/workflow/turn_coordination.py:31` y `:52`) invoca en cada turno con reserva; la copia es `shutil.copyfile`.
- Nada las borra: en `app/` y `scripts/` no hay `rmtree` ni `unlink` sobre `.generations`, y `continuity_staging.py` es el
  único archivo que nombra ese directorio (`shutil` se usa allí sólo para `copyfile`). Comprobado con `grep` el 2026-10-02.

## Por qué importa

- Las copias del historial (con datos personales de la conversación) **sobreviven al reset**: el usuario cree que el
  historial se borró y quedan copias por generación.
- Cada turno con reserva crea un directorio de generación nuevo (`reservation.result_id`) y nada purga los anteriores.
- El plan fija «retención = historial» para el almacén M2a. Mientras `.generations` sobreviva al reset, el «historial» tiene
  copias que el reset no alcanza (el almacén M2a no se copia allí: `stage_continuity` sólo copia `history.jsonl` y
  `pagination.json`).

## Por qué no se repara aquí

- Es una decisión de producto y privacidad (qué significa «reset» para las copias), no un detalle de M2a.
- `conversation_store.py` y `continuity_staging.py` están en el cierre legacy: tocarlos exige divergencia declarada y pin de
  delta (plan §0.6), no regeneración del manifiesto.

## Qué hace M2A-2

**Caracteriza** (con un test o con evidencia reproducible) que tras `clear()` el directorio `.generations/<chat>/` persiste.
**No la repara**, salvo autorización posterior del usuario. La caracterización no se presenta como contrato.

## Opciones de reparación futura (a decidir con el usuario)

1. `clear()` borra `.generations/<chat saneado>/` bajo `conversation_lock`, junto con el resto (cambio en un módulo legacy:
   divergencia declarada + pin de delta; hay que probar la carrera con un turno en vuelo, que la época ya invalida).
2. Purga por generación: al publicar, borrar las generaciones que no sean la del sobre vigente (resuelve también la
   acumulación por turno, pero no por sí sola el borrado en el reset).
