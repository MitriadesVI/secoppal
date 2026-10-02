# Deuda P3 de C1.2 — exención `data/conversations/` en el pin de frontera

**Registrada:** 2026-10-01 (misión M2A-0). **Dueño:** Núcleo. **Vence:** CUTOVER (decisión expresa). **No bloquea.**
Ledger: ADR-103 Enm. 5 punto 5. Esta misión **no** cambia el pin.

## Hecho

- `tests/test_m1_wave1_parity_pins.py` (`NON_DATA_LITERALS`) exime el literal `("app.core.conversation_store",
  "data/conversations")` con el motivo «runtime conversation store; its tracked files are incidental».
- `git ls-files data/conversations` = **20** JSONL versionados (`st_default.jsonl`, `probe_state_only.jsonl` y 18
  `st_anon_<id>.jsonl`). Figuran en el manifiesto `legacy_closure_manifest_88653752.json`, pero ningún pin compara su
  sha256: quedan fuera del congelamiento de datos de D1(b,d).

## Por qué no bloquea

- No son gramática ni datos de interpretación: son historiales de chats concretos (de prueba o anónimos). Cambiarlos
  altera, como mucho, el estado de runtime de esos chats; no cambia cómo legacy interpreta un turno.
- El directorio es estado de runtime por diseño; congelarlo entero rompería el uso normal (cada turno escribe ahí).
- El almacén M2a (`<chat>.accepted.jsonl`) vivirá en el mismo directorio y queda cubierto por la misma exención
  (`.jsonl` de runtime): la exención es necesaria, no accidental.

## Opción de endurecimiento futuro

1. Pinear por sha256 **sólo** los 20 archivos versionados (lista explícita), manteniendo exento lo no versionado; o
2. des-versionarlos (`git rm --cached` + `.gitignore`), si nada los usa como fixture.

Cualquiera de las dos exige el motivo declarado y ZEUS de D1(b) (regenerar el manifiesto o tocar el pin).
