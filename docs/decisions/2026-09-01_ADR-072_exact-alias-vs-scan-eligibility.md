# ADR-072 — Ownership exacto y elegibilidad de scan son autoridades distintas

**Fecha:** 2026-09-01
**Estado:** Accepted — `SCAN_ALIAS_ELIGIBILITY_R2D3`

**Evidencia:**
[`SCAN_ALIAS_ELIGIBILITY_R2D3.md`](../gazetteer/SCAN_ALIAS_ELIGIBILITY_R2D3.md)
y
[`freed_alias_safety_r2d2.json`](../../artifacts/audits/freed_alias_safety_r2d2.json).

## Contexto

El gazetteer suministraba la misma lista a dos mecanismos con semántica
distinta:

```text
resolve_entidad(alias)       → la frase completa identifica al owner
scan_entity(texto más largo) → cualquier aparición del alias identifica al owner
```

R2D.2 demostró que la segunda implicación es falsa: nueve aliases liberados por
D6 capturaron 17 compradores físicos distintos. Un topónimo municipal puede
ser una abreviatura exacta razonable y, a la vez, ser sólo la jurisdicción
dentro de `CONCEJO MUNICIPAL DE PUERTO BOYACA`.

Inferir la separación sólo por longitud, cabezas institucionales o topónimos
ampliaría sin evidencia el cambio a los 2.104 aliases liberados y rompería
formas buenas como las regionales SENA/ICBF. Meter nueve excepciones en
`scan_entity` ocultaría la decisión en código y mezclaría de nuevo autoridad y
mecanismo.

## Decisión

Los registros del gazetteer admiten dos listas declarativas opcionales:

```json
{
  "aliases": ["puerto boyaca"],
  "scan_disabled_aliases": ["puerto boyaca"],
  "resolution_blocked_aliases": []
}
```

- `aliases` conserva ownership exacto.
- `scan_disabled_aliases` contiene claves que siguen en `aliases`, pero no
  pueden entrar a `_scan_index`.
- `resolution_blocked_aliases` conserva evidencia negativa de una clave
  retirada de `aliases`; `resolve_entidad` falla cerrado antes del fuzzy para
  que éste no fabrique de nuevo el owner retirado.

`app/core/scan_alias_policy.py` interpreta esos campos de forma pura y sin
nombres institucionales hardcodeados. `EntityResolver` es el mecanismo que
aplica la decisión al construir índices. Una clave bloqueada que aún tenga
owner es un conflicto de schema y detiene la carga.

La elegibilidad no se deriva automáticamente. Cada entrada exige evidencia
auditada. En R2D.3, tres aliases conservan exacto y pierden scan; seis pierden
ownership y quedan bloqueados frente a fuzzy.

## Consecuencias

- Tier 1 y scan pueden evolucionar sin borrar formas exactas legítimas.
- Las negativas quedan visibles y revisables en datos, no en condiciones del
  hot path.
- El schema gana dos campos opcionales; los registros anteriores conservan
  exactamente su comportamiento.
- Añadir metadata sin evidencia no queda autorizado por este ADR.
- `geography_alias.py` y la regla B' de D6 no cambian.

## Alternativas descartadas

1. **Heurística global:** no dispone en runtime del cruce físico SECOP y abre
   falsos negativos fuera de los nueve acreditados.
2. **Denylist en `scan_entity`:** acopla nombres concretos al mecanismo y no
   expresa el comportamiento exacto.
3. **Eliminar los nueve:** destruye tres abreviaturas municipales exactas que
   la evidencia sí sostiene.
4. **Conservar los nueve sólo con longest-match:** no cubre compradores
   ausentes del gazetteer ni los otros ocho hijacks.
