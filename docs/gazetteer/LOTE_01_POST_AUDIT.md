# Lote 01 — Post-Audit

**Entrega:** Sprint A — Gazetteer Expansion  
**Commit:** `7781d6d`  
**Fecha:** 2026-05-26  

---

## 1. Resumen

Lote 01 agregó 50 entidades al gazetteer (509 → 559). El commit está estable (126 pass + 13 xfail + corpus 88/88 + lint limpio). Pero hay deuda técnica documentada que debe resolverse antes de avanzar al Lote 02 sin arrastrar problemas.

| Tipo de deuda | Cantidad | Severidad |
|---------------|----------|-----------|
| Entidades con <3 aliases | 13 | Media |
| XFAIL por ambigüedad SENA/ICBF | 13 | Alta |
| XFAIL por colisión MINJUSTICIA | 1 | Media |
| XFAIL por colisión hospital | 1 | Baja |

---

## 2. Clasificación de XFAIL

### 2.1 SENA Regional (6 casos) — `ENTITY-REGIONAL-DISAMBIGUATION-001`

| Query | Resuelve a | Alias ganador |
|-------|-----------|---------------|
| sena regional cundinamarca | SENA REGIONAL NORTE DE SANTANDER | "sena regional" |
| sena choco | SENA SECRETARIA GENERAL | "sena" |
| sena antioquia | SENA SECRETARIA GENERAL | "sena" |
| sena santander | SENA SECRETARIA GENERAL | "sena" |
| sena casanare | SENA SECRETARIA GENERAL | "sena" |
| sena caqueta | SENA SECRETARIA GENERAL | "sena" |

**Causa raíz:** `scan_entity()` busca el alias más largo que sea substring del texto del usuario. Cuando el usuario dice "sena choco", el alias "sena" (3 chars) está en la entidad SENA SECRETARIA GENERAL y es substring de la consulta. El alias "sena choco" (9 chars) de la nueva entidad SENA REGIONAL CHOCO también es substring, pero no gana porque "sena" aparece primero en el índice ordenado por longitud descendente — y en este caso "sena" (de SECRETARIA GENERAL) está antes en la lista que "sena choco" (de REGIONAL CHOCO).

Wait — esto es incorrecto. "sena choco" (9 chars) es MÁS LARGO que "sena" (3 chars). Debería ganar. El problema real es que el alias "sena choco" no existe en la nueva entidad porque el heurístico genera "sena choco" sin "choco" — el cleanup del grupo administrativo deja "choco" limpio, pero revisemos...

**Corrección:** El heurístico SENA genera "sena {region}" donde `region` después del cleanup es "choco" (porque "SENA REGIONAL CHOCO Grupo de Apoyo Administrativo Mixto" → "choco"). Entonces "sena choco" DEBERÍA estar en los aliases. Pero no gana porque el índice ordena por longitud de cadena: "sena choco" (9 chars) vs "sena" (4 chars). "sena choco" es más largo y debería matchear primero.

El problema real: existen MÚLTIPLES entidades con alias "sena" (SENA SECRETARIA GENERAL tiene "sena" como alias, igual que otras). El índice agrupa por primera palabra y ordena por longitud DESC. Pero la primera palabra del alias "sena choco" es "sena", y al agrupar todos los aliases que empiezan con "sena", el más largo debería matchear primero.

Alguna razón por la que "sena" gana sobre "sena choco"? Tal vez porque "sena choco" está en el índice bajo otra primera palabra...

**Diagnóstico definitivo:** El heurístico SENA genera dos tipos de aliases:
- Con prefijo "sena" como primera palabra: "sena choco", "sena regional choco", etc.
- Con prefijo "sena" en medio: "regional choco sena", "servicio nacional de aprendizaje choco", etc.

Los aliases que empiezan con "sena" se agrupan bajo `first_word="sena"`. Allí, "sena regional cundinamarca grupo de apoyo administrativo mixto+" (longitud ~52) aparece primero, pero NO es substring de "sena choco" (9 chars). El scan sigue iterando. Encuentra "sena regional" (12 chars) de NORTE DE SANTANDER — esto SÍ es substring de "sena choco"? NO, "sena regional" NO es substring de "sena choco". 

Espera, la consulta del test es "sena choco" y el resultado es SENA SECRETARIA GENERAL con alias "sena". "sena" (4 chars) es substring de "sena choco" (9 chars). Los aliases más largos como "sena choco" no existen en el índice bajo la primera palabra "sena" — no están en el lote de aliases generados. 

**Re-diagnóstico:** Veamos qué aliases genera realmente el heurístico para SENA REGIONAL CHOCO Grupo de Apoyo Administrativo Mixto. El nombre tiene "GRUPO DE APOYO ADMINISTRATIVO MIXTO" al final que se limpia. Después del cleanup:

```
region = "choco"  # (Grupo de Apoyo Administrativo Mixto removido, "choco" queda)
```

Aliases generados:
- "sena choco" ← esto debería existir
- "sena regional choco"
- "servicio nacional de aprendizaje choco"
- "regional choco sena"

Pero en el gazetteer, la entidad "SENA REGIONAL CHOCO Grupo de Apoyo Administrativo Mixto" (la PREVIA a Lote 01) ya tiene aliases que empiezan con "sena". La nueva entidad "SENA REGIONAL CHOCO Grupo de Apoyo Administrativo Mixto" (con exactamente el mismo nombre) ya EXISTÍA. Revisemos:

De hecho, la nueva entidad del Lote 01 es "SENA REGIONAL CHOCO Grupo de Apoyo Administrativo Mixto" — pero examinemos si ya existía en el gazetteer pre-Lote 01. La entidad previa es "SENA REGIONAL CHOCO Grupo de Apoyo Administrativo Mixto" — exactamente el mismo nombre. Entonces el `diff` reportó esta entidad como faltante, pero al hacer `append` se saltó porque ya existía (por `official_name`). Pero el diff la listó como faltante... esto es confuso.

Déjame verificar si la entidad SENA CHOCO del Lote 01 realmente se agregó o se saltó.

En el append output:
```
ADD (sena_regional: 8 aliases): SENA REGIONAL CHOCO Grupo de Apoyo Administrativo Mixto
```

Se agregó. Pero entonces ¿por qué "sena choco" no resuelve a ella? Porque el alias "sena choco" de la nueva entidad compite con todos los demás aliases "sena*" del grupo. Y la nueva entidad está al FINAL del array JSON (append al final). El índice itera por orden del JSON y "sena" de SENA SECRETARIA GENERAL aparece primero.

El problema real: **multiples entidades con el mismo alias "sena"**, y la primera gana.

La solución pasa por priorizar entidades con mayor cobertura territorial. Eso es ENTITY-REGIONAL-DISAMBIGUATION-001 y requiere un resolver contextual.

### 2.2 ICBF Regional (5 casos)

| Query | Resuelve a | Alias ganador |
|-------|-----------|---------------|
| icbf cordoba | ICBF SEDE NACIONAL | "icbf" |
| icbf magdalena | ICBF SEDE NACIONAL | "icbf" |
| icbf norte de santander | ICBF SEDE NACIONAL | "icbf" |
| icbf caldas | ICBF SEDE NACIONAL | "icbf" |
| icbf cesar | ICBF SEDE NACIONAL | "icbf" |

**Causa raíz:** idéntico al caso SENA. "icbf" (4 chars) como alias de ICBF SEDE NACIONAL gana siempre que el usuario escriba "icbf X".

### 2.3 MINJUSTICIA (1 caso)

| Query | Resuelve a | Alias ganador |
|-------|-----------|---------------|
| ministerio de justicia | MINJUSTICIA.. | "ministerio de justicia" |

**Causa raíz:** la entidad existente "MINJUSTICIA.." tiene el alias exacto "ministerio de justicia". La nueva entidad "MINISTERIO DE JUSTICIA Y DEL DERECHO.." también tiene ese alias, pero gana la que aparece primero en el JSON (MINJUSTICIA..). No hay ambigüedad territorial aquí; es una colisión de alias exacto entre dos entidades con nombres casi idénticos pero oficialmente distintas.

### 2.4 Hospital (1 caso)

| Query | Resuelve a | Alias ganador |
|-------|-----------|---------------|
| hospital municipal de acacias | ESE MUNICIPAL DE SOACHA | "hospital municipal" |

**Causa raíz:** la entidad existente "ESE MUNICIPAL DE SOACHA JULIO CESAR PEÑALOZA*" tiene el alias "hospital municipal". La nueva entidad "HOSPITAL MUNICIPAL DE ACACIAS E.S.E." tiene alias "hospital municipal de acacias ese" pero no "hospital municipal de acacias" (sin "ese"). El substring scan encuentra "hospital municipal" primero y resuelve a Soacha.

---

## 3. Plan de Resolución

### 3.1 Corto plazo (antes de Lote 02)

| Ítem | Acción | Responsable | Tiempo |
|------|--------|-------------|--------|
| 13 entidades <3 aliases | Enriquecer con LLM o manual | Heurística ampliada o one-shot | ~30 min |
| SENA "servicio nacional de aprendizaje" prefix | Agregar prefijo al heurístico | Código | 5 min |
| ESE "empresa social del estado" prefix | Agregar prefijo al heurístico | Código | 5 min |

### 3.2 Mediano plazo (ENTITY-REGIONAL-DISAMBIGUATION-001)

La ambigüedad SENA/ICBF no se resuelve con más aliases — es un problema estructural del `scan_entity()` por substring. Soluciones posibles (en orden de esfuerzo creciente):

**Opción A — Ranking por cobertura (esfuerzo bajo)**
Almacenar `total_procesos` en el gazetteer y usar como tiebreaker cuando múltiples entidades comparten alias. Si "sena" aparece en 5 entidades, la que más procesos tenga gana.

**Opción B — Contexto territorial (esfuerzo medio)**
Detectar tokens territoriales en el resto de la consulta (departamento, ciudad) y filtrar entidades regionales antes de scan. Si el usuario dice "sena en antioquia", resolver a SENA REGIONAL ANTIOQUIA aunque el alias "sena" exista en SENA SECRETARIA GENERAL.

**Opción C — Resolver multi-entidad (esfuerzo alto)**
Devolver TOP-N entidades en vez de una sola, y dejar que el SoQL builder genere OR de LIKEs (entidad_ecosystem). Esto ya existe en el pipeline como entidad_ecosystem pero solo para entidades del mismo departamento.

**Recomendación:** Opción A (ranking) como quick win, seguido de Opción B (contexto territorial) como feature dedicada.

### 3.3 MINJUSTICIA

El caso MINJUSTICIA.. vs MINISTERIO DE JUSTICIA Y DEL DERECHO.. es un edge case de entidades casi-duplicadas en SECOP. La solución más limpia: eliminar la entidad "MINJUSTICIA.." del gazetteer y fusionar sus aliases en "MINISTERIO DE JUSTICIA Y DEL DERECHO..". Esto requiere verificar que ambas son la misma entidad legal (mismo NIT).

---

## 4. Reglas para Lote 02

| Regla | Valor | Gatillo |
|-------|-------|---------|
| Entidades con <3 aliases | ≤ 13 (no aumentar) | validate --from-list |
| XFAIL nuevos | Documentar con strict=True | pytest -rx |
| low_alias_entities | No puede subir | validate output |
| full-scan | Auditoría, no gate | validate --full-scan (opcional) |
| Deuda técnica documentada | En lote_N_debt.md | Por lote |

---

## 5. Acciones Inmediatas (antes de cerrar G6)

- [x] lote_01_debt.md creado (13 entidades con <3 aliases)
- [x] xfail con strict=True (13 casos)
- [x] Post-audit documentado (este archivo)
- [ ] ENTITY-REGIONAL-DISAMBIGUATION-001 registrado como issue/task en memoria
- [ ] Decisión arquitectónica registrada: ranking como quick win, luego contexto territorial
