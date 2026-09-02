# FREED-ALIAS-SAFETY-R2D2 — seguridad de ownership tras liberar D6

**Tipo:** auditoría read-only · **Fecha:** 2026-09-01

**Rama:** `main` · **HEAD antes:** `cafe9fc09237dd22a64dc643ba0491f9303dd6e6`

**Evidencia:**
[`freed_alias_safety_r2d2.json`](../../artifacts/audits/freed_alias_safety_r2d2.json)

**Reproductor:**
[`audit_freed_alias_safety.py`](../../scripts/audits/audit_freed_alias_safety.py)

## 1. Veredicto

```text
FREED_ALIAS_SAFETY_R2D2: PASS
FREED_ALIASES:             2.104
HIGH:                         16
MEDIUM:                       76
SAFE:                      1.963
REVIEW:                       49
NEW_WRONG_OWNER_CONFIRMED:
  aliases:                      9
  compradores físicos:        17
R2D1_VERDICT: REMEDIATION_REQUIRED
```

La auditoría terminó y su evidencia es reproducible; por eso R2D.2 es `PASS`.
El resultado auditado no autoriza, sin embargo, a cerrar R2D.1: la liberación
activó 17 capturas nuevas de compradores físicos distintos, concentradas en 9
aliases. No se tocó runtime ni `aliases_db.json` para corregirlas.

## 2. Universo y catálogo físico

`FREED_BY_D6` se reconstruyó, no se tomó de una lista manual:

```text
aliases totales                                  8.645
rechazados por OLD                               2.124
rechazados por B'                                   20
rechazados por OLD y aceptados por B'            2.104
```

El cruce usa el directorio oficial
[`SECOP II - Contacto Entidades y Proveedores`](https://www.datos.gov.co/resource/4ex9-j3n8.json),
filtrado por `es_entidad='Si'`: **27.286 compradores físicos registrados**.
La identidad distinta se cuenta por NIT normalizado sin DV y, si el NIT no es
utilizable, por código físico SECOP. Las variantes del mismo NIT no se cuentan
como compradores ajenos.

La contención es una secuencia completa de tokens normalizados. `valle` no
coincide con `valledupar`; no hay `substring` crudo. El cruce halló **43**
aliases presentes literalmente en el nombre físico de al menos otro comprador.
El canario Santa Marta añade un caso adversarial verificado cuya frase combina
el nombre natural del comprador con su departamento, aunque el nombre físico
SECOP no incluya `Magdalena`.

### 2.1 Niveles de riesgo

| Nivel | Casos | Criterio |
|---|---:|---|
| `HIGH` | 16 | 1–3 tokens, sin cabeza institucional y presente en otro comprador; más el canario Terminal adjudicado |
| `MEDIUM` | 76 | otro comprador contenedor, o alias territorial sin cruce literal actual |
| `SAFE` | 1.963 | sin comprador ajeno contenedor y sin señal territorial genérica |
| `REVIEW` | 49 | ownership exacto múltiple; R2D.2 no elige dueño |

`SAFE` significa seguro frente al catálogo y al mecanismo medidos. No significa
que la longitud o la intuición hayan demostrado universalmente su corrección.

### 2.2 Clases

| Clase | Casos |
|---|---:|
| `GENERIC_LOCATION_ALIAS` | 61 |
| `PARENT_ALIAS_IN_CHILD_NAME` | 1 |
| `CHILD_ALIAS_IN_UNRELATED_NAME` | 23 |
| `INSTITUTIONAL_FRAGMENT` | 7 |
| `SAFE_SPECIFIC_ALIAS` | 1.963 |
| `REVIEW` | 49 |

## 3. Canario Terminal Santa Marta: `ALIAS_HIJACK`

La identidad sí existe en SECOP II y no es la Alcaldía:

```text
NOMBRE FÍSICO: CENTRAL DE TRANSPORTE DE SANTA MARTA LTDA.
CÓDIGO SECOP:  724398862
NIT SECOP:     891701694
ESTADO:        activo
UBICACIÓN:     Santa Marta, Magdalena
PROCESOS:      227
CONTRATOS:     0
```

La razón social y el DV también están publicados por la propia entidad:
`CENTRAL DE TRANSPORTE DE SANTA MARTA LTDA.`, NIT `891.701.694-2`, en su
[`Informe de Gestión 2024`](https://terminaldesantamarta.com/imagenes/documentos/INFORME_DE_GESTI%C3%93N_2024_CTSM.pdf).

No hay registro específico de la Central en `aliases_db.json`. La frase que
motivó el frente produce:

```text
"Terminal de Transportes de Santa Marta Magdalena"

LIVE B'  -> ALCALDÍA DISTRITAL DE SANTA MARTA
             matched_alias = "santa marta magdalena"
OLD      -> FAIL_CLOSED como comprador; conserva Magdalena como territorio
```

`santa marta magdalena` no contiene cabeza institucional: sólo ciudad y
departamento. La frase completa nombra naturalmente a la Terminal y no aporta
evidencia de que el comprador sea la Alcaldía. El veredicto es
**`ALIAS_HIJACK`**. No se asigna automáticamente la Central como owner nuevo:
su ausencia en el gazetteer queda clasificada por separado como
`MISSING_SPECIFIC_IDENTITY`.

## 4. Wrong owners nuevos confirmados

Un caso sólo entra en esta tabla cuando concurren cuatro pruebas:

1. el esperado es un comprador físico SECOP distinto por NIT/código;
2. `QueryRouter` LIVE entrega el dueño actual del alias;
3. el scan LIVE declara como match un alias de `FREED_BY_D6`;
4. OLD no entregaba ese mismo dueño.

| Alias liberado | Dueño actual devuelto | Compradores distintos capturados | Ejemplo SECOP |
|---|---|---:|---|
| `candelaria valle` | ALCALDÍA MUNICIPAL DE CANDELARIA VALLE | 1 | INSTITUTO MUNICIPAL DEL DEPORTE Y LA RECREACIÓN… · NIT 81500395 |
| `ciudad bolivar` | ALCALDIA LOCAL DE CIUDAD BOLIVAR // | 4 | COLEGIO CEDID CIUDAD BOLIVAR IED · NIT 830000758 |
| `corporacion quindio` | CORPORACION AUTONOMA REGIONAL DEL QUINDIO | 1 | CORPORACION QUINDIO CONVENTION BUREAU · NIT 901068969 |
| `francisco jose de caldas` | UNIVERSIDAD DISTRITAL FRANCISCO JOSE DE CALDAS | 1 | CENTRO EDUCATIVO FRANCISCO JOSE DE CALDAS · NIT 901004821 |
| `puerto boyaca` | ALCALDIA MUNICIPAL DE PUERTO BOYACA | 6 | CONCEJO MUNICIPAL DE PUERTO BOYACA · NIT 820000370 |
| `salud arauca` | UNIDAD ADMINISTRATIVA ESPECIAL DE SALUD DE ARAUCA | 1 | UNIDAD PRESTADORA DE SALUD ARAUCA · NIT 9008012094 |
| `salud narino` | INSTITUTO DEPARTAMENTAL DE SALUD DE NARIÑO | 1 | UNIDAD PRESTADORA DE SALUD NARIÑO · NIT 900807338 |
| `santa marta magdalena` | ALCALDÍA DISTRITAL DE SANTA MARTA | 1 | CENTRAL DE TRANSPORTE DE SANTA MARTA LTDA. · NIT 891701694 |
| `soledad atlantico` | MUNICIPIO DE SOLEDAD | 1 | I.E. POLICARPA SALAVARRIETA SOLEDAD ATLÁNTICO · NIT 802003453 |

El JSON conserva los 17 compradores, códigos, NIT, estado activo, ubicación,
query exacta, alias que mordió y los resultados OLD/LIVE. La tabla resume por
alias para no inflar nueve causas con diecisiete filas equivalentes.

## 5. Muestra adversarial por QueryRouter

Se ejecutaron los compradores activos que contienen cada alias `HIGH`, uno por
alias `MEDIUM` con cruce físico y el canario Terminal. Cada query se corrió dos
veces sobre el mismo resolver: LIVE B′ y OLD.

| Resultado LIVE frente al comprador SECOP esperado | Casos |
|---|---:|
| `CORRECT_OWNER` | 14 |
| `WRONG_OWNER` | 36 |
| `FAIL_CLOSED` | 2 |
| **Total** | **52** |

De los 36 `WRONG_OWNER`, **17** son nuevos y satisfacen el gate causal de la
sección anterior. Los otros 19 ya devolvían el mismo dueño —o algún dueño
ajeno— bajo OLD, por lo que son deuda real de resolución pero no evidencia
contra R2D.1. Separarlos evita atribuir a D6 fallos preexistentes.

## 6. Revalidación de canarios no-CAR

| Canario | Gazetteer owner | Verified SECOP owner | Resultado |
|---|---|---|---|
| Secretaría General Alcaldía Mayor Bogotá | SECRETARÍA GENERAL… | mismo · NIT 899999061 | `VERIFIED_SECOP_OWNER` |
| EAAB por nombre completo | EMPRESA DE ACUEDUCTO… | mismo · NIT 899999094 | `VERIFIED_SECOP_OWNER` |
| ICCU por nombre completo | INSTITUTO DE CAMINOS… | mismo · NIT 900258711 | `VERIFIED_SECOP_OWNER` |
| Canal Teleantioquia | SOCIEDAD TELEVISION DE ANTIOQUIA LTDA | mismo · NIT 890937233 | `VERIFIED_SECOP_OWNER` |
| Terminal Santa Marta | ALCALDÍA DISTRITAL DE SANTA MARTA | CENTRAL DE TRANSPORTE… · NIT 891701694 | **`ALIAS_HIJACK`** |
| ESE departamento del Meta | EMPRESA SOCIAL DEL ESTADO… | mismo · NIT 822006595 | scan correcto; router `FAIL_CLOSED` por guard genérica |

En la ESE del Meta no se llama “correcto” al mero gazetteer: el NIT físico
confirma el owner. La ruta del producto no lo entrega porque una guardia
posterior lo cierra; ese comportamiento es deuda separada, no wrong owner de
R2D.1.

## 7. Causas, separadas antes de remediar

- `OVER_GENERIC_ALIAS`: aliases territoriales (`puerto boyaca`, `ciudad
  bolivar`) y fragmentos funcionales (`salud arauca`) no distinguen al owner de
  otros compradores del mismo lugar o materia.
- `SCAN_SUBSTRING_POLICY`: un alias que podría ser tolerable como query exacta
  adquiere autoridad dentro del nombre más largo de otra institución.
- `MISSING_SPECIFIC_IDENTITY`: sólo se añade al canario Terminal; explica por
  qué no existe un match más largo que gane, pero no vuelve segura la frase
  territorial.

No se mete todo en `BAD_ALIAS_OWNERSHIP`: ownership exacto, elegibilidad para
scan y cobertura específica son tres decisiones distintas. R2D.2 no prescribe
todavía cuál de ellas debe cambiar.

## 8. Validación y mutaciones

```text
ruff scripts/audits/audit_freed_alias_safety.py     PASS
reproductor R2D.2                                   PASS
suite completa                                      NO EJECUTADA
runtime                                             SIN CAMBIOS
aliases_db.json                                     SIN CAMBIOS
push                                                NO
```

No se ejecutó la suite completa porque el lote sólo añade auditoría y
documentación. El reproductor vuelve a leer la regla viva, el artefacto R2D.1,
el gazetteer y fuentes SECOP; aborta si `FREED_BY_D6` deja de ser 2.104.

---

## 9. Post-remediación (R2D.3)

La evidencia PRE de las secciones 1–8 no se modifica. Se registra aquí lo que
cambió después, en `debf2e3`.

`scripts/audits/audit_freed_alias_safety.py` dejó de asumir que todo alias
liberado por D6 sigue vivo en `aliases`. Ahora reconstruye el universo sumando
los aliases activos y los retirados que quedaron declarados en
`resolution_blocked_aliases`, y la invariante de 2.104 se verifica sobre esa
suma. Sin ese cambio el reproductor abortaba después de remediar: la
remediación misma habría destruido la posibilidad de reverificarla.

```text
FREED_BY_D6 activos                        2.098
FREED_BY_D6 retirados por política             6
TOTAL                                      2.104   ✓ (invariante intacta)

NEW_WRONG_OWNER_CONFIRMED   17 → 0
SANTA_MARTA_TERMINAL        ALIAS_HIJACK → REMEDIATED_SPECIFIC_IDENTITY
R2D1_VERDICT                REMEDIATION_REQUIRED → PASS_CONFIRMED
```

El artefacto histórico `freed_alias_safety_r2d2.json` se conserva sin
reescribir; la corrida POST vive en
[`freed_alias_safety_r2d2_post.json`](../../artifacts/audits/freed_alias_safety_r2d2_post.json).

Las tres causas de la sección 7 quedaron adjudicadas en R2D.3
([`SCAN_ALIAS_ELIGIBILITY_R2D3.md`](SCAN_ALIAS_ELIGIBILITY_R2D3.md), ADR-072):
`SCAN_SUBSTRING_POLICY` con `scan_disabled_aliases` (3 aliases),
`OVER_GENERIC_ALIAS` con `resolution_blocked_aliases` (6 aliases), y
`MISSING_SPECIFIC_IDENTITY` con la identidad verificada de CENTRAL DE
TRANSPORTE DE SANTA MARTA LTDA.
