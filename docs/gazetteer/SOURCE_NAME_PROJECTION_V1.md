# SOURCE-NAME-PROJECTION-V1 — identidad humana y nombre físico son autoridades distintas

**Tipo:** discovery + diseño + spike read-only  
**Fecha:** 2026-09-02 · **Rama:** `main`  
**HEAD antes:** `c447702ff3389d913f25a4816ca574160c2279b1`  
**Evidencia:**
[`source_name_projection_v1.json`](../../artifacts/audits/source_name_projection_v1.json)

## 1. Veredicto

```text
SOURCE_NAME_PROJECTION_V1_DISCOVERY: PASS
DECISION: GO_IMPLEMENTATION

CANONICAL_IDENTITY       != SOURCE_PHYSICAL_NAME
SOURCE_PHYSICAL_NAME     != ALIAS
SOURCE_NAME_SPLIT        != BUYER_GROUP
```

Hay un problema 1:1 demostrado y acotable: el resolver puede conocer al
comprador correcto y aun así proyectar al dataset un string que éste no guarda.
DAPRE lo demuestra sin depender de puntuación; Soacha, Yopal, el Fondo
Financiero Distrital de Salud y UAESP confirman el patrón entre datasets.

La decisión es crear en implementación V1 un **registro tipado e independiente
de nombres físicos por fuente**, persistido en un sidecar propio. No se agregan
campos a `aliases_db.json`, no se derivan bindings en vivo por NIT y no se
introducen Buyer Groups.

## 2. Preflight y alcance

`main` estaba exactamente en el HEAD esperado y el árbol trackeado estaba
limpio. Había archivos no trackeados preexistentes; no se tocaron. Se leyeron
los cuatro componentes del flujo, el gazetteer y los cinco antecedentes
pedidos. Se buscaron globalmente `SOURCE_NAME_SPLIT`,
`CANONICAL_ENTITY_SOURCE_NAMES`, `MULTIPLE_PHYSICAL_SAME_IDENTITY` y
`SAME_LOGICAL_IDENTITY`.

Este lote sólo añade documentación y evidencia. No cambia runtime, SoQL,
aliases, fuzzy, geografía, Radar, Actor ni transporte SODA3.

## 3. Cuatro conceptos, cuatro contratos

| Concepto | Pregunta que responde | Autoridad |
|---|---|---|
| Identidad canónica/humana | ¿Quién es el comprador que entendimos? | resolver + catálogo de identidades |
| Alias de entrada | ¿Qué frase humana puede identificarlo? | ownership del gazetteer |
| Nombre físico de fuente | ¿Qué string exacto guarda este dataset? | registro de proyección verificado |
| Buyer Group | ¿Qué compradores/unidades se agregan bajo una raíz? | frente futuro, no V1 |

Un nombre físico puede ser un alias útil, pero una condición no implica la
otra. Tampoco un NIT común autoriza por sí solo a sumar todas sus unidades.

## 4. DAPRE congelado por superficie

Identidad canónica estable:

```text
DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA
identity_key = NIT:899999083
```

### 4.1 UI pública y directorio API

Se abrió el selector público de entidades de SECOP II y se buscó
`899999083`. La UI devolvió:

| NIT publicado | Nombre visible |
|---|---|
| `899999083` | `DAPRE` |
| `8999990839` | `PRESIDENCIA DE LA REPÚBLICA` |
| `8999990830` | `FONDO DE PROGRAMAS ESPECIALES PARA LA PAZ DEL DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA – FONDO PAZ` |

La API oficial `4ex9-j3n8` devolvió las mismas tres identidades registrales,
pero no los mismos strings en dos filas: publica `REPuBLICA` donde la UI
renderiza `REPÚBLICA`, y un carácter `U+0096` donde la UI muestra el guion
tipográfico. La fila principal `899999083 / DAPRE` sí coincide exactamente.
Por tanto:

```text
OFFICIAL_SURFACE_NAME_SPLIT: CONFIRMED
```

No se declara una superficie correcta y otra stale: se congela la discrepancia.
Otras superficies oficiales —por ejemplo órdenes de compra— pueden mostrar la
razón social larga, pero no son el `SECOP_UI_REGISTRY_DISPLAY` medido aquí.

### 4.2 Procesos y contratos

| Fuente | NIT publicado | Nombre físico | Filas |
|---|---|---|---:|
| `p6dx-8zbt` | `899999083` | `DAPRE` | **17.128** |
| `jbjy-vk9h` | `899999083` | `DAPRE` | **4.351** |

El nombre canónico largo devuelve **0 / 0**. El nombre físico `DAPRE` devuelve
exactamente los mismos conteos que el NIT.

La clasificación precisa tiene dos ejes:

```text
entre procesos y contratos: SINGLE_SOURCE_NAME
canónico frente a fuente:    SOURCE_NAME_SPLIT
UI frente a 4ex9:            OFFICIAL_SURFACE_NAME_SPLIT
```

Ninguna forma se declara “correcta” ni “stale”. La canónica sirve para
identidad/display; `DAPRE`, para proyección transaccional; las tres filas del
directorio son evidencia registral con identidad que exige cautela.

## 5. Spike read-only

El spike compuso las piezas existentes sin modificar producción:

```text
resolve_entidad("Presidencia de la República")
  → DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA
  → fuzzy(90), medium

CURRENT
  process WHERE entidad LIKE '%DEPARTAMENTO ADMINISTRATIVO ...%'
  contract WHERE nombre_entidad LIKE '%DEPARTAMENTO ADMINISTRATIVO ...%'
  → 0 procesos / 0 contratos

SOURCE-AWARE
  process WHERE entidad = 'DAPRE'
  contract WHERE nombre_entidad = 'DAPRE'
  → 17.128 procesos / 4.351 contratos
  → iguales a los conteos por NIT 899999083
```

Hay una segunda brecha que no debe camuflarse: hoy `QueryRouter` reconoce
`DAPRE` por `scan_exact`, pero la frase completa “Presidencia de la República”
queda como `objeto` si entra directamente al parser. El resolver aislado sí la
reconoce. Es un problema de admisión/routing anterior a la proyección, no una
razón para mezclar aliases con source names. Discovery no lo modifica ni
autoriza expansión de aliases; implementación V1 debe fijarlo como canario o
declarar explícitamente la precondición de identidad ya resuelta.

## 6. Más casos 1:1 que siguen vigentes

Conteos reconsultados el 2026-09-02:

| Caso | Identity key | Procesos | Contratos | P/C | Clasificación | V1 |
|---|---|---|---|---:|---|---|
| Soacha | `NIT:800094755` | `MUNICIPIO DE SOACHA..` | `MUNICIPIO DE SOACHA.` | 22.108 / 21.752 | `SOURCE_NAME_SPLIT` | sí |
| Yopal | `NIT:891855017` | `MUNICIPIO DE YOPAL...` | `MUNICIPIO DE YOPAL..` | 17.548 / 19.039 | `SOURCE_NAME_SPLIT` | sí |
| Fondo Financiero Distrital de Salud | `NIT:800246953` | `…SALUD..` | `…SALUD.` | 23.735 / 21.256 | `SOURCE_NAME_SPLIT` | sí |
| UAESP Bogotá | `NIT:900126860` | `…SERVICIOS PÚBLICOS..` | `…SERVICIOS PÚBLICOS.` | 6.186 / 5.851 | `SOURCE_NAME_SPLIT` | sí |

Son una identidad física por caso, con un nombre por dataset. No hay hijos que
sumar. El directorio muestra ruido y nombres adicionales, pero los dos datasets
transaccionales convergen por NIT y volumen.

El `LIKE` actual oculta parte de la deuda. Cuando el resolver elige la variante
con un punto, esa cadena es substring de la variante con dos puntos y ambas
fuentes devuelven filas por accidente. Yopal expone el sentido contrario: tres
puntos no caben dentro de dos y contratos queda en cero. La corrección no debe
depender de qué variante sea prefijo de la otra.

### MinJusticia: no contar evidencia caducada como confirmación

R1C documentó `MINJUSTICIA..` / `MINJUSTICIA.`. En el corte vivo, el NIT
`900457461` publica `MINISTERIO DE JUSTICIA Y DEL DERECHO..` en procesos y la
variante de un punto en contratos; las formas cortas no aparecieron. El caso
sigue siendo deuda histórica del gazetteer, pero queda `REVIEW` y fuera de los
bindings V1 hasta explicar la deriva.

## 7. Casos negativos: parecidos en forma, distintos en semántica

| Caso | Evidencia viva | Clasificación | Por qué no entra |
|---|---|---|---|
| CGR | NIT `899999067`, 32 nombres y 8.280/7.561 filas | `PHYSICAL_UNITS_UNDER_COMMON_IDENTITY` + `BUYER_GROUP_CANDIDATE` | nivel central, unidad de regalías y 30 gerencias son unidades organizacionales, no grafías |
| Cundinamarca | 28 nombres, 45.675/42.642 filas, sin raíz exacta | `BUYER_GROUP_CANDIDATE` | contratación distribuida entre hijos |
| Valle | 29 nombres y 79.749/86.877 filas sobre tres formas de NIT, sin raíz exacta | `BUYER_GROUP_CANDIDATE` | contratación distribuida entre hijos |
| Fondo CGR vs CGR | NIT `800252683` vs `899999067` | compradores separados | el nombre del Fondo contiene al padre, pero la identidad no es la misma |

La deuda `CGR_PHYSICAL_NAME_FRAGMENTATION_DEBT` permanece abierta. El corte
transaccional actual devuelve 32, no 33, nombres para el NIT base; la diferencia
con R2E es drift medido, no licencia para reclasificar el caso.

## 8. Dónde se confunde hoy identidad con fuente

```text
EntityResolver
  ResolutionResult.value / ScanResult.official_name
          │
          ▼
QueryRouter
  params["entidad_resolved"] = official_name
          │
          ▼
QueryFrame / resolved_params
  conserva el mismo string, sin identity_key ni dataset binding
          │
          ▼
SoQLBuilder._build_where
  entidad_value = entidad_like or entidad_resolved
  LIKE '%' + entidad_value + '%'
```

Puntos exactos:

- `EntityResolver._load_aliases` carga `record["official_name"]` como valor de
  resolución y choice físico a la vez.
- `QueryRouter` copia `match.value` o `entity_scan.official_name` a
  `entidad_resolved`.
- `QueryFrame._SCOPE_KEYS` sólo conserva el string y su metadata de resolución.
- `resolve_entities` pasa por alto toda entidad que ya venga resuelta.
- `SoQLBuilder._build_where` consume directamente ese mismo string contra
  `entidad` o `nombre_entidad`.

El supuesto no está en un único comentario: es la ausencia de una frontera
entre `entidad_resolved` y el valor ejecutable por dataset.

## 9. API mínima futura

La API debe ser plural desde el inicio:

```python
project_buyer_names(
    canonical_identity: str,
    dataset: Literal["processes", "contracts"],
) -> tuple[str, ...] | None
```

- `tuple` con 1..N nombres: existe binding verificado; construir igualdad
  exacta por nombre unida con `OR`.
- `None`: no existe binding; conservar íntegro el camino actual.
- tupla vacía: schema inválido; fallar al cargar, no convertirla en consulta
  global.

La implementación puede internamente indexar por `identity_key`, pero el primer
adaptador puede recibir el nombre canónico porque ése es el contrato que hoy
expone el resolver. El registro valida correspondencia única entre ambos.

```text
entidad = 'NOMBRE 1' OR entidad = 'NOMBRE 2'
```

No `LIKE` inventado, no fuzzy en Socrata, no N nombres descubiertos en vivo.

## 10. Uno o varios nombres

V1 admite 1..N porque el dominio ya demuestra que N es posible. Eso no
autoriza a llenar N por parecido, NIT compartido o prefijo común.

Para cada nombre adicional se exige:

1. misma identidad física demostrada;
2. dataset y nombre exacto;
3. evidencia fechada;
4. prueba de que no es unidad hija ni Buyer Group;
5. límite de cardinalidad declarado por el registro.

DAPRE comienza con 1/1. CGR 32/32 no puede pasar esas puertas y queda fuera.

## 11. NIT: buen ancla, mala autoridad global

Ambos datasets publican `nit_entidad`. En una muestra descriptiva de 50.000
filas por dataset:

| Fuente | Presente | Con puntuación | Longitudes observadas |
|---|---:|---:|---|
| procesos | 50.000/50.000 | 16 | 7–11 |
| contratos | 50.000/50.000 | 0 | 8–10 |

En procesos aparecieron `814005267-5` y `900.479.658`. La muestra no es una
estimación poblacional: el agregado completo agotó el timeout de Socrata. Sí
demuestra que “campo presente” no equivale a “formato canónico uniforme”.

Casos concretos:

- DAPRE: `899999083` funciona en ambas fuentes.
- IEMP: publica `8300157281`; el base `830015728` devuelve cero.
- Fondo Distrital de Salud: publica `8002469532`.
- Valle: publica `890399029`, `8903990291` y `8903990295`.
- El propio directorio DAPRE asocia sufijos `9` y `0` con otros nombres; quitar
  el último dígito por longitud no es autoridad de identidad.

### Decisión

`FILTER_BY_SOURCE_NAME` exacto es el default V1. El NIT permanece como
`identity_key`, evidencia de reconciliación y posible estrategia futura por
binding. `FILTER_BY_NIT` sólo podrá activarse para un comprador después de
verificar sus formas por dataset y probar que no agrega unidades físicas.

## 12. Alternativas

| Opción | Simplicidad | Trazabilidad | Compatibilidad | Drift | Testabilidad | Veredicto |
|---|---|---|---|---|---|---|
| A. Campos en `aliases_db.json` | media | baja | media | alto | media | rechazar |
| B. Sidecar JSON sin tipo | alta | alta | alta | medio | media | aceptable, no preferida |
| C. Registro tipado independiente | media | alta | alta | bajo | alta | **recomendada** |
| D. Derivar en vivo por NIT | baja | baja | media | alto | baja | rechazar como autoridad runtime |

### A. `aliases_db.json`

Parece barato, pero revierte el principio del lote: vuelve a poner alias de
entrada, display canónico y proyección física en el mismo tipo de registro. El
archivo ya contiene fichas duplicadas por variante de dataset; extenderlas
haría la migración posterior más costosa.

### B. Sidecar `buyer_source_names.json`

Separa bien los datos y no rompe carga actual. Sin un loader tipado no impide
dataset desconocido, lista vacía, clave duplicada, nombre repetido o evidencia
ausente.

### C. Registro tipado independiente

Recomendado. Puede persistir en un sidecar JSON dedicado, pero su contrato es
un tipo y un validador, no “otro diccionario”. Índices:

```text
identity_key   -> BuyerSourceNameRecord
canonical_name -> identity_key
```

Campos mínimos: `identity_key`, `canonical_name`, nombres 1..N por dataset,
`verified_at` y `evidence`. Carga una vez, coste despreciable frente a red y
tests puros sin Socrata.

### D. Derivación dinámica por NIT

Útil para auditoría, peligrosa en ejecución: importa latencia, caída de red,
drift y la decisión Buyer Group a cada consulta. No es reproducible y hace que
la misma pregunta cambie de alcance sin despliegue ni revisión.

## 13. Preview de universidades

| Universidad | NIT base | Procesos | Contratos | Clasificación | ¿V1 ayuda? |
|---|---|---|---|---|---|
| Nacional | `899999063` | `UNIVERSIDAD NACIONAL DE COLOMBIA` | igual | `SINGLE_SOURCE_NAME` | no demostrado |
| Antioquia | `890980040` | `UNIVERSIDAD DE ANTIOQUIA` | igual | `SINGLE_SOURCE_NAME` | no demostrado |
| Valle | `890399010` | `UNIVERSIDAD DEL VALLE` | igual | `SINGLE_SOURCE_NAME` | no demostrado |
| UPTC | `891800330` | `UPTC` | `UPTC` | canónico/registro largo frente a fuente corta | **sí, DAPRE-shaped** |
| Distrital | `899999230` | `UNIVERSIDAD DISTRITAL FRANCISCO JOSE DE CALDAS` | igual | `SINGLE_SOURCE_NAME` | no demostrado |

La muestra no justifica migrar universidades. Sólo UPTC valida externamente que
R2F puede encontrar identidades cuyo nombre humano largo y nombre físico corto
difieren aunque procesos y contratos coincidan entre sí.

## 14. Alcance de implementación V1

`GO_IMPLEMENTATION`, limitado a:

1. registro tipado independiente + sidecar validado;
2. DAPRE como único binding runtime inicial;
3. proyección exacta por dataset en procesos y contratos;
4. fallback íntegro al comportamiento actual cuando no hay binding;
5. tests de display estable, 1..N, OR exacto, schema fail-closed y ambos
   datasets;
6. canario separado para la brecha de `QueryRouter` con “Presidencia de la
   República”, sin resolverla mediante alias expansion en este discovery.

No incluye Soacha/Yopal/FFDS/UAESP como bindings iniciales: son evidencia de
generalidad y fixtures de diseño. Migrarlos requiere un lote posterior con
pruebas de equivalencia contra el `LIKE` actual.

## 15. Fuera de alcance preservado

- Buyer Groups y raíces Cundinamarca/Valle;
- las unidades CGR;
- cobertura universitaria completa;
- expansión de aliases o fuzzy;
- geografía, Radar, Actor y cambios de transporte SODA3.

## 16. Validación

```text
jq empty artifacts/audits/source_name_projection_v1.json    PASS
ruff check (sin Python nuevo)                               N/A
make lint-core                                               PASS
git diff --check                                             PASS
```

El discovery no ejecuta la suite completa porque no cambia runtime. Los
conteos de Socrata y la UI se congelan en el artefacto con fecha, dataset,
campo, NIT publicado y límite de muestra cuando aplica.
