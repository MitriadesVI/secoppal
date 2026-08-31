# ADR-071 — Acceso a SECOP como autoridad separada de visibilidad y etapa

**Fecha:** 2026-08-31
**Estado:** Accepted — `SECOP_ACCESS_V2_CLOSED`
**Integración:**

```text
feature commit:   fccbe4ddcfdfee2231736f6ac73e30ed7563b5f6
parity closeout:  9a5cceea759bf882c19bfeeb968c0a568146f579
```

**Evidencia:** `docs/audits/2026-08-31_CO1_REQ_PUBLIC_LINK_SPIKE.md`
(crudo en `docs/audits/evidence/2026-08-31_CO1_REQ_PUBLIC_LINK_SPIKE/`).
**Supersede parcialmente:** ADR-027 (su causa, no su decisión).

---

## 1. Contexto

Desde DOGFOOD-URL-005 (2026-05-25) y ADR-027 (2026-06-10) el sistema operaba
sobre una premisa heredada nunca medida:

```text
CO1.REQ.*
→ requisición / prepublicación
→ ausencia esperable de vista pública
```

De ahí salían dos comportamientos: un guard que impedía construir la URL desde
un id `CO1.REQ.*`, y una nota de UI que **afirmaba una etapa** —"Vista pública
no disponible (proceso pre-publicación)"— en los tres canales.

El censo completo de `p6dx-8zbt` al 2026-08-31 falsifica la premisa:

| Hecho | Filas | % |
|---|---:|---:|
| Total del dataset | 9.096.877 | 100% |
| `id_del_proceso` que empieza por `CO1.REQ.` | 9.096.877 | **100%** |
| `urlproceso` con `OpportunityDetail` + `noticeUID=CO1.NTC.*` | 9.002.823 | **98,97%** |
| `urlproceso` con `noticeUID=CO1.REQ.*` | **0** | **0%** |

El prefijo lo llevan **todas** las filas y el 98,97% de ellas **sí** tiene vista
pública: `CO1.REQ` no tiene ningún poder predictivo sobre la publicación. Un
predictor real existe y es otro —el estado del procedimiento—:

```text
estado_del_procedimiento = Borrador
    42.580 / 43.304  →  98,3% sin vista pública

Contratación régimen especial   (la causa que ADR-027 nombraba)
    34.163 / 3.563.571  →  0,96%
tasa base del dataset
    1,03%
```

La atribución a ESAL / régimen especial es indistinguible de la tasa base.

Lo único que el censo **sí** demuestra a favor del guard es distinto y más
fuerte: **cero** filas usan su propio id como `noticeUID`, así que construir la
URL desde `id_del_proceso` produciría un enlace roto en el 100% de los casos.
El comportamiento se conserva; el motivo escrito se corrige.

## 2. Decisión

### 2.1 Autoridad de acceso

`app/core/secop_access.py` es la única autoridad sobre **cómo llegar a la fuente
oficial**. Responde una sola pregunta:

> ¿Qué vía segura puede ofrecer SECOPPAL para llegar a la fuente oficial de este
> proceso?

**No** responde, y no debe empezar a responder:

```text
¿está publicado?
¿está abierto?
¿puede ofertar?
¿es prepublicación?
¿qué etapa tiene?
```

Esas preguntas pertenecen a otras autoridades. Contestarlas aquí crearía un
segundo árbitro del ciclo de vida, más silencioso que los existentes y sin su
evidencia.

### 2.2 Separación de responsabilidades

```text
SECOP Access            → navegación a la fuente
Truth Layer visibility  → evidencia sobre visibilidad
Opportunity Stage       → etapa del proceso
Operational State       → lectura operativa
Radar                   → detección / monitoreo
```

**Ninguna capa infiere en silencio la respuesta de otra.** La ausencia de un
enlace no es un dato de etapa, y un dato de etapa no decide la vía de acceso.

### 2.3 Enlace directo

Un enlace directo **debe provenir de una URL aceptada por `secop_access`**: se
observa en lo que SECOP entregó, nunca se construye desde un id ni desde una
referencia. Para ser aceptada debe:

```text
usar esquema http/https
tener host de la allow-list de SECOP
representar una VISTA del proceso, no una puerta de entrada
```

Se rechazan como ficha:

```text
/login                      (pantalla de autenticación, 94.054 filas)
/ContractNoticeManagement   (buscador público de avisos)
el buscador público
hosts externos
esquemas inseguros
URL malformada
```

El buscador se reconoce por su **ruta**, no por el parámetro `Page=login`:
depender del parámetro dejaría pasar la misma pantalla sin él o con otra
capitalización, y seguiría sin haber ningún proceso que mostrar.

### 2.4 Fallback

Cuando no existe ficha directa, la respuesta es una acción, no un diagnóstico:

```text
Buscar en SECOP II  +  referencia observada (si existe)
```

El destino canónico es `SECOP_PUBLIC_SEARCH_URL`, y **no es una ruta inventada**:
es exactamente el literal que el propio SECOP publica en `urlproceso` para los
4.130 contratos de `jbjy-vk9h` sin ficha pública. Es decir, el destino del
fallback es el que la fuente declara para "este registro no tiene ficha".
`Page=login` forma parte de ese literal oficial y **no** significa que la página
exija sesión.

Las tres clases resultantes:

```text
direct                  SECOP entregó una vista pública del proceso
search_with_reference   sin vista propia, pero con referencia observada
search                  sin ninguna de las dos; queda la entrada pública
```

### 2.5 No ocultar resultados

La falta de enlace directo:

```text
NO elimina una oportunidad
NO reduce su ranking
NO implica prepublicación
NO implica borrador
```

El usuario conserva **siempre** una vía hacia SECOP. Este es el principio de
ADR-027 —señalizar, no omitir— conservado y reforzado: lo señalizado dejó de ser
una nota que nombraba una etapa y pasó a ser una acción ejecutable.

### 2.6 Guardar sigue siendo local

El Workspace **no hace lookup remoto al guardar**: copia únicamente el snapshot
observado. Los ítems legacy sin `secop_url` pueden usar su `reference` para
ofrecer "Buscar en SECOP II", sin migración ni reescritura del histórico.

## 3. Resolución CO1.REQ → CO1.NTC

```text
CO1_REQ_DIRECT_RESOLUTION = NO_GO
```

Sobre una muestra congelada y reproducible (31 filas, 10 estratos, `$order` y
`$limit` fijos), las 18 filas sin vista pública se intentaron resolver por dos
vías oficiales:

| Vía | Resueltas | Ambiguas | No resolubles |
|---|---:|---:|---:|
| `referencia_del_proceso` + `nit_entidad` | **0 / 18** | 0 | 18 |
| `id_del_portafolio` | **0 / 18** | 0 | 18 |

En los 18 casos la consulta devolvió exactamente 1 fila: la propia. No existe
fila hermana publicada a la que apuntar. Además `id_del_portafolio` **no es una
clave determinista**: 2 de las 13 filas con vista comparten portafolio con 3 URLs
públicas distintas, de modo que aun existiendo hermana la vía sería ambigua y
tendría que fallar cerrado igual.

No se halló ninguna columna oficial que relacione `CO1.REQ` con `CO1.NTC`. La
única portadora del `CO1.NTC` es la propia `urlproceso`: si está, ya hay enlace
directo; si no está, no hay de dónde derivarlo.

Por tanto:

```text
NO fabricar REQ → NTC
NO resolver mediante terceros
NO adivinar noticeUID
```

**Oráculo HTTP: NO DISPONIBLE.** `community.secop.gov.co` responde 403 a cliente
no-navegador y 302 hacia reCAPTCHA con User-Agent de navegador. No se resolvieron
CAPTCHAs. El veredicto descansa íntegramente en la evidencia Socrata, que es
suficiente: 0/18 resolubles.

## 4. Deudas aceptadas

### 4.1 `TRUTH_LAYER_SEARCH_URL_CLASSIFICATION_DEBT`

**Estado:** `DEFERRED`. **Impacto en SECOP Access UI:** `NONE`.

El `_classify_url` de `truth_layer/visibility_resolver.py` solo busca `/login` en
el *path*, así que clasifica el buscador público de avisos
(`/Public/Tendering/ContractNoticeManagement/Index?...&Page=login&...`) como
`public`, y la fila resuelve a `public_view_available` — una "vista pública" que
no muestra ningún contrato. Es semánticamente incorrecto: esa página es el
buscador, no una ficha.

No se corrige aquí, deliberadamente: el contrato semántico del Truth Layer está
congelado por sus documentos vinculantes, y corregirlo mueve **un veredicto de
evidencia**, no una etiqueta de UI. Merece su propia adjudicación.

El impacto en el runtime de SECOP Access V2 es ninguno porque ninguna superficie
consulta el Truth Layer para decidir la vía a SECOP: las cuatro superficies web y
los tres canales de texto deciden con `app/core/secop_access.py`, que rechaza esa
página por su ruta. Regresiones que lo fijan: `tests/test_secop_access.py`
(`test_the_contract_management_page_never_becomes_a_direct_link`) y
`tests/test_url_fallback_channel_parity.py`
(`test_the_contract_search_page_is_never_presented_as_a_process_view`).

### 4.2 `SECOP_SYNTHETIC_URL_PROVENANCE_DEBT`

**Estado:** registrada, no corregida. Sin regresión P0/P1 conocida hoy.

`Formatter._safe_url` y `SecopClient._normalize_row` todavía pueden **fabricar**
ciertas URLs desde ids y referencias cuando la observada falta o es la pantalla
de autenticación. Después de esa capa, `secop_access` recibe un string sin poder
saber si fue:

```text
observed | normalized | synthetic
```

Eso contradice conceptualmente el contrato de §2.3 —"el enlace directo se
observa, no se construye"— aunque hoy no produzca una regresión conocida: para
`p6dx-8zbt` las ramas constructoras son inalcanzables (el 100% de los ids es
`CO1.REQ.*` y el guard las corta), de modo que la política efectiva de ese
dataset ya es "usa la URL observada o no hay URL".

Arquitectura futura candidata, **no implementada en este commit**:

```text
raw_url · observed_url · normalized_url · synthetic_url · url_provenance
```

## 5. Fuera de alcance

- Corregir `_classify_url` del Truth Layer (§4.1).
- Introducir el eje de provenance de URL (§4.2).
- Cualquier resolución REQ → NTC, propia o de terceros (§3).
- Cualquier inferencia de etapa a partir de la ausencia de enlace.

## 6. Consecuencias

Una oportunidad detectada nunca deja al usuario sin camino a SECOP, y ninguna
superficie afirma una etapa que los datos no sostienen. La decisión de acceso
existe en **un solo lugar**: host, `/login`, ruta de la página, referencia y URL
del buscador dejaron de ser reglas dispersas dentro de `Formatter` y de las
vistas. Las superficies —feed y detalle de Radar, Workspace, tarjetas de
búsqueda, detalle de direct lookup, procesos seguidos, tarjetas agrupadas,
Telegram y WhatsApp— comparten la misma autoridad y, para la misma fila, toman la
misma decisión.
