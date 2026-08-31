# Spike CO1.REQ → enlace público SECOP II

**Fecha:** 2026-08-31
**Rama:** `fix/radar-secop-access-v2` (base `23a2ed3`)
**Fuentes (únicas autorizadas):** `datos.gov.co` / Socrata (`p6dx-8zbt`,
`jbjy-vk9h`) y `community.secop.gov.co`. **No** se usó BuscaSECOP ni ningún
tercero como autoridad ni como resolver.
**Evidencia cruda:** `docs/audits/evidence/2026-08-31_CO1_REQ_PUBLIC_LINK_SPIKE/`
(`sample.py`, `sample.json`, `resolve.py`, `resolved.json`).

---

## 1. Veredicto

```
UX_FALLBACK                 = GO
CO1_REQ_DIRECT_RESOLUTION   = NO_GO
```

Son separables y se separan: el fallback de búsqueda pública se aprueba aunque
la resolución directa de REQ no sea viable.

---

## 2. La premisa heredada es falsa

ADR-027 y los comentarios de `secop_client._normalize_row` /
`formatter._safe_url` afirmaban:

> `CO1.REQ.*` = requisiciones pre-publicación / ESAL → no hay link consultable.

Censo completo de `p6dx-8zbt` al 2026-08-31:

| Hecho | Filas | % |
|---|---:|---:|
| Total del dataset | 9.096.877 | 100% |
| `id_del_proceso` que empieza por `CO1.REQ.` | 9.096.877 | **100%** |
| `id_del_proceso` que empieza por `CO1.NTC.` | 0 | 0% |
| `urlproceso.url` nula | 0 | 0% |
| `urlproceso.url` con `OpportunityDetail` + `noticeUID=CO1.NTC.*` | 9.002.823 | **98,97%** |
| `urlproceso.url` = pantalla de autenticación (`/STS/Users/Login/Index`) | 94.054 | 1,03% |
| `urlproceso.url` con `noticeUID=CO1.REQ.*` | **0** | **0%** |

Tres consecuencias:

1. **El prefijo `CO1.REQ` no tiene ningún poder predictivo sobre el enlace**:
   lo llevan TODAS las filas, y el 98,97% de ellas SÍ tiene vista pública.
   Coincide con el Truth Layer vigente (`visibility_resolver`), que exige
   evidencia conjunta para `prepublication_supported` y nunca el id solo.
2. **El predictor real es otro**. `estado_del_procedimiento='Borrador'`:
   42.580 de 43.304 filas (98,3%) traen la pantalla de autenticación. En
   contraste, `Contratación régimen especial` —la causa que ADR-027 nombraba—
   tiene 34.163 de 3.563.571 (0,96%), es decir la tasa base.
3. **El guard del código igual debe conservarse**, por un motivo distinto y
   demostrado: cero filas usan su propio id como `noticeUID`, así que construir
   la URL desde `id_del_proceso` produciría un enlace roto en el 100% de los
   casos. Se conserva el comportamiento y se corrige el motivo escrito.

Nota sobre el efecto real del guard: como el 100% de los ids son `CO1.REQ.*`,
las dos ramas constructoras de `_normalize_row`/`_safe_url` son **inalcanzables**
para `p6dx-8zbt`. La política efectiva del dataset de procesos es "usa la URL
observada o no hay URL".

---

## 3. Muestra congelada (31 filas, ≥20 exigidas)

Determinista y reproducible: cada estrato con `$order=id_del_proceso` y
`$limit` fijo (`sample.py`). Campos registrados por caso: `id_del_proceso`,
`referencia_del_proceso`, `id_del_portafolio`, `estado_del_procedimiento`,
`fase`, `urlproceso` crudo, URL normalizada y clase de URL.

| Estrato | n | clase de URL |
|---|---:|---|
| `Borrador` | 4 | autenticación |
| `En aprobación` | 3 | autenticación |
| `Aprobado` | 3 | autenticación |
| `Cancelado` (sin vista) | 3 | autenticación |
| `Publicado` (sin vista) | 3 | autenticación |
| `Seleccionado` (sin vista) | 2 | autenticación |
| `fase='Presentación de oferta'` | 4 | pública |
| `Publicado` | 3 | pública |
| `Evaluación` | 3 | pública |
| `Cancelado` | 3 | pública |

Las 18 filas sin vista pública tienen `fase = NULL` sin excepción; las 13 con
vista pública tienen `fase` poblada.

---

## 4. Intentos de resolución oficial

Dos vías candidatas, ambas sobre datos oficiales, para las 18 filas sin vista:

| Vía | Resueltas a URL pública | Ambiguas | No resolubles |
|---|---:|---:|---:|
| `referencia_del_proceso` + `nit_entidad` | **0 / 18** | 0 | 18 |
| `id_del_portafolio` (`CO1.BDOS.*`) | **0 / 18** | 0 | 18 |

En los 18 casos la consulta devolvió **exactamente 1 fila: la propia**. No
existe una fila hermana publicada a la que apuntar.

Además, `id_del_portafolio` **no es una clave determinista**: 2 de las 13 filas
con vista pública comparten portafolio con 3 URLs públicas distintas. Aunque
existiera una fila hermana, la vía del portafolio sería ambigua y por §8
tendría que fallar cerrado igual.

**No se encontró ninguna columna oficial que relacione `CO1.REQ` con
`CO1.NTC`.** La única portadora del `CO1.NTC` es la propia `urlproceso`: si
está, ya hay enlace directo; si no está, no hay de dónde derivarlo.

### Oráculo HTTP: NO DISPONIBLE

Se intentó verificar contra `community.secop.gov.co`. El host responde `403` a
cliente no-navegador y `302` hacia `/Public/Common/GoogleReCaptcha/Index` con
User-Agent de navegador. **No se resolvieron CAPTCHAs** (prohibido), así que el
sondeo HTTP no sirve como oráculo — ni a favor ni en contra. El veredicto
descansa íntegramente en la evidencia Socrata, que es la autoridad que §7 nombra
primero y que es suficiente: 0/18 resolubles.

---

## 5. El destino del fallback no se inventó

`jbjy-vk9h` (contratos, 5.981.689 filas) tiene 4.130 filas sin vista pública
(0,07%; cero pantallas de autenticación). En **todas** ellas SECOP publica
literalmente esta URL en `urlproceso`:

```
https://community.secop.gov.co/Public/Tendering/ContractNoticeManagement/Index?currentLanguage=es-CO&Page=login&Country=CO&SkinName=CCE
```

Es exactamente el buscador público que §5 exige como fallback. Es decir: el
destino del fallback es el que el propio SECOP declara para "este registro no
tiene ficha pública". `SECOP_PUBLIC_SEARCH_URL` copia ese literal.

Corolario: `Page=login` forma parte del literal oficial y **no** significa que la
página exija sesión.

Cobertura de host, ambos datasets: **0 filas** con `urlproceso` fuera de
`community.secop.gov.co`. La allow-list de hosts no rechaza nada observado.

---

## 6. Hallazgo colateral (no corregido en runtime)

Esas 4.130 filas de contratos pasan hoy el filtro `"Login" not in url` de
`_safe_url` —el literal oficial trae `Page=login` en minúscula— y también el
`_classify_url` del Truth Layer, que solo mira `/login` en el *path*. Resultado:
Telegram y WhatsApp las rotulan `🔗 Ver en SECOP` apuntando al buscador, como si
fuera la ficha del contrato.

En Streamlit ya no ocurre: `resolve_secop_access` reconoce la página de búsqueda
y la ofrece como "Buscar en SECOP II". Los canales quedan fuera de alcance en
este bloque (§14 y "no refactor masivo") y se registran como P2. El
`visibility_resolver` no se tocó: su contrato está congelado.
