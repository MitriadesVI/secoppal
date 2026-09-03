# BUYER PUNCTUATION SURFACE P0

**Tipo:** discovery read-only

**Fecha de observación:** 2026-09-02

**HEAD auditado:** `0c4df013cbf52a71adfcea8b22e7f642e21dace2`

**Resultado:** `PASS`

**Decisión:** `KEEP_EXPLICIT_ALIASES`

`PASS` significa que el censo terminó y que los cuatro controles R2F-B se
reprodujeron. No significa que la superficie esté sana: se encontraron 210
grafías físicas puntuadas que no entran por `exact`, pertenecientes a 124 NIT
deduplicados.

## Resultado ejecutivo

| Medida | Resultado |
|---|---:|
| Identidades canónicas SECOPPAL revisadas | 603 |
| Identidades con al menos un NIT acreditado | 591 |
| Pares canonical/NIT acreditados antes de deduplicar | 1.398 |
| Superficies físicas acreditadas revisadas | 2.457 |
| Superficies con puntuación | 486 |
| Superficies puntuadas afectadas (no exactas) | 210 |
| NIT afectados, deduplicados | 124 |
| `EXACT_SAME_OWNER` | 276 |
| `FUZZY_SAME_OWNER` | 91 |
| `WRONG_OWNER` | 15 |
| `FAIL_CLOSED` | 104 |

La deuda es material: 43,2% de las superficies puntuadas acreditadas no son
exactas. De las 210 afectadas, 152 sólo se observaron en el directorio y 58
también aparecen en procesos o contratos. El detalle completo, incluida la
procedencia por superficie, está en el artefacto JSON.

## Autoridad de identidad y deduplicación

La auditoría nunca usa fuzzy para decidir quién es quién. La precedencia es:

1. NIT declarado en `aliases_db.json` o en el `identity_key` de
   `buyer_source_names.json`;
2. si no existe NIT declarado, nombre exacto y unívoco del directorio contra
   el canonical o un alias existente;
3. unión de procesos y contratos por el NIT ya acreditado.

La equivalencia NIT base/dígito de verificación sólo se acepta cuando el nombre
exacto y el NIT declarado acreditan la pareja. No se eliminan dígitos de forma
global. Los homónimos que contradicen un NIT declarado se rechazan y quedan
enumerados en `methodology.rejected_registry_homonyms`.

Una superficie se deduplica por `canonical + NIT + string físico`. Para el
censo de identidades afectadas y el Top 50 se deduplica por NIT; si varios
canonicales conocidos comparten NIT, se conservan sus nombres pero la actividad
se cuenta una sola vez.

Doce canonicales no obtuvieron NIT acreditable con estas reglas. No se les
inventó identidad mediante similitud. Los grupos no atribuibles también se
reportan, pero no entran en numeradores ni denominadores de superficies.

## Esto no es Source Name Projection

ADR-073 responde `canonical -> nombres físicos para consultar una fuente`.
Esta auditoría ejecuta la dirección opuesta: toma el string físico publicado,
como lo copiaría un usuario, y llama al resolver sin alterar un carácter para
medir `physical -> canonical`. Un copy-paste no exacto no autoriza por sí solo
un binding de proyección.

ADR-072 se conserva igualmente: ownership exacto y elegibilidad de scan se
miden por separado. Una recomendación `SAFE_EXACT_ONLY` no implica scan.

## Formas de puntuación

La categoría primaria usa precedencia `REPEATED`, `TYPO_OR_MALFORMED`,
`TRAILING`, `INTERNAL`, `OTHER`; el JSON conserva además todas las señales de
cada string. La puntuación no se presupone ruido.

| Categoría primaria | Superficies |
|---|---:|
| `INTERNAL_PUNCTUATION` | 273 |
| `REPEATED_PUNCTUATION` | 72 |
| `TRAILING_PUNCTUATION` | 141 |

## Riesgo y recomendaciones, sin aplicar decisiones

| Recomendación | Superficies | Lectura |
|---|---:|---|
| `SAFE_EXACT_AND_SCAN` | 51 | no se halló owner/token activo rival y la forma aparece en fuente runtime |
| `SAFE_EXACT_ONLY` | 139 | forma registral o evidencia insuficiente para scan |
| `ALREADY_COVERED` | 276 | ya resuelve exacto al mismo owner |
| `NEEDS_REVIEW` | 5 | colisión de tokens/owner que necesita decisión humana |
| `DO_NOT_ADD` | 15 | el string hoy resuelve a otro owner o ya pertenece a otro owner exacto |

Los 15 `WRONG_OWNER` no son todos ruido de directorio. Hay casos activos como
`CENAC PUENTE ARANDA+` (2.967 procesos y 1.454 contratos), y duplicados de
identidad del gazetteer donde el NIT físico de contratos queda asociado a un
canonical puntuado pero el string exacto pertenece a otro canonical, por
ejemplo Soacha, Yopal, FFDS, MinJusticia y UAESP. Deben revisarse como deuda de
ownership/deduplicación, no curarse automáticamente como puntuación.

## Controles R2F-B

| Control | Fuente reproducida | Resolver | Scan | Resultado |
|---|---|---|---|---|
| `UNIVERSIDAD DEL PACIFICO//` | directorio, procesos, contratos | exact, mismo owner | habilitado | PASS |
| `UNIVERSIDAD POPULAR DEL CESAR.` | directorio, procesos | exact, mismo owner | habilitado | PASS |
| `UNIVERSIDA DE LA GUAJIRA` | directorio | exact, mismo owner | deshabilitado (`EXACT_ONLY`) | PASS |
| `UNIVERSIDAD TECNOLOGICA DEL CHOCO.` | ninguna | no se ejecuta como superficie acreditada | no aplicable | PASS: sigue sin reproducirse |

## Simulación de normalización

No se cambió `normalize_text`. Cada estrategia se simuló sobre todas las
claves de canonicales/aliases y sobre las 210 superficies afectadas.

| Estrategia | Ganancias exactas | Nuevas colisiones | Nuevas claves multi-owner | Riesgos wrong-owner |
|---|---:|---:|---:|---:|
| A. actual | 0 | 0 | 0 | 5 |
| B. quitar puntuación terminal | 45 | 12 | 12 | 7 |
| C. colapsar puntuación terminal repetida | 9 | 10 | 10 | 5 |
| D. quitar toda puntuación | 54 | 13 | 13 | 7 |

Las ganancias de B/C no son seguras. B fusiona, entre otras, identidades
distintas de FFDS, Soacha, Yopal, MinJusticia y UAESP. C conserva menos ganancia
y todavía crea diez claves multi-owner. D suma sólo nueve ganancias frente a B
y abre una colisión adicional; tampoco queda recomendada.

Por volumen, la primera bifurcación podría sugerir un normalizador dedicado,
pero el experimento lo veta: introducir normalización produce colisiones y
riesgo de owner equivocado. La decisión P0 es `KEEP_EXPLICIT_ALIASES`, con
curación posterior únicamente después de resolver los duplicados de identidad
y revisar individualmente los 5 `NEEDS_REVIEW` y 15 `DO_NOT_ADD`.

## Top 50 afectados por actividad

Actividad total histórica y reciente desde 2025-09-02. Las cifras se agrupan
por NIT, no por superficie. `899999061` enlaza 35 canonicales conocidos: su
volumen es `SHARED_NIT_UPPER_BOUND`, no atribución a las dos alcaldías afectadas.
Las otras 49 filas tienen un solo canonical conocido para el NIT.

| # | NIT | Canonical afectado | Procesos | Contratos | Total | Reciente | Superficies |
|---:|---|---|---:|---:|---:|---:|---:|
| 1 | `899999061` | ALCALDIA LOCAL DE TEUSAQUILLO(oficial) / ALCALDÍA LOCAL DE CHAPINERO - FONDO DE DESARROLLO LOCAL | 191.718 | 382.372 | 574.090 | 77.575 | 4 |
| 2 | `900971006` | SUBRED INTEGRADA DE SERVICIOS DE SALUD NORTE E.S.E. (OFICIAL) | 72.364 | 64.553 | 136.917 | 16.047 | 4 |
| 3 | `899999063` | UNIVERSIDAD NACIONAL DE COLOMBIA | 110.849 | 14 | 110.863 | 28.548 | 1 |
| 4 | `900958564` | SUBRED INTEGRADA DE SERVICIOS DE SALUD SUR E.S.E.** | 40.430 | 63.019 | 103.449 | 11.581 | 4 |
| 5 | `890480184` | ALCALDÍA DEL DISTRITO TURÍSTICO Y CULTURAL DE CARTAGENA DE INDIAS | 48.537 | 46.587 | 95.124 | 21.262 | 4 |
| 6 | `891480030` | MUNICIPIO DE PEREIRA- OFICIAL | 42.204 | 42.703 | 84.907 | 12.755 | 1 |
| 7 | `900959051` | SUBRED INTEGRADA DE SERVICIO DE SALUD CENTRO ORIENTE E.S.E 1 | 27.956 | 54.820 | 82.776 | 8.171 | 4 |
| 8 | `892400038` | GOBERNACIÓN DEL DEPARTAMENTO ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA | 31.354 | 34.240 | 65.594 | 11.647 | 2 |
| 9 | `890102018` | DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA | 17.020 | 48.348 | 65.368 | 15.090 | 2 |
| 10 | `899999069` | INSTITUTO COLOMBIANO AGROPECUARIO - ICA | 32.866 | 30.189 | 63.055 | 7.945 | 31 |
| 11 | `899999004` | IGAC Sede Central * | 28.281 | 34.398 | 62.679 | 8.834 | 6 |
| 12 | `890201900` | ALCALDIA DISTRITAL BARRANCABERMEJA | 29.248 | 29.486 | 58.734 | 12.505 | 2 |
| 13 | `890501434` | MUNICIPIO DE SAN JOSE DE CUCUTA | 29.276 | 28.722 | 57.998 | 14.956 | 1 |
| 14 | `860061099` | IDRD - ENTIDAD OFICIAL. | 25.904 | 29.080 | 54.984 | 6.538 | 1 |
| 15 | `891280000` | ALCALDIA DE PASTO | 24.608 | 24.357 | 48.965 | 5.873 | 1 |
| 16 | `891580016` | GOBERNACIÓN DEL CAUCA | 24.731 | 23.419 | 48.150 | 9.630 | 1 |
| 17 | `890905211` | DISTRITO ESPECIAL DE CIENCIA TECNOLOGIA E INNOVACION DE MEDELLIN | 27.798 | 20.161 | 47.959 | 6.175 | 1 |
| 18 | `800103923` | GOBERNACION DE NARIÑO | 23.268 | 23.475 | 46.743 | 9.351 | 1 |
| 19 | `8002469532` | FONDO FINANCIERO DISTRITAL DE SALUD.. | 23.735 | 21.256 | 44.991 | 4.715 | 1 |
| 20 | `800094755` | MUNICIPIO DE SOACHA.. | 22.108 | 21.752 | 43.860 | 7.553 | 3 |
| 21 | `892099324` | ALCALDIA MUNICIPIO DE VILLAVICENCIO | 21.595 | 22.110 | 43.705 | 8.305 | 2 |
| 22 | `891480085` | GOBERNACIÓN DE RISARALDA** | 22.780 | 20.769 | 43.549 | 10.257 | 1 |
| 23 | `899999333` | INSTITUTO DISTRITAL PARA LA PROTECCIÓN DE LA NIÑEZ Y LA JUVENTUD - IDIPRON* | 21.172 | 20.482 | 41.654 | 4.432 | 1 |
| 24 | `900490473` | UARIV-UNIDAD PARA LA ATENCION Y REPARACION INTEGRAL A LAS VICTIMAS | 20.712 | 19.809 | 40.521 | 9.715 | 3 |
| 25 | `890102006` | DEPARTAMENTO DEL ATLANTICO** | 13.156 | 25.830 | 38.986 | 5.043 | 2 |
| 26 | `900002583` | RADIO TELEVISION NACIONAL DE COLOMBIA. | 21.797 | 16.595 | 38.392 | 5.177 | 3 |
| 27 | `891855017` | MUNICIPIO DE YOPAL... | 17.548 | 19.039 | 36.587 | 5.350 | 1 |
| 28 | `891780185` | ESE HOSPITAL UNIVERSITARIO JULIO MENDEZ BARRENECHE | 12.927 | 23.147 | 36.074 | 7.793 | 1 |
| 29 | `899999059` | AEROCIVIL | 22.606 | 11.133 | 33.739 | 6.247 | 1 |
| 30 | `899999007` | SUPERINTENDENCIA DE NOTARIADO Y REGISTRO | 12.418 | 18.149 | 30.567 | 4.426 | 1 |
| 31 | `900474727` | MINSALUD | 14.975 | 13.989 | 28.964 | 5.100 | 1 |
| 32 | `891680010` | GOBERNACION DEL CHOCÓ. | 13.273 | 14.429 | 27.702 | 6.689 | 1 |
| 33 | `890900286` | DEPARTAMENTO DE ANTIOQUIA// | 15.049 | 11.623 | 26.672 | 3.012 | 1 |
| 34 | `800130625` | HOSPITAL SAN CRISTOBAL DE CIENAGA EMPRESA SOCIAL DEL ESTADO | 18.023 | 7.484 | 25.507 | 9.492 | 1 |
| 35 | `891800846` | MUNICIPIO DE TUNJA/ | 12.025 | 11.513 | 23.538 | 3.657 | 3 |
| 36 | `891480035` | UNIVERSIDAD TECNOLÓGICA DE PEREIRA | 22.583 | 0 | 22.583 | 3.857 | 2 |
| 37 | `890980112` | ALCALDÍA MUNICIPIO DE BELLO | 10.813 | 11.332 | 22.145 | 5.138 | 1 |
| 38 | `819004070` | ALEJANDRO PROSPERO REVEREND E.S.E. | 13.053 | 8.536 | 21.589 | 6.388 | 1 |
| 39 | `900514813` | COLOMBIA COMPRA EFICIENTE | 14.821 | 5.223 | 20.044 | 1.160 | 1 |
| 40 | `822006595` | EMPRESA SOCIAL DEL ESTADO DEL DEPARTAMENTO DEL META E.S.E | 9.339 | 9.449 | 18.788 | 6.262 | 1 |
| 41 | `842000004` | HOSPITAL DEPARTAMENTAL SAN JUAN DE DIOS DE PUERTO CARREÑO1 | 7.142 | 11.463 | 18.605 | 6.156 | 1 |
| 42 | `800150861` | INSTITUTO NACIONAL DE MEDICINA LEGAL Y CIENCIAS FORENSES | 13.766 | 3.775 | 17.541 | 2.186 | 1 |
| 43 | `830025267` | FONDO NACIONAL AMBIENTAL - ANLA | 8.183 | 8.358 | 16.541 | 1.638 | 1 |
| 44 | `899999028` | MINISTERIO DE AGRICULTURA Y DESARROLLO RURAL | 8.638 | 7.528 | 16.166 | 2.004 | 1 |
| 45 | `8440041972` | RED SALUD CASANARE E.S.E. * | 7.982 | 7.867 | 15.849 | 3.681 | 1 |
| 46 | `899999074` | CAJA DE LA VIVIENDA POPULAR// | 7.810 | 7.565 | 15.375 | 2.340 | 1 |
| 47 | `901536799` | EMPRESA SOCIAL DEL ESTADO UNIVERSITARIA DEL ATLANTICO// | 15.002 | 3 | 15.005 | 5.304 | 1 |
| 48 | `900457461` | MINISTERIO DE JUSTICIA Y DEL DERECHO.. | 7.595 | 6.922 | 14.517 | 1.786 | 1 |
| 49 | `800006850` | empresa social del estado región de salud soacha. | 7.084 | 6.945 | 14.029 | 4.123 | 1 |
| 50 | `830039670` | DISPENSARIO MEDICO DE CALI | 7.929 | 5.505 | 13.434 | 1.001 | 1 |

## Reproducción y límites

```bash
.venv/bin/python scripts/audits/audit_buyer_punctuation_surface_p0.py
```

El script consulta las tres fuentes vivas, pagina/reintenta y falla si Socrata
no responde; no convierte errores de red en ceros. El artefacto conserva los
casos completos, colisiones, otros owners con los mismos tokens, riesgo de
scan, actividad y procedencia del NIT.

No se modificaron `aliases_db.json`, `buyer_source_names.json`,
`normalize_text`, `EntityResolver` ni ningún archivo runtime.
