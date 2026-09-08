# Buyer–Actor V1 — cierre de reparación

**Veredicto: BUYER_ACTOR_V1_READY_FOR_REVIEW**. Las dos semillas y los contrastes obligatorios pasan; integración y activación no forman parte de este cierre.

## Tesis y alcance

El rol se decide por la mención completa y la evidencia del comprador antes de consultar el directorio de proveedores. No se usa cardinalidad del índice, similitud, ciudad, tipo documental inferido ni una lista de instituciones exceptuadas.

La gramática «qué ha contratado X» conserva su reconocimiento contractual. Si X coincide íntegramente con la superficie que acreditó un comprador exacto, QueryRouter consume petición y mención antes de extraer tema, conserva los filtros y selecciona contratos. No crea name_unresolved ni SET_ACTOR. La misma prueba impide la sonda bare opcional para «contratos de X». Las construcciones dirigidas a un proveedor, incluidos documentos y adjudicados a, conservan Actor.

El helper no da autoridad a entidad_resolved aislada: exige value coherente, kind buyer_entity, confidence high, identidad exacta (o root_alias_evidence determinista) y equivalencia completa con rewritten_from / alias escaneado. Rechaza fuzzy, city_to_entity, pendientes, prefijos y evidencia de otra mención.

Dos controles exigieron enganches adicionales autorizados:
- Una petición completa de comprador podía heredar Actor del turno anterior. La admisión previa al merge reconoce búsqueda nueva con los mismos criterios; Frame ON conserva la precedencia de un perfil explícito.
- Una institución explícitamente proveedora podía conservar el comprador extraído de su propio nombre. El scrub consume rewritten_from cuando pertenece a esa ocurrencia Actor; la relación independiente X con comprador conserva ambos filtros.
- En una relación con rol explícito, el marcador proveedor ahora conserva evidencia fuerte de Actor para que la detección posterior no vuelva a absorber al comprador en su nombre.

Archivos y mapa: [scope.md](scope.md). No se tocó query_plan.py, catálogos, índices productivos, flags por defecto ni mecanismos de selección/TTL.

## Procedencia histórica acreditada

| Transición | Evidencia | Conclusión limitada |
|---|---|---|
| 547c80ff, 18 ago 2026, padre 3d3b54de | history-verbal-introduction.diff.gz | Introduce premisa verbal de sujeto contratista. |
| 3b3c7f97, 19 ago, padre 6fc85ad5 | history-probe-introduction.diff | Introduce resolución contextual y proyección bare_exact_probe. |
| 2f3b66c8, 20 ago, padre 34053f4f | history-article-change.diff.gz | Protección article_led_buyer posterior al exact miss, con artículo; una colisión exacta la precede. |
| 691ca352, 6 sep, padre bb1876f1 | history-commits.txt y git log -S default=True | Actor pasa a ON por defecto. Distinguir presencia de regla de configuración efectiva. |
| 78686647, 7 sep, antes del Frame | history-pre-frame/ y history-pre-frame.txt | Ambas semillas reproducen el secuestro completo con colisiones sintéticas y Actor ON. |
| 52a6bcc6, 8 sep, base congelada | history-base/ y history-base.txt | Misma falla con Frame OFF y ON. |

Los cuatro módulos Actor responsables no cambian entre 78686647 y BASE; el diff acotado está en history-before-frame-to-base.diff. Se usaron git log -S, blame, diffs contra padre y exportaciones propias mediante git archive. No se operó ningún checkout ajeno.

Las reproducciones usan fecha de consulta 2026-09-08, mismas filas fuente sintéticas, persistencia nueva e índices construidos con el builder de cada versión. La referencia pre-Frame se ejecuta solamente con Frame OFF, porque esa capacidad aún no existe; el selector histórico de esos diez casos se registra como tal. No se confunde ese límite con un fallo de producto.

La primera versión en la que ESTE encargo demuestra el fallo completo es 78686647. No se demuestra que cada versión intermedia desde agosto fallara de punta a punta ni se encontró/probó un ancestro verde compatible. Esto no acredita «antes del Frame funcionaba»; acredita lo contrario para la referencia suministrada. No se ejecutaron todas las versiones ni sus suites.

## Antes/después de las semillas

| Consulta | Base con 3 identidades homónimas, OFF y ON | Reparación, OFF y ON |
|---|---|---|
| contratos de alcaldia de cartagena por mas de 500 millones | bare_exact_probe → exact_ambiguous_alias; candidatos y cero llamadas SECOP | contratos; comprador Cartagena; mínimo 500000000; cero lookup de proveedores y una consulta de contratos |
| que ha contratado alcaldia de barranquilla por mas de 2000 millones | strong_actor_syntax / ACTOR_VERB_MARKER → exact_ambiguous_alias; candidatos y cero llamadas | contratos; comprador Barranquilla; mínimo 2000000000; sin Actor ni tema «contratado»; una consulta |

Sin homónimo, Cartagena ya podía ejecutar, aunque realizaba la sonda opcional; no se presenta ese caso como secuestro. Con un alias único, el resultado incorrecto puede ejecutar un perfil/filtro de proveedor, no necesariamente pedir selección. Barranquilla puede bloquear aun sin índice. Los 20 rojos iniciales incluyen la condición de no hacer lookup innecesario: no significan 20 listas de candidatos.

[seed-matrix.json](seed-matrix.json) contiene 50 filas de reproducción histórica y final. Cada archivo de history-base/ y after/ conserva texto, params iniciales, configuración/persistencia, llamadas reales al resolver instrumentadas sin sustituir su lógica, params finales, SQL enviado, filas de fuente, respuesta y estado publicado.

## SQL y universo respondido

Los WHERE efectivos finales son:

```sql
unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%ALCALDÍA DEL DISTRITO TURÍSTICO Y CULTURAL DE CARTAGENA DE INDIAS%'))
AND valor_del_contrato >= 500000000
```

```sql
unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA%'))
AND valor_del_contrato >= 2000000000
```

Dataset físico jbjy-vk9h. El SELECT completo incluye documento_proveedor porque es una columna de resultados; el WHERE no contiene documento de contratista. Se conserva >= según la semántica monetaria existente, sin rediseñar comparadores. No se agrega fecha, estado ni geografía no pedida.

La frontera de transporte evalúa el SQL en SQLite sobre 60 contratos sintéticos válidos más dos señuelos (otro comprador y monto insuficiente), con nombres físicos obtenidos de la autoridad project_buyer_names cuando corresponde. Rechaza predicados incorrectos y registra violaciones fuera del try del producto: un error absorbido por el workflow no puede regalar un test verde. El resultado muestra comprador y filtro económico; no es una respuesta prefabricada.

Continuidad: comprador → solo 2026 → muéstrame más, nueva instancia workflow/lectura de persistencia en cada turno. Se conserva comprador/monto, se agrega exclusivamente la fecha solicitada y se verifica que la página siguiente contenga IDs distintos del universo anterior correcto. Evidencia: after/continuity-*.json. Otra secuencia parte de FUNDACARIBE legítimo y abre una nueva búsqueda de comprador sin heredar su documento: after/new-after-actor-*.json.

## Contrastes y propiedades

Pruebas nuevas normalmente recolectadas en tests/test_buyer_actor_role_*.py:

- 2 semillas × Frame OFF/ON × índice vacío, único, ambiguo de tres identidades, ausente e incompatible.
- Artículo, acentos, mayúsculas, año, tema software y orden por valor.
- Alcaldías Galapa/Baranoa y Universidad del Atlántico del catálogo vigente; se respeta la separación UNIATLANTICO / nombre físico de fuente.
- FUNDACARIBE bare y verbal; proveedor por nombre/documento; único y ambiguo; proveedor con Cartagena; institución explícitamente proveedora; adjudicados a; nombre largo con alias institucional parcial.
- El nombre largo bare con tema mantiene la aclaración de intención vigente, sin ejecutar ni usar el prefijo como comprador. El equivalente con proveedor explícito conserva Actor completo.
- Ciudad desnuda y tema normal/homónimo; negaciones explícitas de Actor/comprador conservan aclaración previa a resolver.
- Hypothesis determinista, 40 ejemplos: cardinalidad, orden, frecuencia, CC/NIT y aliases ajenos no cambian params/rol/SQL de las semillas. Cada índice usa builder real. La consulta explícita de proveedor sí varía entre identidad única y ambigua. No hay I/O externo.
- Controles negativos del helper: fuzzy, city promotion, no confirmado, value distinto, kind distinto, provenance ausente y mención parcial/ajena.

La única expectativa anterior adjudicada es «que ha contratado UNIVERSIDAD INDUSTRIAL DE SANTANDER». Véase [expectation-adjudication.md](expectation-adjudication.md): commit/hash original y contrato autorizado. Las filas de identidad se conservaron. Ningún xfail o deselect nuevo oculta ese caso.

## Índice local real

Disponible: data/actor_alias_index.sqlite del worktree principal. Solo lectura immutable=1, limitado a exact lookup de las dos menciones y metadata; no se exportan candidatos personales.

Schema 1, normalización N2, construido 2026-08-20T18:10:58+00:00, 662409216 bytes. Los lookups devuelven exact_ambiguous_alias: Cartagena 20 candidatos devueltos (no afirmación del total), Barranquilla 4. Las cuatro ejecuciones reparadas OFF/ON usan el resolver real configurado y no invocan el índice para imponer rol. El transporte SECOP es sintético. Tamaño, mtime_ns e inode permanecen iguales; ninguna reconstrucción ni escritura en el artefacto.

## Validación y límites

HEAD funcional probado: aab5d41d45e58f21b727929d4ec454f8fbbbf9cd. Rama fix/buyer-actor-role-arbitration-v1. Los commits posteriores de cierre contienen únicamente documentación/evidencia; ver git log para HEAD final.

Herramientas: Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10. Codegraph 0.9.6 ejecutado con Node 22: Not initialized; MCP no cargado. Fallback rg/Git/manual expresamente declarado. No se usó un índice desactualizado como autoridad.

Salidas literales:

```text
All checks passed!
Ran 3 rules on 327 files: 10 findings.
1718 passed, 5 skipped, 2 xfailed in 140.19s (0:02:20)
530 passed, 2 xfailed in 85.27s (0:01:25)
Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)
app/core/ limpio
```

Ver ruff.txt, semgrep-final.txt.gz y semgrep-final.json, focal-final.txt, corpus.txt, lint-core.txt y diff-check.txt. Semgrep es baseline global rojo: diez hallazgos idénticos por regla/ruta/fragmento, cero nuevos y cero removidos (semgrep-delta.json), ninguno en archivos propios. El exit 0 del CLI sin --error no significa cero findings.

Corrí los archivos de focal-manifest.txt, no la suite. La pasada adicional frame-on-manifest.txt ejecutó consumidores Actor/admisión/utilidad con Frame ON ambiental y las nuevas pruebas en ambos estados: 530 passed, 2 xfailed. Incluye las cuatro comprobaciones locales efectivas, sin skips. Esos totales se solapan: no se suman como garantías independientes. Los dos xfails estrictos son anteriores y siguen ejecutándose; un skip corresponde a ausencia del índice en el worktree aislado. Otros cuatro skips son los tests locales opt-in, ejecutados aparte con resultado 4 passed. No se ejecutó test_actor_conversation_dogfood.py porque lee otras menciones del directorio real fuera de la autorización; los controles de selección/tokens/TTL se ejecutaron en las suites aisladas.

No pytest global, app, SECOP live, LLM, radares/mensajes, merge, push, despliegue, modificación de dogfood ni cambio de flags productivos. Flags únicamente por proceso de tests. No repin de evidencia ni alteración de catálogos.

## Hallazgos fuera del lote y recomendaciones

1. «contratos sin proveedor FUNDACARIBE» conserva actualmente objeto_exclude=FUNDACARIBE y produce una exclusión temática. Se encontró al ensayar negaciones; diagnostic.txt conserva la cadena. La negación explícita «donde el proveedor no sea…» sí mantiene la política de aclaración y está en los controles. No se modificó el parser de negaciones; recomiendo adjudicar esta forma corta en un lote propio.
2. Se mantienen dos xfails históricos de Actor + región explícita + comprador. No se modifican para declarar verde este carril.
3. Antes de integrar corresponde revisión independiente del SHA y suite global única del coordinador. La reparación no autoriza activación ni integración.

Nada de FORMADO/FORMANDO VIDAS, ranking fuzzy de personas, Radar, contador de trazas, publicación P2 o nuevos catálogos forma parte del cambio.

Los logs y diffs con espacios terminales emitidos por las herramientas se guardan en gzip sin pérdida (raw-evidence-packaging.json conserva SHA256 de bytes originales). Así se preserva la salida literal y el diff del carril no introduce whitespace inválido.
