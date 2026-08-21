# AP3-E.1 — FULL-INDEX P0 REMEDIATION

**Veredicto: PASS.** Los tres P0 que solo eran visibles con el directorio
completo están corregidos en la raíz, la suite dejó de depender del artefacto
local, y la re-auditoría adversarial completa vuelve con todos los gates en
cero.

```
BASE_HEAD   e3141f7baa9806532b4271dabc59d0203d63c344
FINAL_HEAD  (ver commits al final)
FROZEN_CORPUS_SHA256  a3db00d3…a8df7518   sin cambios
```

---

## 1. Los P0, congelados antes de tocar nada

```
P0-1  contratos del proveedor E.S.E HOSPITAL PEDRO LEON ALVAREZ DIAZ en 2025
      mención truncada 'e s'  →  NIT 900047333 (E&S)   esperado 890680027
P0-2  … por mayor valor                       (misma truncación)
P0-3  contratos del proveedor ALCALDIA DE PUERTO SALGAR CUNDINAMARCA
      mención truncada 'alcaldia de puerto salgar' → CC 1127389939
      esperado 8999994138
```

Los tres con `RESOLVED` / `exact_unique_alias`: autoridad plena, sin
clarificación. Congelados en `tests/fixtures/actor_full_index/p0_repros.jsonl`
(sha256 `d896a82a…29fc1541`) y no modificados después.

## 2. Causa raíz

No era el parser. El parser producía la mención **completa**
(`'e s e hospital pedro leon alvarez diaz'`). El fallo estaba en el buscador de
frontera de `resolve_actor_name`: `strong_actor_name_prefixes` genera prefijos
de la mención de más largo a más corto, y el consumidor aceptaba **el primero
que el índice resolviera**. Para el hospital recorría nueve prefijos hasta
llegar a `'e s'` — dos letras — que en el directorio completo es el alias
exacto de la empresa E&S.

Ese bucle existe por una razón legítima: recuperar tokens que la gramática de
modificadores recorta de más, como el `de santander` de *Universidad
Industrial de Santander*. El recorte se validaba reparseando el resto, y ahí
estaba la grieta — el gazetteer reconocía `hospital pedro leon alvarez diaz`
como **entidad compradora**, así que el resto pasaba por cola de modificadores
legítima.

**Por qué no se vio antes.** Con 474 identidades ningún fragmento coincidía con
nada: el mismo bucle producía un falso negativo inocuo. El defecto es idéntico;
lo que cambió es que con 1,2 millones de aliases un fragmento de tres
caracteres encuentra a alguien.

## 3. El arreglo

Dos reglas sobre la **forma del sufijo**, sin excepciones por nombre ni por NIT:

1. **Un modificador externo viene introducido por un conector** — `… en
   Bolívar`, `… en 2025`, `… por mayor valor`. Un token pegado al final del
   nombre pertenece al nombre, por mucho que parezca geografía: la alcaldía de
   *Puerto Salgar Cundinamarca* y el *Consorcio Salud Bolívar 2024* se llaman
   así.
2. **Si el resto se reparsea como entidad nombrada, el corte partió un nombre
   propio.** Las colas admisibles son fecha, orden, monto, geografía, estado y
   modalidad; nunca una entidad.

Los prefijos siguen probándose de más largo a más corto, así que el caso que el
bucle existía para resolver sigue funcionando — y ahora además recupera `en
Bolívar` como departamento:

```
proveedor UNIVERSIDAD INDUSTRIAL DE SANTANDER en Bolívar
  → NIT 890201213 · departamento Bolívar
```

`INV-ACTOR-RESOLUTION-REQUIRES-COMPLETE-MENTION` queda congelado con un
**índice-trampa** donde el fragmento truncado existe deliberadamente como otro
actor real. Un fixture donde el fragmento no coincide con nada recrearía el
punto ciego del índice muestreado que dejó pasar el P0.

## 4. Aislamiento del índice local (§6)

La suite daba dos resultados según la máquina:

| | antes | ahora |
|---|---|---|
| con el artefacto de 662 MB | 2 failed, 8089 passed | **8117 passed** |
| sin él | 8091 passed | **8117 passed** |

Ahora la ruta apunta por defecto a un archivo inexistente en `tmp_path` — el
caso base honesto: sin índice, la resolución falla cerrado. Un test que
necesite directorio construye el suyo. Una prueba que solo corre donde el
artefacto existe demuestra que aun así no se filtra.

El test que asumía que **FUNDACARIBE no podía existir** pasa a un nombre
sintético: el directorio demostró lo contrario (NIT 900429803), así que la
prueba estaba afirmando algo falso sobre SECOP para verificar una política que
no depende de ese nombre.

## 5. Precedencia de geografía canónica (§8)

`contratos de Bogotá` dejaba de ser territorial —dos cédulas cuyo nombre
observado es literalmente "Bogota"— y `contratos de Barranquilla` iba peor:
resolvía a un actor y **ejecutaba** como perfil.

Lo grave no era el caso: el routing de una geografía canónica pasaba a depender
de qué personas figuren en el directorio, y reconstruirlo habría cambiado el
producto sin tocar una línea de código. `INV-BARE-GEOGRAPHY-PRECEDENCE` protege
solo la mención **desnuda**; con sintaxis explícita —`proveedor Barranquilla`—
la resolución sigue permitida, porque alguien puede llamarse como un lugar.

Los topónimos se derivan de las mismas fuentes que el router ya usa
(`DEPARTAMENTOS` y `_COMMON_CITIES`), para que la política no pueda divergir de
lo que el sistema considera un lugar.

## 6. Re-auditoría adversarial completa (§12)

1.150 casos, corpus con SHA idéntico, contra el directorio de 1,2 M:

```
WRONG_AUTHORITATIVE_IDENTITY  0     (antes 3)
TOPIC_HIJACKS                 0
ACTOR_DROPS                   0
FUZZY_AUTO_RESOLVED           0
CANDIDATES / AMBIGUOUS que ejecutan   0 / 0
documento de fuente no cubierto       0  (de 919 resoluciones)
CONTEXT_LOSSES                0     (38 secuencias)
CC_VISIBLE_LEAKS              0
P0 = 0 · P1 = 0
```

Holdout externo con la consulta desnuda obligatoria: 3/6 resueltos con la
identidad correcta, 3/6 `UNRESOLVED` seguro — el mismo resultado que antes de
los arreglos, sin regresión.

## 7. Mutaciones (§11)

4/4 detectadas, código restaurado byte a byte:

| mutación | |
|---|---|
| reintroducir el truncado temprano | detectada |
| permitir que un fragmento existente resuelva la consulta | detectada |
| dejar que el SQLite local afecte al resultado de la suite | detectada |
| geografía canónica desnuda pierde ante el alias exacto | detectada |

La segunda no caía al principio: el caso de prueba usaba un sufijo que solo
producía entidad, y eso ya lo descarta la lista de claves de modificador. Se
rehízo con un sufijo que trae **entidad y modificador a la vez**, que es lo
único que aísla esa guarda.

## 8. Rendimiento — registrado, no resuelto (§10)

```
exact    p50 0,52 ms   p95 1,98 ms
trigram  p50 166 ms    p95 857 ms
```

La medición de AP3-E dio `trigram p95 287 ms` justo tras el build, con el
índice caliente en caché de página; en frío sube a ~857 ms. El rango honesto es
**287–857 ms** según el estado de la caché, sobre un presupuesto de turno de
15 s. Es material y queda **diferido**, junto con el pico de **3,02 GB** del
builder. No se tocó ninguno: cambiarlos aquí habría sido negociar corrección
por velocidad.

## 9. Salida

```
AP3E1_FULL_INDEX_REMEDIATION: PASS

P0_REPROS_BEFORE  3/3 reproducidos y congelados
P0_REPROS_AFTER   3/3 corregidos (mención completa, sin identidad, no ejecuta)

ROOT_CAUSE            el buscador de frontera aceptaba el primer prefijo que el
                      índice resolviera; el resto se validaba reparseándolo y la
                      entidad compradora pasaba por cola legítima
MENTION_BOUNDARY_FIX  conector obligatorio en el sufijo + entidad nombrada en la
                      cola invalida el corte

INV_COMPLETE_MENTION           congelado, índice-trampa con el fragmento como otro actor
INV_BARE_GEOGRAPHY_PRECEDENCE  congelado, con prueba de determinismo frente al índice

FULL_INDEX  IDENTITIES 1.214.351 · ALIASES 1.217.203 · UNCHANGED sí (no se reconstruyó)

TEST_INDEX_ISOLATION
  WITH_PRODUCTION_INDEX     8117 passed / 0 failed / 2 skipped / 24 xfailed
  WITHOUT_PRODUCTION_INDEX  8117 passed / 0 failed / 2 skipped / 24 xfailed
  (idénticos)

FROZEN_CORPUS_SHA256  a3db00d39cc8ed54811ac3ebfea2c05c6c1e275711171295e34015b5a8df7518
UNCHANGED             sí

ADVERSARIAL  P0 0 · P1 0 · WRONG_IDENTITIES 0 · TOPIC_HIJACKS 0 ·
             ACTOR_DROPS 0 · CONTEXT_LOSSES 0

FULL_SUITE 8117 passed · QUERY_CORPUS 89/89 · ACTOR_CORPUS 41/41 ·
DIALOGUE 60/61 (GOB-DEPTO-001 idéntico) · LINT limpio

TRIGRAM_P95  287–857 ms según caché   DEFERRED_PERFORMANCE_ITEM sí
BUILDER_PEAK_MEMORY 3,02 GB           DEFERRED_MEMORY_ITEM sí

DOGFOOD_READY                YES
PRODUCTION_ACTIVATION_READY  YES, con la condición de abajo
MERGE_READY                  YES
UT_READY                     NO (fuera de alcance de V1)
```

**BLOCKERS:** ninguno.

**Condición de activación:** el directorio no se versiona, así que activar el
flag en un entorno sin build previo deja la resolución por nombre inoperante —
su estado seguro, pero inoperante. La activación debe ir acompañada de un build
del directorio en ese entorno.

**NON_BLOCKING:** latencia trigram · memoria del builder · `S.A.S.` frente a
`SAS` en la normalización N2, que afecta a la forma societaria más común del
país y hoy se resuelve por candidato en rango 1.
