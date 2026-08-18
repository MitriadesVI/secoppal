# Money V2 closeout — M3 a M9

- Fecha: 2026-08-18
- Rama: `feature/money-v2`
- Base del arco: `245147cee2485e3eaf0af37bb773cc6a73e36a8f`

## Veredicto

`MONEY_V2_ACCEPT_WITH_LIMITATIONS`

El arco cierra los 16 XFAIL estrictos que quedaban después de M2 sin cambiar
la autoridad de polaridad, la herencia conversacional general, la proyección
`DatasetSpec.value` ni el contrato público `valor_min` / `valor_max` en COP.
La limitación deliberada principal es SMMLV multianual: se bloquea y se pide un
solo año en vez de aplicar silenciosamente la tarifa de una vigencia a otra.

## Problema resuelto

Money V2 debía separar cifras monetarias de NIT y referencias, entender la
puntuación colombiana y cardinales escritos de uso práctico, y convertir SMMLV
sin ocultar que su valor depende del año. El defecto peligroso no era solamente
numérico: un mismo token podía terminar como contratista o como predicado de
valor según el orden de los extractores.

## ADR — autoridad monetaria determinista

### Decisión

La autoridad queda distribuida por responsabilidad:

1. `app/core/money_resolution.py` resuelve literales y el subconjunto práctico
   de cardinales españoles con `Decimal`.
2. `app/utils/money.py` aplica unidades COP y ofrece la conversión SMMLV que
   exige un año explícito.
3. `app/data/smmlv.py` contiene el catálogo anual estático y el reloj civil
   `America/Bogota`.
4. `QueryRouter` arbitra el rol semántico, conserva `valor_min` / `valor_max`
   como enteros COP y adjunta `money_metadata` solo cuando la unidad fuente es
   SMMLV.
5. `QueryPlan` bloquea SMMLV multianual o sin año catalogado antes del builder.
6. `ResponsePolicy` explica el concepto SECOP filtrado y la procedencia SMMLV.

No se introducen LLM, embeddings, paquetes numéricos externos ni llamadas de
red en runtime.

### Consecuencias

- `money_to_cop()` conserva `str -> int | None` y rehúsa SMMLV porque ese
  contrato escalar no puede revelar el año.
- `money_metadata` es un diccionario mínimo y opcional, no una jerarquía de
  objetos que todos los consumidores deban entender.
- La mutación de follow-up acepta metadata únicamente como parte de un filtro;
  un refinamiento COP ordinario elimina metadata SMMLV obsoleta.
- Los campos SoQL siguen siendo decididos exclusivamente por `DatasetSpec`.

## Gramática numérica

Se mantienen los separadores colombianos e internacionales ya cerrados por M2.
M4 añade unidades, 10–29, decenas con `y`, centenas y una composición con
`mil`. Ejemplos soportados incluyen `quinientos millones`, `setecientos
cincuenta millones`, `mil doscientos millones`, `dos mil quinientos millones`,
`veinte mil millones` y `dos billones`.

No es un motor universal de español. Fracciones verbales como `un millón y
medio`, ordinales y escalas encadenadas fuera del subconjunto se rechazan en
vez de interpretarse parcialmente.

Los invariantes de escala permanecen:

- `1 billón = 10^12 COP`.
- `mil millones = 10^9 COP`.
- `1 billón = 1000 × mil millones`.
- Display compacto: `B = 10^12`, `MM = 10^9`, `M = 10^6`.

## Arbitraje NIT ↔ dinero

Una forma de 8–11 dígitos se conserva como identificador por defecto. Etiquetas
como `NIT`, `contratista`, `proveedor` o `documento` son evidencia fuerte de
identificador. Moneda, unidad o comparador monetario adyacente son evidencia
fuerte de dinero. La etiqueta de identificador explícita tiene precedencia.

Los montos se extraen del texto progresivamente scrubbed: un NIT aceptado ya no
está disponible para el scanner monetario, mientras `$1.500.000.000` permanece.
Una consulta puede contener simultáneamente NIT y monto. Referencias como
`CD-08-2026-5533` y `338-2026` no producen filtros de valor.

## SMMLV

Aliases: `SMMLV`, `SMLMV`, `SMLV`, `salario mínimo`, `salarios mínimos`.

Catálogo COP:

| Año | Un SMMLV | Fuente oficial |
|---:|---:|---|
| 2024 | 1.300.000 | Decreto 2292 de 2023, art. 1 |
| 2025 | 1.423.500 | Decreto 1572 de 2024, art. 1 |
| 2026 | 1.750.905 | Decretos 1469 de 2025 y 159 de 2026, art. 1 |

Referencias: [Decreto 2292 de 2023](https://www.suin-juriscol.gov.co/viewDocument.asp?ruta=Decretos/30050726),
[Decreto 1572 de 2024](https://www.suin-juriscol.gov.co/viewDocument.asp?id=30054293),
[Decreto 1469 de 2025](https://www.suin-juriscol.gov.co/viewDocument.asp?id=30055940) y
[Decreto 159 de 2026](https://www.suin-juriscol.gov.co/viewDocument.asp?id=30056106).

Autoridad del año:

- Año único explícito o fechas parseadas dentro de una sola vigencia: ese año.
- Sin año: año civil actual de `America/Bogota`; metadata declara la suposición.
- Follow-up sin año sobre contexto de una vigencia: hereda esa vigencia y
  recalcula el COP antes de ejecutar.
- Contexto o consulta multianual: `QueryPlan` bloquea y solicita un solo año.
- Año no catalogado: bloqueo transparente; nunca extrapolación.

## Semántica de dataset y presentación

- Procesos: `valor_min/max` se proyecta a `precio_base`; la respuesta dice
  `precio base del proceso`.
- Contratos: se proyecta a `valor_del_contrato`; la respuesta dice
  `valor del contrato`.
- Una búsqueda SMMLV muestra cantidad fuente, año y conversión COP.
- Tarjetas compactas ordinarias conservan `format_money`, `format_cop` y la
  convención B/MM/M sin añadir SMMLV contextual fuera de una búsqueda SMMLV.

## M5 y M8

`MONEY_METADATA_DECISION: MINIMAL_METADATA`.

El consumidor real necesita unidad, cantidad fuente, año, tarifa y autoridad
del año; un diccionario opcional basta y evita obligar a todo el sistema a
entender una dataclass monetaria.

`NUMERIC_PRIMITIVE_DECISION: KEEP_SEPARATE`.

`notify.grouping.numeric_value` sanea strings persistidos del Radar, acepta
signos y ordena valores SECOP. `resolve_numeric_literal` exige un literal de
usuario puro, sin signo, y trata `0.500` deliberadamente como decimal. Comparten
tests de paridad para el dominio común, pero consolidarlos cambiaría uno de los
dos contratos.

## Burn-down de los 16 XFAIL originales

| Caso original | Estado final |
|---|---|
| `quinientos millones` | FIXED |
| `novecientos millones` | FIXED |
| `mil quinientos millones` | FIXED |
| `dos mil millones` | FIXED |
| `dos mil quinientos millones` | FIXED |
| `tres mil ochocientos millones` | FIXED |
| `diez mil millones` | FIXED |
| `dos billones` | FIXED |
| NIT sin DV no crea valor | FIXED |
| NIT sin DV no llega a WHERE monetario | FIXED |
| Verbal `más de dos mil millones` conserva mínimo | FIXED |
| `$1.500.000.000` con `más de` es dinero | FIXED |
| `$1.500.000.000` con `por` es dinero | FIXED |
| `2.350.000.000 pesos` produce filtro | FIXED |
| `mil salarios mínimos` no inventa 1.000.000 COP | FIXED |
| API escalar no trata SMMLV como COP atemporal | FIXED |

FINAL: 0 `STILL_BUG`, 0 `RECLASSIFIED`, 0 eliminados.

## Matriz adversarial parser → WHERE

Los casos A13–A15 usan `allow_global=True` únicamente para inspeccionar la
proyección del literal aislado; el workflow normal conserva el guard que exige
tema o scope.

| ID | Query | Dataset | min / max COP | Evidencia WHERE / decisión | Resultado |
|---|---|---|---:|---|---|
| A01 | más de 1.500 millones | contratos | 1.500.000.000 / — | `valor_del_contrato >= 1500000000` | PASS |
| A02 | más de dos mil millones | contratos | 2.000.000.000 / — | `valor_del_contrato >= 2000000000` | PASS |
| A03 | entre mil quinientos y tres mil millones | contratos | 1.500.000.000 / 3.000.000.000 | ambos predicados | PASS |
| A04 | NIT 900123456 | contratos | — / — | `documento_proveedor`, sin valor | PASS |
| A05 | NIT 900.123.456-7 | contratos | — / — | OR base + DV, sin valor | PASS |
| A06 | por $1.500.000.000 | contratos | 1.200.000.000 / 1.800.000.000 | rango ±20 % legacy | PASS |
| A07 | por 2.350.000.000 pesos | contratos | 1.880.000.000 / 2.820.000.000 | rango ±20 % legacy | PASS |
| A08 | procesos mayores a 2.000 millones | procesos | 2.000.000.000 / — | `precio_base >= 2000000000` | PASS |
| A09 | contratos mayores a 2.000 millones | contratos | 2.000.000.000 / — | `valor_del_contrato >= 2000000000` | PASS |
| A10 | contratos > 1000 SMMLV en 2026 | contratos | 1.750.905.000 / — | valor contrato + fecha 2026 | PASS |
| A11 | procesos > 1000 SMMLV en 2026 | procesos | 1.750.905.000 / — | precio base + fecha 2026 | PASS |
| A12 | 2024–2026 > 1000 SMMLV | contratos | — / — | bloqueado; pide un año | PASS |
| A13 | 1 billón | procesos | — / 1.000.000.000.000 | `precio_base <= 1000000000000` | PASS |
| A14 | mil millones | procesos | — / 1.000.000.000 | `precio_base <= 1000000000` | PASS |
| A15 | 1.5 billones | procesos | — / 1.500.000.000.000 | `precio_base <= 1500000000000` | PASS |

## Validación final

| Gate | Resultado |
|---|---|
| Focal M7 / paridad pública | 324 passed |
| M8 Radar + Money | 268 passed |
| Canary workflow local | 6 passed |
| Matriz adversarial | 15/15 |
| Query corpus | 89/89 |
| Dialogue corpus | 60/61; solo `GOB-DEPTO-001` basal |
| Torture matrix | 38/38 |
| `make lint-core` | PASS |
| Suite global | 7645 passed, 2 skipped, 24 xfailed |

El canario usa `SecopalWorkflow` con cliente SECOP determinista y cubre COP
numérico, separadores colombianos, verbal, NIT, SMMLV y follow-up SMMLV con año
heredado. No depende de disponibilidad Socrata.

## Arco de commits

| Fase | Commit | Propósito |
|---|---|---|
| M3 | `aa51cb4` | arbitraje dinero / identificadores |
| M4 | `0d60b3d` | cardinales monetarios prácticos |
| M5 | NOOP | decisión `MINIMAL_METADATA` |
| M6 | `1432140` | SMMLV con semántica anual |
| M7 | `7aacb5f` | transparencia de filtro económico |
| M8 | NOOP | decisión `KEEP_SEPARATE` |
| M9 | este commit | seam de metadata descubierto por canario, pruebas y closeout |

## Limitaciones intencionales y deuda real

1. SMMLV multianual se aclara; no se divide aún en consultas por vigencia.
2. El catálogo SMMLV cubre 2024–2026 y requiere actualización manual con acto
   oficial para una nueva vigencia.
3. La gramática verbal no cubre fracciones, ordinales ni un motor universal de
   español.
4. `money_metadata` es un dict mínimo; si aparecen varias unidades indexadas o
   más consumidores tipados, se debe reevaluar una estructura formal.
5. Las consultas `por X` conservan el rango legacy ±20 %; Money V2 no redefine
   esa semántica.
6. `GOB-DEPTO-001` continúa como único fallo basal del corpus de diálogo y es
   ajeno a dinero.

Fuera de alcance por decisión: UVT, divisas, inflación, CPI, ranking, Radar,
analytics, Truth Layer y rediseños generales de parser o formatter.
