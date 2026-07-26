# Guía — Búsqueda conversacional de modalidad

**Estado:** vigente desde 2026-07-26
**Decisión de fondo:** ADR-054 (`docs/decisions/2026-07-26_ADR-054_conversational-modality-contract.md`)

Esta guía explica cómo SECOPPAL entiende la modalidad de contratación en una
conversación: qué reconoce en el primer turno, cómo refina en los siguientes,
qué frases mutan el filtro de modalidad y qué frases lo dejan quieto pidiendo
aclaración.

---

## 1. Qué entiende SECOPPAL

En el primer turno se puede pedir modalidad junto con objeto, territorio y
estado en una sola frase:

```text
mínimas cuantías de mantenimiento abiertas
licitaciones públicas de alimentación en Atlántico
procesos excepto contratación directa
```

Cada uno produce un filtro de modalidad **canónico**: un conjunto de modalidades
incluidas y/o excluidas, resuelto una sola vez por turno. `excepto contratación
directa` no restringe la búsqueda a una modalidad: la abre a todas menos esa.

---

## 2. Refinamientos ortogonales

Un follow-up que habla de otra dimensión **no toca** la modalidad. Ejemplo real:

```text
T1: mínimas cuantías de mantenimiento abiertas
T2: en la costa atlántica
T3: entre 30 y 50 millones
```

Después del T3 la búsqueda conserva modalidad (mínima cuantía), estado
(abiertas) y topic (mantenimiento), y suma la región Caribe y el rango de
30 a 50 millones. Nada de lo anterior se pierde ni se re-pregunta.

Lo mismo aplica al revés: una operación de modalidad no borra el territorio, el
monto, la fecha ni el orden que ya venían del turno anterior.

---

## 3. Operaciones conversacionales de modalidad

Estas son las siete operaciones. Cada una opera sobre el filtro del turno
anterior:

| Frase de ejemplo | Operación | Qué hace |
| --- | --- | --- |
| `ahora licitación pública` | REPLACE | descarta el filtro previo y deja solo lo nuevo |
| `además contratación directa` | ADD_INCLUDE | suma una modalidad a las ya buscadas |
| `excepto mínima cuantía` | ADD_EXCLUDE | agrega una exclusión al filtro |
| `quita contratación directa` | REMOVE_MEMBER | retira esa modalidad del filtro, esté incluida o excluida |
| `permite contratación directa` | REMOVE_EXCLUDE | levanta una exclusión previa |
| `vuelve a incluir contratación directa` | RESTORE_INCLUDE | levanta la exclusión; solo la agrega a la lista si ya había lista |
| `sin filtro de modalidad` | CLEAR | quita por completo el filtro de modalidad |

Otras frases equivalentes reconocidas: `cambia a` / `solo` / `únicamente` (=
REPLACE), `también` / `agrega` (= ADD_INCLUDE), `excluye` (= ADD_EXCLUDE),
`elimina` / `saca` (= REMOVE_MEMBER), `deja de excluir` (= REMOVE_EXCLUDE),
`reincorpora` (= RESTORE_INCLUDE), `quita el filtro de modalidad` / `cualquier
modalidad` / `todas las modalidades` / `no importa la modalidad` (= CLEAR).

### 3.1 La diferencia entre `permite` y `vuelve a incluir`

Ambas levantan una exclusión, pero el resultado depende de si había una lista
blanca:

- Si la búsqueda era `alimentación excepto contratación directa` (universo
  abierto con una exclusión), `vuelve a incluir contratación directa` devuelve
  el universo abierto — **no** deja la búsqueda restringida a contratación
  directa.
- Si la búsqueda era `licitación pública o contratación directa` y luego se
  excluyó una, `vuelve a incluir` la devuelve a esa lista.

### 3.2 Repetir una operación no es un error

Quitar algo que ya no estaba, o agregar algo que ya estaba, se resuelve como
`no_op`: la búsqueda queda igual y no se reporta como fallo.

---

## 4. Frases que piden aclaración

Estas tres **no ejecutan nada** y conservan la búsqueda anterior intacta:

```text
sin modalidad
agrega y quita contratación directa
además subasta
```

- `sin modalidad` es ambiguo: puede querer decir "quita el filtro" o "busca
  procesos que no tengan modalidad registrada". SECOPPAL no adivina. Para
  quitar el filtro hay que decir `sin filtro de modalidad`.
- `agrega y quita contratación directa` pide dos operaciones incompatibles
  sobre el mismo target: es un conflicto de control.
- `además subasta` nombra un target que no se resuelve a una modalidad única.

En los tres casos no se construye SoQL ni se consulta SECOP: el filtro previo
queda exactamente como estaba. **Nunca hay mutación parcial**: si un pedazo del
turno no es ejecutable, no se aplica ninguna parte.

---

## 5. Lenguaje descriptivo

Verbos como `incluye`, `permite`, `usa` o `excluye` son señales de control
**solo cuando no pertenecen al objeto ni al nombre jurídico de la modalidad**:

```text
contratación directa que incluye suministro
régimen especial que permite ofertas
```

Aquí `incluye` y `permite` describen el objeto del contrato, no ordenan mutar
la modalidad. La misma regla protege el `sin` que vive dentro de un nombre
jurídico: en `contratación directa sin ofertas`, ese `sin` no es una exclusión.

Cuando la frase es ambigua entre las dos lecturas, gana la lectura jurídica: el
filtro no se muta.

---

## 6. LLM

- El camino habitual es **determinístico**: parser, resolución de modalidad y
  álgebra conversacional se resuelven sin modelo.
- El LLM solo completa algunos huecos semánticos que quedaron vacíos: una única
  representación del objeto, entidad, ciudad o departamento.
- **No decide** dataset, modalidad, estados, monto, fechas, orden ni intent. Un
  campo que el pipeline ya resolvió no se sobrescribe.
- Ver `LLM: No` en el trace de una consulta es lo normal y lo deseable — no
  indica que algo haya fallado.

---

## 7. Limitaciones y deuda conocida

- **Ruido de verbos descriptivos en topic:** `incluye`, `permite` y `quien`
  pueden quedar dentro del objeto extraído y ensuciar la búsqueda de texto.
- **Huecos de cortesía y gazetteer** que aún activan el fallback LLM:
  `Colombia` queda como objeto; `Cali` y `Barranquilla` requieren desambiguación
  ciudad vs. departamento; el typo `cundinamarcar` no resuelve; saludos (`hola`,
  `estimado`) y palabras vacías (`algo`) quedan como topic.
- **`GOB-DEPTO-001`** (`entidad_resolved` no resuelve `GOBERNACIÓN DE BOYACÁ`)
  es un fallo basal del corpus, ajeno a modalidad, con firma idéntica al
  baseline.
- **M3C-D diferido:** no existe todavía el sidecar de aclaración que recuerde un
  bloqueo entre turnos. Se reabrirá solo cuando la telemetría real cumpla la
  compuerta definida en ADR-054.

---

## 8. Para desarrolladores

| Recurso | Ruta |
| --- | --- |
| Decisión aceptada | `docs/decisions/2026-07-26_ADR-054_conversational-modality-contract.md` |
| Closeout as-built (autoridad) | `docs/planes/2026-07-25_M3C_modality_conversation_closeout.md` |
| Contrato histórico de diseño M3C | `docs/planes/2026-07-23_M3C_modality_conversation_contract.md` |
| Enmiendas control↔target (R3) | `docs/planes/2026-07-24_M3C_B_R3_target_span_control.md` |
| Motor puro del álgebra | `app/core/modality_followup.py` |
| Wiring al pipeline | `app/core/modality_followup_wiring.py` |
| Telemetría / product gate | `app/core/modality_followup_telemetry.py` |

Tests principales:

```text
tests/test_modality_followup.py             # álgebra pura y clasificador
tests/test_modality_followup_wiring.py      # asociación control↔target y spans
tests/test_modality_followup_pipeline.py    # E2E de operaciones, rollback y SoQL
tests/test_modality_pipeline_integration.py # integración parser/builder
tests/test_modality_precedence.py           # precedencia y masking
tests/test_dialogue_e2e_invariants.py       # invariantes del corpus multi-turn
```

Corpus multi-turn congelado: `tests/fixtures/dialogue_e2e_corpus.yaml`
(29 secuencias / 64 turnos; 10 de ellas son las secuencias `DLG-M3C-*`). Las
matrices completas de cobertura y adversariales viven en el closeout, no aquí.
