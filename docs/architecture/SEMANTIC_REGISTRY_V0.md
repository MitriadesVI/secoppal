# Semantic Registry V0

**Estado:** `SHADOW_DORMANT`

**Versión de schema:** `1`

**Versión de datos:** `0.1.2`

**Auditoría de origen:**
[`docs/audits/2026-08-22_SEMANTIC_AUTHORITY_AUDIT.md`](../audits/2026-08-22_SEMANTIC_AUTHORITY_AUDIT.md)

**Fuente de evidencia RS1.2:** MP1 diagnostic human labels,
[`docs/audits/2026-08-22_MP1_DIAGNOSTIC_LABEL_ANALYSIS.md`](../audits/2026-08-22_MP1_DIAGNOSTIC_LABEL_ANALYSIS.md)

## Decisión

SECOPPAL incorpora un registro semántico mínimo y tipado como autoridad de
**datos**, todavía sin autoridad de ejecución. El artefacto canónico es
`app/data/semantic_registry_v0.json`; `app/core/semantic_registry.py` solo lo
carga y valida con biblioteca estándar.

Ningún módulo productivo importa el loader. El registro no modifica
`QueryFrame`, no genera SoQL, no consulta SECOP, no expande consultas, no rankea,
no formatea y no participa en follow-ups. Cualquier conexión futura requiere un
bloque explícito identificado como `RUNTIME_WIRING_REQUIRED_LATER`.

## Alcance V0

La muestra contiene 20 conceptos y 13 relaciones. Dieciocho conceptos y once
relaciones provienen de conocimiento ya codificado en
`morphological_variants.py`; RS1.2 agrega únicamente dos conceptos y dos
relaciones respaldados por los labels humanos de MP1. No es una migración
completa.

Tipos utilizados por la muestra:

- `EXACT_EQUIVALENT`
- `PROGRAM_ALIAS`
- `NARROWER`
- `ASSOCIATED_WITH`
- `QUALIFIER`
- `UNKNOWN`

El schema acepta además el catálogo inicial completo:
`MORPHOLOGICAL`, `ALIAS`, `BROADER`, `COMPOSED_WITH`,
`CONTRACT_FUNCTION`. Que el schema pueda representarlos no crea relaciones de
esos tipos.

Conceptos con `REVIEW_REQUIRED`: `simat`, `hospital`, `hogar` y `edificio`.
Sus relaciones actuales se preservan como evidencia histórica `UNKNOWN`, no se
promueven a asociación o equivalencia y todas sus políticas fallan cerradas.

## Contrato de datos

Cada concepto declara:

```json
{
  "concept_id": "nutricion",
  "concept_type": "DOMAIN_OR_SERVICE",
  "lexical": {
    "canonical": ["nutricion"],
    "variants": ["nutrición"]
  },
  "relations": [
    {
      "type": "ASSOCIATED_WITH",
      "target": "alimentacion",
      "review_status": "REVIEWED"
    }
  ],
  "retrieval_policy": {
    "replace_explicit_term": false,
    "equivalent_auto_expand": false,
    "associated_auto_expand": false,
    "associated_candidate_only": true
  },
  "provenance": {
    "source": "app/data/morphological_variants.py:nutricion->alimentacion",
    "review_status": "REVIEWED"
  }
}
```

Las listas léxicas describen superficies del **mismo concepto**. Las relaciones
describen vínculos entre conceptos con IDs estables. Ninguna de las dos cosas
es una instrucción de query en V0.

## Revisión limitada RS1.2

La evidencia humana congelada de MP1 soporta la identidad distinguible
`centro_de_vida` como `FACILITY`. Sus únicas superficies registradas son
`centro de vida` y `centros de vida`; no se incorporan términos adicionales.

La arista revisada

```text
centro_de_vida --ASSOCIATED_WITH--> adulto_mayor
```

significa solamente que un centro de vida **puede** ser un setting asociado a
Adulto Mayor cuando existe contexto suficiente. No declara equivalencia, no
afirma que todo centro de vida pertenezca a ese mercado y no autoriza apertura
de retrieval por la frase desnuda. Por eso las cuatro flags de retrieval de
`centro_de_vida` permanecen en `false`, incluida
`associated_candidate_only`.

Los cinco hard negatives humanos de “Centro de Vida Sensorial” respaldan el
concepto mínimo `sensorial` como `DOMAIN_SIGNAL` y la arista dormida:

```text
centro_de_vida --QUALIFIER--> sensorial
```

En este contexto composicional, `sensorial` es conflictivo con la herencia
automática hacia Adulto Mayor. La arista registra evidencia, no ejecuta una
regla de exclusión.

```text
V0_POLICY_LIMITATION:
V0 has concept-scoped boolean retrieval policy and no runtime conflict polarity.
QUALIFIER records the reviewed contextual signal but does not implement blocking.
centro_de_vida + sensorial may not inherit or auto-open Adulto Mayor.
```

`hogar` conserva su relación histórica `UNKNOWN -> vivienda` y estado
`REVIEW_REQUIRED`. `HOGAR_SIGNAL = REQUIRES_CONTEXT`: la palabra desnuda no es
señal de Adulto Mayor y RS1.2 no agrega una relación con esa población.

`alimentaria` queda diferida porque su evidencia es `PARTIAL` y requiere
adjudicación dirigida. `tercera_edad` queda diferida porque es
`NOT_EVALUATED`. Ninguna de las dos formas entra al registro en este bloque.

## Tipos intrínsecos de concepto

`concept_type` responde **qué clase estable de cosa es el concepto**, no qué rol
ocupa dentro de una consulta o un Market Pack. Un `FACILITY` como `hospital`
puede actuar contextualmente como asset, setting, destination o qualifier sin
cambiar su identidad en el registro. Los roles composicionales pertenecen al
Market Pack futuro y no se modelan en V0.

El vocabulario cerrado de RS1.1 tiene ocho miembros:

```text
POPULATION
SERVICE
DOMAIN_OR_SERVICE
PROGRAM
SYSTEM_SIGNAL
FACILITY
DOMAIN_SIGNAL
ACTION
```

`DOMAIN_OR_SERVICE` y `DOMAIN_SIGNAL` son salidas conservadoras para conceptos
cuya clase más específica no está demostrada. No autorizan inferencia ni
expansión. `mantenimiento`, `mantenimiento_correctivo` y
`mantenimiento_preventivo` son `ACTION`; no se clasifican como función
contractual por el papel que quizá ocupen en un mercado futuro.

## Semántica de dirección

Una relación se lee desde el concepto que la declara hacia `target`. El loader
no crea inversas.

```text
mantenimiento_correctivo --NARROWER--> mantenimiento
```

no declara por sí sola:

```text
mantenimiento --BROADER--> mantenimiento_correctivo
```

Si una relación simétrica necesita navegación en ambos sentidos, cada edge debe
estar declarado de forma auditable. Por eso `adulto_mayor` y `persona_mayor`
declaran explícitamente sus dos edges `EXACT_EQUIVALENT`.

## Alcance de retrieval policy

```text
V0 LIMITATION:
Retrieval policy is concept-scoped.
All outgoing relations therefore share the concept-level eligibility envelope.
Relation-scoped policy is deferred until demonstrated necessary.
```

No existe `RelationPolicy` ni motor de expansión. Mientras la política sea por
concepto, una sola arista `REVIEW_REQUIRED` obliga a que todas las políticas de
reemplazo, expansión o candidate eligibility del concepto estén desactivadas.
Esto no cambia `provenance.review_status`: autoridad del concepto y autoridad
de la relación siguen siendo estados independientes.

## Invariantes V0

### INV-SR-01 — Explicit survives

El loader rechaza cualquier concepto con `replace_explicit_term=true`.
`nutricion` conserva ID y política propia; su asociación con `alimentacion` no
autoriza sustitución.

### INV-SR-02 — Association is not equivalence

`ASSOCIATED_WITH` y `EXACT_EQUIVALENT` son miembros distintos de un enum cerrado.
No existe conversión entre ambos. `nutricion` solo declara el primero.

### INV-SR-03 — Direction matters

`BROADER` y `NARROWER` son edges dirigidos. `relations_from()` devuelve solo lo
declarado y no deriva inversas.

### INV-SR-04 — Data authority, not runtime engine

El loader depende únicamente de `json`, `re`, dataclasses, enum, pathlib,
`MappingProxyType` y typing. No importa builder, cliente SECOP, router, ranking,
BM25, PhraseSpec ni LLM.

### INV-SR-05 — Existing runtime unchanged

El modo permitido es `SHADOW_DORMANT`. Los tests inspeccionan todos los módulos
de `app/core` y fallan si alguno importa el registro.

### INV-SR-06 — Phrase layer remains independent

`PhraseSpec`, anchors, gaps, matching, validación de fila y ranking permanecen
en su capa actual. El registro no los modela.

### INV-SR-07 — Unknown fails closed

Toda relación `UNKNOWN` debe tener `REVIEW_REQUIRED`. Un concepto con provenance
`REVIEW_REQUIRED` no puede reemplazar, auto-expandir ni habilitar candidatos.
Además, cualquier arista `REVIEW_REQUIRED` bloquea políticas accionables del
concepto, aunque el concepto mismo permanezca `REVIEWED`.

### INV-SR-08 — One new semantic data authority

Los conceptos y edges V0 existen solo en el JSON. Python define tipos y
validaciones, no una lista espejo de relaciones.

## Validaciones del loader

La carga falla ante:

- schema o modo desconocido;
- `concept_id` duplicado o que no sea slug estable;
- `concept_type` fuera del vocabulario intrínseco cerrado;
- canonical vacío o superficies duplicadas;
- tipo de relación fuera del enum;
- edge duplicado;
- referencia a concepto inexistente;
- `UNKNOWN` revisado como si fuera seguro;
- reemplazo de término explícito;
- auto-expansión de asociaciones;
- cualquier política expansiva en conceptos `REVIEW_REQUIRED`;
- cualquier política accionable cuando una arista sea `REVIEW_REQUIRED`.

## Fronteras que permanecen fuera

No se absorben PhraseSpec/matching/ranking, BM25, order semantics, query sanity,
follow-ups, dinero, estados, modalidad, naturaleza contractual, geografía,
entity resolution, actor identity, Truth Layer ni Radar. Sus contratos actuales
responden preguntas distintas y no deben converger en un god-object semántico.

## Próximo bloque posible

Antes de conectar producción debe decidirse, relación por relación, cómo
preservar el término explícito en el frame y el trace, y qué expansiones son solo
candidatas. V0 no autoriza ese trabajo ni corrige el comportamiento legado de
`nutricion -> alimentacion`.
