# ADR-069 — Una forma lingüística que una autoridad no resuelve no se degrada en silencio

> **Registro canónico.** Esta decisión está registrada en
> [`docs/decisions_log.md`](../decisions_log.md), índice canónico de ADR del
> repositorio. La autoridad *as-built* es
> `docs/audits/2026-08-26_HCA_FRESH_SYSTEMIC_REMEDIATION.md`.

**Estado:** Accepted
**Fecha:** 2026-08-26
**Rama:** `feature/hca-fresh-semantic-surface-safety`

```text
AS_BUILT_AUTHORITY:
  docs/audits/2026-08-26_HCA_FRESH_SYSTEMIC_REMEDIATION.md
```

## Contexto

La adjudicación causal de los hallazgos fresh
([`2026-08-26_FRESH_FINDINGS_CAUSALITY.md`](../audits/2026-08-26_FRESH_FINDINGS_CAUSALITY.md))
demostró que las siete causas raíz declaradas eran siete síntomas de un solo
mecanismo:

```text
ENUMERATED_SURFACE_FORM_VOCABULARY_WITH_FAIL_OPEN_DEFAULT
```

Cada dimensión semántica —tema, estado, control, dinero, intención— se reconoce
con una lista de formas superficiales mantenida a mano. Un token ausente de su
lista no produce error ni aclaración: cae al destino por defecto o desaparece, y
el sistema construye y ejecuta una consulta **bien formada y semánticamente
equivocada**.

Lo decisivo no es que hubiera huecos —siempre los habrá—, sino **qué pasa
cuando se encuentra uno**. El comportamiento por defecto era continuar. Por eso
el mismo lema con distinta flexión producía comportamientos opuestos: `compra`
estaba catalogado y `compró` no; `participar` estaba y `licitar` no; la escala
`millones` contaba como evidencia monetaria y `mil` no.

Y por eso la peor manifestación no fue perder una restricción sino **invertirla**:
`procesos que no estén abiertos` devolvía exactamente y solo los abiertos. Un
fail-open entrega resultados de más y el usuario puede notarlo; un fail-inverted
entrega el complemento exacto de lo pedido y se ve perfectamente plausible.

## Decisión

**INV-NO-SILENT-SEMANTIC-FALLBACK.** Una forma lingüística materialmente
estructural que una autoridad no puede resolver no puede degradarse en silencio
a otra dimensión. En particular, no puede: convertirse en objeto, desaparecer,
invertir su polaridad, activar una dimensión fabricada, ni producir un filtro
parcial.

El invariante es común; las autoridades siguen separadas. No se crea un gestor
semántico global. Lo que cambia es la disciplina con que cada autoridad trata su
propio hueco:

1. **Reconocimiento y semántica son capas distintas.**
   `app/core/state_vocabulary.py` posee el reconocimiento léxico del estado y se
   **construye** desde los catálogos semánticos en vez de escribirse a mano, así
   que no puede quedarse atrás cuando uno de ellos crece. La semántica sigue en
   `estado_families`, la proyección en `ESTADO_FAMILIES` y `app/data/estados.py`.

2. **La polaridad se resuelve antes que cualquier extractor positivo.**
   `_seal_negated_state` garantiza una propiedad, no un catálogo: ninguna forma
   de estado bajo el alcance de un marcador negativo llega al extractor
   positivo, sea cual sea la razón por la que quedó libre. Y una siembra de
   estado hecha sobre texto crudo —el atajo de oportunidades, el vocabulario de
   intención— se revoca si la negación se comió su span.

3. **Un rango explícito se aplica entero o pide precisión.**
   `INV-MONEY-RANGE-ATOMIC`. Con dos extremos declarados: dos resueltos,
   ejecuta; uno, bloquea; ninguno, comportamiento normal de no resuelto. Nunca
   una cota unilateral silenciosa.

4. **Una corrección ortográfica no puede crear una dimensión semántica.**
   `INV-SPELL-CORRECTION-CANNOT-CREATE-SEMANTIC-DIMENSION`. El lenguaje
   funcional se protege por identidad **antes** de que el corrector actúe, no
   después: una capa que reescribe el texto deja a las defensas de la siguiente
   apuntando a un texto que ya no existe.

5. **Las familias lingüísticas se declaran una vez y todas sus ramas se
   construyen desde ellas** — verbos de participación, acciones de compra,
   gramática de paginación y de reinicio. Una rama no puede quedarse corta
   respecto de las demás porque no hay dos listas que sincronizar.

6. **El criterio que separa control de contenido es morfológico donde puede
   serlo.** Un verbo conjugado no es un objeto contractual en ninguna lectura
   del español; una nominalización sí puede serlo. Tratarlos igual producía
   fallos opuestos según qué más hubiera en la frase.

7. **La regla se vigila estáticamente donde el lenguaje lo permite.** `F601`
   entra al `select` de `ruff.toml`: la clave duplicada de `NATURAL_TO_FAMILY`
   —que proyectaba el complemento del estado pedido sin que nada fallara— era
   detectable por el linter y la puerta la había dejado fuera.

## Consecuencias y límites

**Fallar cerrado cuesta cobertura, a propósito.** `entre 500 mil y 3 millones`
antes ejecutaba con un techo; ahora pide precisión. Se prefiere una pregunta a
un universo que el usuario no pidió y no puede ver.

**El invariante no promete cerrar todos los huecos de vocabulario.** Promete que
un hueco produzca una pregunta y no una consulta equivocada. Los catálogos
seguirán creciendo; lo que no vuelve es el comportamiento por defecto.

**La vigencia deja de ser un estado.** `vigente`, `activo` y `disponible` no
nombran ningún valor de SECOP: dicen que la cosa está viva ahora, y qué es estar
vivo depende del dataset. Se declara como familia con proyección por dataset, no
como dos entradas del mismo literal en el mismo dict.

**No se introduce LLM-as-parser, ni embeddings, ni una dependencia NLP.** Nada
de lo anterior lo necesitaba: el paradigma copulativo es morfología cerrada, y
las gramáticas de control se componen de sus partes en vez de enumerar frases.

**Queda fuera, declarado:** la exclusión ejecutable de estado. Dimensional
Negation V1 la dejó como no soportada y este sprint no la implementa; una
negación de estado aclara.

## Evidencia de aceptación

- **Gate predictivo 12/12.** Las doce consultas derivadas del mecanismo antes de
  escribir runtime, que no están en ningún corpus, dejan de manifestarlo. Sus
  expectativas no se tocaron y la línea base congelada se verifica por SHA;
- **Gate anti-vocabulario.** Las pruebas recorren las familias declaradas
  completas —las ~90 formas de estado, las 390 palabras del lenguaje funcional,
  los verbos de participación y de compra—, no las formas observadas. La prueba
  generativa encontró una fuga que los nueve escenarios nunca mostraron:
  `procesos que no estén en convocatoria` sembraba la familia contraria;
- **Canario vivo contra SECOP:** `semantic_contract_violations = 0`,
  `hard_filter_violations = 0`. La contradicción original —familia
  `oferta_abierta` proyectando `Cerrado`/`Seleccionado`— ya no se produce;
- **HCA canónico 188/195, 0 rotos**, corpus byte-idéntico;
- **Plan fresh congelado 30 → 37 de 52**, sin regresiones, sin tocar el plan;
- `F601`: 3 hallazgos antes, 0 después, con la regla ya dentro del gate.
