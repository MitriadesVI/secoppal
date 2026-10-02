# Instrucciones para el juez

Eres un lector independiente. Evalúas casos de un asistente conversacional sobre contratación pública colombiana
(SECOP). No uses LLM auxiliares ni herramientas externas, no consultes red ni otros archivos del directorio, y no
intentes adivinar qué se espera de ti: sólo juzga lo que ves en cada caso.

## Qué lees

Sólo `judge_input.jsonl` (una línea JSON por caso). No abras `cases.jsonl`, `BUILD_REPORT.md` ni el script. Los casos
vienen en orden mezclado; júzgalos de forma independiente, uno por uno.

Campos de cada caso:

| campo | significado |
|---|---|
| `case_id` | identificador; cópialo tal cual a tu salida |
| `query_text` | lo que escribió el usuario |
| `universe` | qué se busca (CONTRACTS = contratos firmados, PROCESSES = procesos de contratación) y la vista |
| `applied_filters` | criterios de la consulta que el sistema aplicaría tal cual |
| `not_applied` | criterios de la consulta que el sistema no aplica como los pidió el usuario |
| `answer_scope_shown` | lo que el sistema usaría en su lugar para esos criterios |
| `notice_shown_to_user` | la línea que vería el usuario encima de la respuesta |

El sistema produciría una lista de resultados (contratos o procesos) con esos filtros. Tú no ves la lista; juzga a partir de
la consulta, los filtros y el aviso.

## Qué decides, por caso

**(a) `condition_role`: `DECISIVE`, `SECONDARY` o `UNCLEAR`.** Piensa en los criterios de `not_applied`:

- `DECISIVE`: sin ese criterio la respuesta no le sirve al usuario para lo que pidió; es lo que define su intención.
- `SECONDARY`: el criterio es un refinamiento; sin él la respuesta sigue respondiendo lo esencial de lo que el usuario quería.
- `UNCLEAR`: con el texto de la consulta no se puede saber cuál de las dos.

Si hay más de un criterio en `not_applied`, responde `DECISIVE` si al menos uno lo es; si ninguno lo es pero alguno es incierto,
`UNCLEAR`; si todos son refinamientos, `SECONDARY`. Juzga el criterio por lo que el usuario escribió, no por lo que el sistema
muestre en su lugar.

**(b) `answer_useful`: `yes` o `no`.** ¿Al usuario real le sirve más recibir la respuesta con ese aviso que recibir una
pregunta de aclaración antes de ver nada? Decide como si fueras ese usuario, con su consulta, leyendo el aviso. Responde `yes` sólo si
la respuesta con aviso le resulta de más ayuda que la pregunta; `no` si la pregunta le habría servido más o si la respuesta
lo llevaría a una conclusión equivocada o inútil. Si el aviso es confuso o no se entiende, tenlo en cuenta.

**(c) `reason`: una línea** (máximo 200 caracteres) que justifique ambas decisiones. No cites datos personales; usa el
`case_id` si necesitas referirte al caso.

## Reglas de juicio

- Juzga cada caso con la información de ese caso. No compares casos entre sí ni mantengas una cuota.
- Un criterio no es decisivo ni secundario por su tipo (lugar, fecha, estado, monto, orden): depende de la consulta concreta.
- Si el aviso afirma algo falso o engañoso sobre lo que se muestra, cuenta como motivo para `answer_useful: no`.
- No inventes datos del mundo real; no hay conteos de resultados disponibles.
- Si no puedes decidir, usa `UNCLEAR` en (a); (b) siempre debe ser `yes` o `no`.

## Formato de salida

Un archivo `judgments.jsonl` (o `judgments_<tu_nombre>.jsonl` si hay varios jueces), una línea JSON por caso, exactamente con estas claves
y valores, todos los casos de `judge_input.jsonl`, sin claves extra:

```
{"case_id": "R0000", "condition_role": "DECISIVE", "answer_useful": "no", "reason": "una línea"}
```

- `condition_role` ∈ {`DECISIVE`, `SECONDARY`, `UNCLEAR`}
- `answer_useful` ∈ {`yes`, `no`}
- `reason`: cadena de una línea, sin saltos de línea.

Escribe sólo ese archivo, en el directorio del scratchpad indicado por quien te lanza.
