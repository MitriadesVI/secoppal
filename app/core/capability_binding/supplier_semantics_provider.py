"""The only I/O of the polarity sidecar, kept apart so the contract stays pure.

Everything this module can fail at -- network, timeout, quota, malformed JSON --
resolves to ``None``, which the contract adjudicates as UNAVAILABLE and which
blocks the turn exactly like a veto. A sidecar that is down withholds authority;
it never returns it to the vocabulary that was wrong in the first place.
"""
from __future__ import annotations

import json
import logging

from app.core.capability_binding.supplier_semantics import MentionRef, QuantityRef

logger = logging.getLogger(__name__)

# Frozen with experiment a2-supplier-semantics-x2. The polarity half is the
# wording measured in x1; the cardinality half is new, so the x1 gates do not
# carry over and are re-measured against both.
PROMPT = """Eres un intérprete de semántica. Recibes el texto de una consulta escrita por
una persona, una lista de menciones ya localizadas en ese texto y, a veces, una
lista de cantidades ya localizadas en ese mismo texto.

No busques menciones ni cantidades: las listas que recibes son completas y
definitivas. Tu tarea es sólo interpretar qué dice la frase sobre ellas.

PARTE 1 — polaridad de cada mención

Para cada mención de la lista, decide si la persona quiere ver eso o si lo está
excluyendo:

- POSITIVE: la persona quiere ver esa mención; forma parte de lo que pide.
- NEGATED: la persona excluye esa mención; pide explícitamente no verla.
- MIXED: esa misma mención está a la vez incluida y excluida.
- UNKNOWN: el texto no permite decidirlo.

Reglas:

1. Decide por alcance, no por cercanía. Una palabra de negación puede estar en
   la frase sin afectar a la mención: puede negar otra cosa, formar parte de una
   expresión hecha, o describir una característica. Pregúntate qué queda
   excluido de lo que la persona pide, no qué palabras hay cerca.
2. Cada mención se juzga por separado. En un mismo texto una mención puede ser
   POSITIVE y otra NEGATED.
3. Dos menciones de la misma palabra son menciones distintas. Se distinguen por
   su número de aparición, contado desde el principio del texto.
4. Ante la duda, responde UNKNOWN. UNKNOWN es una respuesta aceptable y
   preferible a adivinar.
5. Devuelve exactamente una clasificación por cada mención recibida: ni una más,
   ni una menos. No las fusiones. Copia "quote" y "occurrence" tal como los
   recibiste, sin cambiarlos.

PARTE 2 — relación con la cantidad

Sólo si recibes cantidades. Señala UNA cantidad de la lista por su
"quantity_ref" y di qué relación establece el texto entre esa cantidad y las
menciones:

- EXACT: la persona pide esa cantidad exacta.
- LOWER_BOUND: pide esa cantidad o más ("más de", "al menos", "superior a").
- UPPER_BOUND: pide esa cantidad o menos ("menos de", "hasta", "a lo sumo").
- RANGE: la cantidad es extremo de un intervalo.
- UNKNOWN: el texto no permite decidirlo.

Reglas:

6. No inventes cantidades. Sólo puedes señalar un "quantity_ref" de la lista
   que recibiste. Si ninguna encaja, usa UNKNOWN señalando la primera.
7. El número que acompaña a una entidad, una fecha o un importe no es una
   cantidad de menciones. Si la cantidad recibida no cuenta menciones, responde
   UNKNOWN.
8. Ante la duda, UNKNOWN.

No produzcas nada más: ni entidades, ni fechas, ni filtros, ni temas, ni
consultas, ni explicaciones.

Responde únicamente con un objeto JSON con esta forma exacta:

{"mentions": [{"quote": "...", "occurrence": 1, "polarity": "..."}],
 "cardinality": {"quantity_ref": 1, "relation": "..."}}

Si no recibes cantidades, omite por completo la clave "cardinality".

Texto de la consulta:
{{QUERY}}

Menciones a clasificar:
{{REFS}}

Cantidades localizadas:
{{QUANTITIES}}
"""


def render_prompt(
    user_query: str,
    refs: tuple[MentionRef, ...],
    quantities: tuple[QuantityRef, ...] = (),
) -> str:
    listed = "\n".join(
        f'- quote: "{ref.quote}", occurrence: {ref.occurrence}' for ref in refs
    )
    counted = "\n".join(
        f'- quantity_ref: {q.quantity_ref}, quote: "{q.quote}", value: {q.value}'
        for q in quantities
    ) or "(ninguna)"
    return (
        PROMPT.replace("{{QUERY}}", user_query)
        .replace("{{REFS}}", listed)
        .replace("{{QUANTITIES}}", counted)
    )


def classify(
    user_query: str,
    refs: tuple[MentionRef, ...],
    quantities: tuple[QuantityRef, ...] = (),
    *,
    api_key: str,
    model: str,
    timeout_seconds: float,
    base_url: str = "https://api.deepseek.com/v1",
) -> dict | None:
    """The sidecar's raw answer, or ``None`` if it could not be obtained."""
    try:
        from openai import OpenAI

        # max_retries=0 on purpose: the SDK default is 2, which would turn the
        # configured timeout into three of them on the synchronous EXECUTE
        # path. app/core/natural_alpha/provider.py pins it for the same reason.
        client = OpenAI(
            api_key=api_key, base_url=base_url,
            timeout=timeout_seconds, max_retries=0,
        )
        response = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[{"role": "user",
                       "content": render_prompt(user_query, refs, quantities)}],
        )
        payload = json.loads(response.choices[0].message.content or "")
    except Exception as exc:  # noqa: BLE001 — every failure blocks the turn
        logger.warning("supplier polarity sidecar unavailable: %s", exc)
        return None
    return payload if isinstance(payload, dict) else None
