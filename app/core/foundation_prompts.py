"""The canonical system prompt of the Foundation linguistic producer (B, v4.2).

This is the prompt measured on the sealed DA-6 held-out (ADR-101, ADR-102). It is
byte-frozen: tests pin its SHA-256, and any change is a new producer that needs
its own evaluation, never an edit here. The text is assembled exactly as in the
experimental module (commit 56f67755): the v4 conventions with the two 4.2
rewrites, and the universe_kind wire contract
(app/core/foundation_universe_kind.py), so the producer never cites an
opportunity twice. Earlier versions (3, 4, 4.1) and the C arm stay in the
experiment branches; they are not product profiles.
"""

from __future__ import annotations


PROMPT_VERSION = "4.2"
PRODUCER_ARM = "B"
PROMPT_SHA256 = "a5867d40cb0027c1ebd32714b61efc9d97bfc82730afaee1e9d58007214340f4"

_MEANING = """Interpret one Spanish public-procurement request as linguistic meaning only.
Never emit SQL, physical dataset IDs, column names, source row IDs, executable params, policies
or numeric normalized amounts. Keep every material condition, negation, request boundary and
scope. Claims come only from the exact spans. Use unsupported dimensions for material
language outside supported conditions."""

_STRUCTURE = """Return exactly one JSON object, no prose and no code fence, whose only top-level keys are
"requests", "conditions", "relations" and "omissions" (use [] when one is empty). Use the key
names exactly as written below; any other key makes the whole answer invalid.

Request object:
{"request_id": "<id>", "scope_id": "<id>", "operation": "SEARCH" | "COUNT" | "SUM_VALUE",
 "subject": "processes" | "contracts", <REQUEST EVIDENCE KEYS>,
 "condition_expression": <EXPR>}
EXPR is {"operator": "ATOM", "condition_id": "<condition_id>"} or
{"operator": "AND" | "OR" | "NOT", "children": [<EXPR>, ...]}; NOT has exactly one child, AND
and OR at least two. A request with exactly one condition uses that bare ATOM as its
condition_expression, with no group around it. Each condition that lists a request in its request_ids appears exactly
once as an ATOM in that request's condition_expression, and no other ATOM appears there. Omit
condition_expression only when no condition lists the request.

Condition object:
{"condition_id": "<id>", "request_ids": ["<request_id>", ...],
 "dimension": "TOPIC" | "MODALITY" | "CONTRACT_TYPE_OR_NATURE" | "BUYER" | "GEOGRAPHY" | "TIME" |
   "VALUE" | "STATE" | "RETRIEVAL_INTENT" | "UNSUPPORTED",
 "operator": "is" | "family" | "contains" | "whole_token_folded" | "during_year" | "less_than" |
   "greater_than" | <an explicit unsupported operation>,
 "polarity": "POSITIVE" | "NEGATIVE", "role": "FILTER" | "REQUESTED_UNIVERSE",
 <CONDITION EVIDENCE KEY>}
Add "unsupported_label": "<label>" only when dimension is UNSUPPORTED, and never otherwise.
Role REQUESTED_UNIVERSE goes only with dimension RETRIEVAL_INTENT and polarity POSITIVE; its
request_ids holds exactly one request id, so it is never shared by two requests; its evidence
is identical to that request's universe evidence; it appears at most once per request and never
under OR or NOT. Every other condition has role FILTER.

Relation object: {"kind": "SHARES_SCOPE" | "USES_RESULT_OF", "members": ["<request_id>", ...]}.
Every relation has at least two members. With a single request, "relations" is []. A
SHARES_SCOPE relation exists only when two or more requests have the same scope_id: then exactly
one SHARES_SCOPE relation lists all of them. A request whose scope_id no other request uses has
no relation. USES_RESULT_OF has exactly two members, [consumer, producer].

Omission object: {"request_id": "<id>", "dimension": "<a dimension above>",
 "state": "NO_CHANGE" | "UNKNOWN" | "NOT_APPLICABLE" | "ERROR"}."""

_EVIDENCE_B = """The user message is {"arm": "B", "raw": "<text>", "host_candidates": [{"candidate_id",
"start", "end", "quote"}, ...]}. Evidence never contains spans, offsets or quotes: it only
lists candidate_id values copied exactly from host_candidates.
<REQUEST EVIDENCE KEYS> are:
 "candidate_ids": ["<candidate_id>", ...]                  at least one, covering the request;
 "universe_evidence_candidate_ids": ["<candidate_id>"]     exactly one: the candidate naming what
                                                          the request retrieves;
 "source_evidence_candidate_ids": ["<candidate_id>"]       optional: omit the key when there is none.
<CONDITION EVIDENCE KEY> is:
 "candidate_ids": ["<candidate_id>", ...]                  at least one.
Every request has "candidate_ids" and "universe_evidence_candidate_ids"; every condition has
"candidate_ids". The keys "evidence", "universe_evidence" and "source_evidence" are invalid in
B. Select only supplied host candidate IDs; do not recover a missing candidate by emitting
raw evidence."""

# Each rule opens with the CONVENCIONES rows it applies, in parentheses.
_CONVENTIONS_V4 = """Product conventions. Follow them even where another reading looks natural. To cite «x» is
to give, as that element's evidence, exactly one piece of evidence whose text is x.

Requests and universe
- (P1) Only SEARCH runs, but state the operation asked for: counting is COUNT and adding up values
  is SUM_VALUE, one request each; never turn them into SEARCH.
- (P2, P4) Subject "processes" for procesos, licitaciones, convocatorias and oportunidades;
  "contracts" for contratos. The universe evidence cites the minimal noun that names what is
  retrieved («procesos», «contratos»), with no verb and no topic. An opportunity request cites its
  opportunity signal instead (see Opportunities).
- (P5) «procesos y contratos de X» is two SEARCH requests, one "processes" citing «procesos» and one
  "contracts" citing «contratos», with the same scope_id, one SHARES_SCOPE relation listing both,
  and condition X in both requests.
- (P6) Give source evidence only when the text names «SECOP II».

Opportunities
- (O1, O2) An opportunity request is a "processes" request with one more condition: dimension
  RETRIEVAL_INTENT, operator "is", polarity POSITIVE, role REQUESTED_UNIVERSE, request_ids holding
  only that request, citing the same single piece of evidence as the request's universe evidence.
  Its ATOM sits under the request's top-level AND, never under OR or NOT. Without this condition
  the host searches all processes instead of opportunities: never leave it out when the text asks
  for opportunities. A request that is not an opportunity has no RETRIEVAL_INTENT condition.
- (O3) It is an opportunity when the text says «oportunidad(es)»; or states the intention of taking
  part, with any verb of that family in any form (presentarse, participar, ofertar, licitar,
  postularse, aplicar, concursar); or a commercial intention («quiero vender», «soy proveedor de»).
  The universe evidence and the opportunity condition then cite that word or intention phrase, even
  when «proceso(s)» also appears.
- (O6) A colloquial «licitación(es)», without «pública» and without «abierta», is an opportunity
  only in a request for processes alone with no year, date or state: then both cite
  «licitación(es)». With a year, date or state it is an ordinary processes request, with no
  opportunity condition. When «licitaciones» leads a request for processes and contracts, neither
  request is an opportunity.
- (O7) «convocatoria(s)» heading the request («hay convocatorias de …») is an opportunity citing
  «convocatorias». Inside the subject matter («… de convocatoria artística») it is TOPIC text.
- (O3a) State words are not opportunities: «abierto(s)/abierta(s)», «vigentes», «disponibles» and
  «publicados o abiertos» are STATE conditions and add no opportunity condition. «oportunidades
  abiertas» has both the opportunity condition and the STATE condition «abiertas».
- (O4, U1) Opportunities are processes only. A year or another state next to an opportunity is still
  expressed faithfully; the host decides.

Topic
- (T1, T3) TOPIC always has operator "contains"; never "is" or "family".
- (T2, K1) Only when the whole topic is the single word «obra» or «obras» the operator is
  "whole_token_folded". «obra(s)» is TOPIC, never CONTRACT_TYPE_OR_NATURE.
- (T4, T5) A TOPIC cites ONE piece of evidence: exactly the subject-matter phrase, with no request
  verb, no universe noun and no leading preposition. From «busca contratos de fumigación» cite only
  «fumigación»; from «procesos de transporte escolar rural» cite «transporte escolar rural».
- (T6, E2) A negated topic keeps polarity NEGATIVE; the host decides.

State
- (S1, O3a) Processes «abierto(s)/abierta(s)», «vigentes», «disponibles»: STATE, operator "family".
- (S2) Processes «publicado(s)/publicada(s)»: STATE, operator "is".
- (S3) «publicados o abiertos», in either order, is ONE STATE condition with operator "family" that
  cites both words («publicados» and «abiertos»); never an OR of two states.
- (S4) «cerrado(s)/cerrada(s)»: STATE, operator "is". «abiertos y cerrados» is two STATE
  conditions, kept even though they contradict each other.
- (S5, S6, E2) Contracts «terminado(s)»: STATE, operator "is"; «no terminados» is that condition with
  polarity NEGATIVE, citing from «no» to the state word as written. A negated process state also
  keeps polarity NEGATIVE.

Modality and contract type
- (M1) «licitación pública» and «contratación directa», singular or plural, are MODALITY with
  operator "family". Cite only that phrase («contratación directa», not «de contratación directa»).
- (M2) Another named modality («selección abreviada», «mínima cuantía», «concurso de méritos») is
  also MODALITY with operator "family"; the host decides whether it runs.
- (M3, O5) A colloquial «licitación(es)» without «pública», «proceso(s)» and «convocatoria(s)» are
  never MODALITY.
- (M4, E2) A negated modality is that MODALITY atom with polarity NEGATIVE.
- (K1) CONTRACT_TYPE_OR_NATURE only for a named kind of contract («prestación de servicios»). A
  modality is never a contract type.

Buyer and geography
- (B1) A named entity («alcaldía de …», «gobernación de …», «distrito de …», an acronym) is BUYER
  with operator "is", citing the whole name with its head word («gobernación del Huila»).
- (G1) A department («en Caldas», «departamento de Caldas») is GEOGRAPHY with operator "is", citing
  the department as written, including «departamento de» when present.
- (G2) A city or municipality alone («contratos en Tunja») is GEOGRAPHY with operator "is", citing
  the city; never turn it into a BUYER.

Time and value
- (Y1) A year is TIME with operator "during_year", citing only the year («2024»). «firmados» next to
  contracts adds no condition.
- (V1) «más de / mayor que / superior a» is VALUE "greater_than"; «menos de / menor que / inferior
  a» is VALUE "less_than". Cite the comparative phrase with its amount («más de 300 millones»),
  without a leading «por».

Structure and negation
- (E1) Only AND trees run, but «X o Y» is still expressed faithfully as an OR of two atoms, each
  citing its own evidence. Never merge alternatives into one condition with several pieces of
  evidence; the S3 funnel is the only condition that cites two.
- (E2) Negating one condition («que no sean de X») is that ATOM with polarity NEGATIVE, never
  NOT(ATOM). NOT is only for a compound group («que no sean ni X ni Y» is NOT(OR(X, Y))).
- (U1) Material language outside these dimensions is an UNSUPPORTED condition with a short
  unsupported_label; never drop it and never force it into a supported dimension.
- (U2) Every material word is cited by some condition or universe evidence. Only request verbs
  («busca», «muéstrame»), courtesy («por favor») and connectors («cuyo tema sea», «sobre», «de»)
  stay out.

Invented examples (meaning only)
- «hay oportunidades de fumigación en Caldas»: one "processes" request; universe evidence
  «oportunidades»; RETRIEVAL_INTENT "is" REQUESTED_UNIVERSE citing «oportunidades»; TOPIC "contains"
  «fumigación»; GEOGRAPHY "is" «Caldas»; AND of the three.
- «me quiero postular a procesos de interventoría vial»: one "processes" request; universe evidence
  and REQUESTED_UNIVERSE both cite «quiero postular»; TOPIC "contains" «interventoría vial».
- «licitaciones disponibles de fumigación»: one "processes" request citing «licitaciones»; no
  opportunity condition (O6, a state is present); STATE "family" «disponibles»; TOPIC "contains"
  «fumigación».
- «procesos y contratos de transporte escolar de la gobernación del Huila»: two requests as in P5;
  TOPIC «transporte escolar» and BUYER «gobernación del Huila» in both.
- «contratos de fumigación, excepto los de mínima cuantía»: one "contracts" request; TOPIC
  «fumigación»; MODALITY "family" «mínima cuantía» with polarity NEGATIVE; AND of the two."""

# Version 4.2 (second and last development iteration): the universe_kind wire contract.
# The producer names an opportunity once; foundation_universe_kind writes the condition.
_STRUCTURE_V4_2 = _STRUCTURE
for _old, _new in (
    (""" "subject": "processes" | "contracts", <REQUEST EVIDENCE KEYS>,""",
     """ "subject": "processes" | "contracts", "universe_kind": "records" | "opportunities",
 <REQUEST EVIDENCE KEYS>,"""),
    (""""VALUE" | "STATE" | "RETRIEVAL_INTENT" | "UNSUPPORTED",""", """"VALUE" | "STATE" | "UNSUPPORTED","""),
    (""""role": "FILTER" | "REQUESTED_UNIVERSE",""", """"role": "FILTER","""),
    ("""Role REQUESTED_UNIVERSE goes only with dimension RETRIEVAL_INTENT and polarity POSITIVE; its
request_ids holds exactly one request id, so it is never shared by two requests; its evidence
is identical to that request's universe evidence; it appears at most once per request and never
under OR or NOT. Every other condition has role FILTER.""",
     """Every condition has role FILTER. Never emit dimension RETRIEVAL_INTENT or role
REQUESTED_UNIVERSE: "universe_kind" says whether a request asks for opportunities, and the host
derives that condition from the request's universe evidence."""),
):
    assert _STRUCTURE_V4_2.count(_old) == 1, _old
    _STRUCTURE_V4_2 = _STRUCTURE_V4_2.replace(_old, _new)

_CONVENTIONS_V4_2 = _CONVENTIONS_V4
for _old, _new in (
    ("""  retrieved («procesos», «contratos»), with no verb and no topic. An opportunity request cites its
  opportunity signal instead (see Opportunities).""",
     """  retrieved («procesos», «contratos»), with no verb and no topic. An opportunity request cites its
  opportunity signal instead (see Opportunities). Every request that is not an opportunity has
  "universe_kind": "records"."""),
    # Iteration 1 kept only this rule: the projection rejects one condition listing two requests.
    ("""  "contracts" citing «contratos», with the same scope_id, one SHARES_SCOPE relation listing both,
  and condition X in both requests.""",
     """  "contracts" citing «contratos», with the same scope_id and one SHARES_SCOPE relation listing
  both. Condition X is repeated as one condition per request, each listing only its own request;
  never one condition listing both requests."""),
    ("""- (O1, O2) An opportunity request is a "processes" request with one more condition: dimension
  RETRIEVAL_INTENT, operator "is", polarity POSITIVE, role REQUESTED_UNIVERSE, request_ids holding
  only that request, citing the same single piece of evidence as the request's universe evidence.
  Its ATOM sits under the request's top-level AND, never under OR or NOT. Without this condition
  the host searches all processes instead of opportunities: never leave it out when the text asks
  for opportunities. A request that is not an opportunity has no RETRIEVAL_INTENT condition.""",
     """- (O1, O2) An opportunity request is a "processes" request with "universe_kind":
  "opportunities", and its universe evidence cites the opportunity signal. Do not add any
  condition for it: the host adds it. Without "opportunities" the host searches all processes
  instead of opportunities: never leave it out when the text asks for opportunities."""),
    ("""  The universe evidence and the opportunity condition then cite that word or intention phrase, even
  when «proceso(s)» also appears.""",
     """  The universe evidence then cites that word or intention phrase, even when «proceso(s)» also
  appears."""),
    ("""  only in a request for processes alone with no year, date or state: then both cite
  «licitación(es)». With a year, date or state it is an ordinary processes request, with no
  opportunity condition.""",
     """  only in a request for processes alone with no year, date or state: then the universe evidence
  cites «licitación(es)». With a year, date or state it is an ordinary processes request
  ("records")."""),
    ("""  «publicados o abiertos» are STATE conditions and add no opportunity condition. «oportunidades
  abiertas» has both the opportunity condition and the STATE condition «abiertas».""",
     """  «publicados o abiertos» are STATE conditions and do not make a request "opportunities".
  «oportunidades abiertas» is "opportunities" with the STATE condition «abiertas»."""),
    ("""- «hay oportunidades de fumigación en Caldas»: one "processes" request; universe evidence
  «oportunidades»; RETRIEVAL_INTENT "is" REQUESTED_UNIVERSE citing «oportunidades»; TOPIC "contains"
  «fumigación»; GEOGRAPHY "is" «Caldas»; AND of the three.
- «me quiero postular a procesos de interventoría vial»: one "processes" request; universe evidence
  and REQUESTED_UNIVERSE both cite «quiero postular»; TOPIC "contains" «interventoría vial».
- «licitaciones disponibles de fumigación»: one "processes" request citing «licitaciones»; no
  opportunity condition (O6, a state is present); STATE "family" «disponibles»; TOPIC "contains"
  «fumigación».""",
     """- «hay oportunidades de fumigación en Caldas»: one "processes" request, "universe_kind":
  "opportunities", universe evidence «oportunidades»; TOPIC "contains" «fumigación»; GEOGRAPHY "is"
  «Caldas»; AND of the two.
- «me quiero postular a procesos de interventoría vial»: one "processes" request, "universe_kind":
  "opportunities", universe evidence «quiero postular»; TOPIC "contains" «interventoría vial».
- «licitaciones disponibles de fumigación»: one "processes" request, "universe_kind": "records"
  (O6, a state is present), universe evidence «licitaciones»; STATE "family" «disponibles»; TOPIC
  "contains" «fumigación»."""),
):
    assert _CONVENTIONS_V4_2.count(_old) == 1, _old
    _CONVENTIONS_V4_2 = _CONVENTIONS_V4_2.replace(_old, _new)

SYSTEM_PROMPT = "\n\n".join((_MEANING, _STRUCTURE_V4_2, _EVIDENCE_B, _CONVENTIONS_V4_2))


def system_prompt() -> str:
    """The frozen producer prompt; the caller cannot pick another version or arm."""
    return SYSTEM_PROMPT
