"""Morphological variants dictionary for SECOP query expansion.

Maps surface forms (as typed by users) to the canonical root term that
will be sent to the SoQL builder as the ``objeto`` search token.

Structure:
  MORPHOLOGICAL_VARIANTS: dict[str, str]
    key  = variant as typed by user (normalized, no accents needed in
           query_router since normalize_text() is applied first)
    value = root term used in SoQL LIKE clause

Derived constants (built at import time):
  VARIANT_TO_ROOT: dict[str, str]   -- O(1) lookup alias for MORPHOLOGICAL_VARIANTS
  KNOWN_BIGRAMS: frozenset[tuple[str, str]]  -- all 2-word keys as tuples

Guidelines for adding entries:
- Key: lowercased, accent-stripped form (as produced by normalize_text())
- Value: the shortest, most common singular root that appears in SECOP
- Include plurals, feminines, and common abbreviations
- Bigrams: use space-separated key; will auto-appear in KNOWN_BIGRAMS
"""
from __future__ import annotations

MORPHOLOGICAL_VARIANTS: dict[str, str] = {
    # ── mantenimiento / mantenimientos ──────────────────────────────────────
    "mantenimiento": "mantenimiento",
    "mantenimientos": "mantenimiento",
    # B6: frases multi-token muy frecuentes que el parser troceaba en AND
    # de tokens individuales — reducía drásticamente el recall.
    # Se mapean al root "mantenimiento" para que el SoQL emita un único
    # LIKE expandido en lugar de N LIKE encadenados con AND.
    "mantenimiento correctivo y preventivo": "mantenimiento",
    "mantenimiento preventivo y correctivo": "mantenimiento",
    "mantenimiento correctivo": "mantenimiento",
    "mantenimiento preventivo": "mantenimiento",
    "parque automotor": "parque_automotor",
    "parques automotores": "parque_automotor",
    # ── vial / vias / malla vial ─────────────────────────────────────────────
    "vial": "vial",
    "vias": "vias",
    "vía": "vial",
    "vías": "vial",
    "malla vial": "vial",
    "infraestructura vial": "vial",

    # ── dotacion / dotaciones ───────────────────────────────────────────────
    "dotacion": "dotacion",
    "dotaciones": "dotacion",
    # ── pavimentacion ───────────────────────────────────────────────────────
    "pavimentacion": "pavimentacion",
    "pavimentaciones": "pavimentacion",
    "pavimento": "pavimentacion",
    "pavimentos": "pavimentacion",
    # ── construccion ────────────────────────────────────────────────────────
    "construccion": "construccion",
    "construcciones": "construccion",
    # ── adecuacion ──────────────────────────────────────────────────────────
    "adecuacion": "adecuacion",
    "adecuaciones": "adecuacion",
    # ── suministro ──────────────────────────────────────────────────────────
    "suministro": "suministro",
    "suministros": "suministro",
    # ── consultoria ─────────────────────────────────────────────────────────
    "consultoria": "consultoria",
    "consultorias": "consultoria",
    # ── interventoria ───────────────────────────────────────────────────────
    "interventoria": "interventoria",
    "interventorias": "interventoria",
    # ── rehabilitacion ──────────────────────────────────────────────────────
    "rehabilitacion": "rehabilitacion",
    "rehabilitaciones": "rehabilitacion",
    # ── reparacion ──────────────────────────────────────────────────────────
    "reparacion": "reparacion",
    "reparaciones": "reparacion",
    # ── señalizacion ────────────────────────────────────────────────────────
    "senalizacion": "senalizacion",
    "senalizaciones": "senalizacion",
    # ── aseo / aseos ────────────────────────────────────────────────────────
    "aseo": "aseo",
    "aseos": "aseo",
    # ── vigilancia ──────────────────────────────────────────────────────────
    "vigilancia": "vigilancia",
    "vigilancias": "vigilancia",
    # ── acueducto ───────────────────────────────────────────────────────────
    "acueducto": "acueducto",
    "acueductos": "acueducto",
    # ── alcantarillado ───────────────────────────────────────────────────────
    "alcantarillado": "alcantarillado",
    "alcantarillados": "alcantarillado",
    # ── capacitacion ────────────────────────────────────────────────────────
    "capacitacion": "capacitacion",
    "capacitaciones": "capacitacion",
    # ── transporte ──────────────────────────────────────────────────────────
    "transporte": "transporte",
    "transportes": "transporte",
    # ── acrónimos/programas gubernamentales ──────────────────────────────────
    "pae": "alimentacion_escolar",
    "alimentacion escolar": "alimentacion_escolar",
    "programa de alimentacion escolar": "alimentacion_escolar",
    "complemento alimentario": "alimentacion_escolar",
    "restaurante escolar": "alimentacion_escolar",
    "restaurantes escolares": "alimentacion_escolar",
    "comedor escolar": "alimentacion_escolar",
    "comedores escolares": "alimentacion_escolar",
    "refrigerios escolares": "alimentacion_escolar",
    "simat": "alimentacion_escolar",
    "paef": "paef",
    # ── carpa / carpas (BIDDER-INTENT-003) ───────────────────────────────────
    "carpa": "carpa",
    "carpas": "carpa",
    "programa de apoyo al empleo formal": "paef",
    "icbf": "icbf",
    "instituto colombiano de bienestar familiar": "icbf",
    "sgp": "sgp",
    "sistema general de participaciones": "sgp",
    "ocad": "ocad",
    "organo colegiado de administracion y decision": "ocad",
    "pdet": "pdet",
    "programas de desarrollo con enfoque territorial": "pdet",
    "sat": "sat",
    "sistema de alertas tempranas": "sat",
    # ── cultura ──────────────────────────────────────────────────────────────
    "cultura": "cultura",
    "cultural": "cultura",
    "culturales": "cultura",
    "artistico": "cultura",
    "artístico": "cultura",
    "artistica": "cultura",
    "artística": "cultura",
    "artisticos": "cultura",
    "artísticos": "cultura",
    "artisticas": "cultura",
    "artísticas": "cultura",
    "patrimonio cultural": "cultura",
    "eventos culturales": "cultura",
    "actividades culturales": "cultura",
    "actividades artisticas": "cultura",
    "actividades artísticas": "cultura",
    "ferias": "cultura",
    "fiestas": "cultura",
    "festivales": "cultura",
    # ── centros de vida  (verified: real SECOP objects) ─────────────────────
    "centros de vida": "centros de vida",
    "centro de vida": "centros de vida",
    # ── trampas de grasas (verified: real SECOP objects) ────────────────────
    "trampas de grasas": "trampas de grasas",
    "trampa de grasas": "trampas de grasas",
    "trampas de grasa": "trampas de grasas",
    # ── adulto mayor / persona mayor (protected domain phrase) ──────────────
    "adulto mayor": "adulto_mayor",
    "adultos mayores": "adulto_mayor",
    "persona mayor": "adulto_mayor",
    "personas mayores": "adulto_mayor",
    # ── PTAR / plantas de tratamiento de aguas residuales (PTAR-VARIANTS-001) ─
    "plantas de tratamiento de aguas residuales": "tratamiento_aguas_residuales",
    "planta de tratamiento de aguas residuales": "tratamiento_aguas_residuales",
    "tratamiento de aguas residuales": "tratamiento_aguas_residuales",
    "aguas residuales": "tratamiento_aguas_residuales",
    "ptar": "tratamiento_aguas_residuales",
    "star": "tratamiento_aguas_residuales",
}

# O(1) lookup (same data, clearer intent at call site)
VARIANT_TO_ROOT: dict[str, str] = MORPHOLOGICAL_VARIANTS

# Inverse index: root → set of all variants (including the root itself).
# Used by SoQL builder to emit OR clauses for all surface forms.
_ROOT_TO_VARIANTS: dict[str, set[str]] = {}
for _variant, _root in MORPHOLOGICAL_VARIANTS.items():
    _ROOT_TO_VARIANTS.setdefault(_root, set()).add(_variant)
# Ensure root always maps to itself even if not listed as its own variant
for _root in list(_ROOT_TO_VARIANTS.keys()):
    _ROOT_TO_VARIANTS[_root].add(_root)
# PAE como token de entrada sí normaliza a alimentación escolar, pero NO debe
# generar LIKE '%pae%' porque captura apellidos/lugares como Páez.
if "alimentacion_escolar" in _ROOT_TO_VARIANTS:
    _ROOT_TO_VARIANTS["alimentacion_escolar"].discard("pae")
    _ROOT_TO_VARIANTS["alimentacion_escolar"].discard("alimentacion_escolar")
# B6: "parque_automotor" es un slug interno. No debe emitirse como LIKE
# (no aparece en la descripción real). Sí "parque automotor" / "parques automotores".
if "parque_automotor" in _ROOT_TO_VARIANTS:
    _ROOT_TO_VARIANTS["parque_automotor"].discard("parque_automotor")
# OR-only protected phrase: preserve legacy normal parsing as "primera", "infancia",
# but allow explicit alternatives like "adulto mayor o primera infancia" to be grouped.
_ROOT_TO_VARIANTS.setdefault("primera_infancia", set()).update({
    "primera infancia",
    "primera_infancia",
})
ROOT_TO_VARIANTS: dict[str, frozenset[str]] = {
    k: frozenset(v) for k, v in _ROOT_TO_VARIANTS.items()
}

# Auto-build multiword phrase set from all multi-word keys (bigrams, trigrams, etc.)
# Used by query_router for greedy left-to-right matching before single-token parsing.
KNOWN_BIGRAMS: frozenset[tuple[str, ...]] = frozenset(
    tuple(k.split())
    for k in MORPHOLOGICAL_VARIANTS
    if len(k.split()) >= 2
)
