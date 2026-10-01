"""M1-D-GEO: exhaustive, catalog-derived sweeps of ``resolve_geography``.

Expectations are derived from the raw catalogs (DIVIPOLA JSON, DEPARTAMENTOS,
REGION_ALIASES, the curated alias tables), never typed by hand per case. The
hand-written block at the bottom only pins typical Gold phrases, and the
mutation block proves the battery kills wrong implementations.
"""
from __future__ import annotations

from collections import defaultdict
import json
from pathlib import Path
import re

import pytest

from app.core import geo_authority as ga
from app.core.geo_authority import resolve_geography
from app.core.geo_regions import region_lookup
from app.core.municipal_territory import PATH, key
from app.data import geo_secop_variants as variants
from app.data.colombia_geography import REGION_ALIASES, ZONES_BY_NAME
from app.data.departamentos import DEPARTAMENTOS

POS = 'POSITIVE'
EVIDENCE = Path(__file__).resolve().parents[1] / 'docs/audits/m1_d_geo/secop_ciudad_evidence.json'
LEAD = re.compile(r'^(?:(?:en|de|del)\s+)+')

ROWS = json.loads(PATH.read_bytes())['territories']          # raw DIVIPOLA, independent of the registry class
OFFICIAL = {r['divipola']: (r['municipality'].title(), r['department']) for r in ROWS}


def _dept_value(dane_department: str, code: str) -> str:
    from app.core.municipal_territory import get_municipal_territories
    return get_municipal_territories().by_code[code].department_value


MUNI_KEYS: dict[str, set[str]] = defaultdict(set)               # name key -> divipola codes (catalog + curated)
for _row in ROWS:
    MUNI_KEYS[key(_row['municipality'])].add(_row['divipola'])
for _alias, _codes in variants.USER_ALIASES.items():
    MUNI_KEYS[_alias].update(_codes)
DEPT_OF_CODE = {c: _dept_value(d, c) for c, (_n, d) in OFFICIAL.items()}
DEPT_OF_CODE.update({c: dep for c, (_n, dep) in variants.SUPPLEMENT_TERRITORIES.items()})
DEPT_KEYS: dict[str, str] = {key(a): v for a, v in DEPARTAMENTOS.items()
                             if LEAD.sub('', key(a)) not in variants.CITY_KEYS_FILED_AS_DEPARTMENT}
DEPT_ROOTS = {LEAD.sub('', k): v for k, v in DEPT_KEYS.items()}
REGION_KEYS = {key(a): r for a, r in REGION_ALIASES.items()}


def _is_bogota(code_set: set[str]) -> bool:
    return code_set == {'11001'}


# --------------------------------------------------------------------------- #
# Departments
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize('alias,official', sorted(DEPT_KEYS.items()))
def test_every_department_alias_resolves_when_typed(alias, official):
    got = resolve_geography(alias, 'DEPARTMENT', POS)
    assert (got.decision, got.level_resolved, got.department) == ('RESOLVED', 'DEPARTMENT', official)
    assert got.municipality_divipola is None and got.authority == 'geo-authority@1'


@pytest.mark.parametrize('root,official', sorted(DEPT_ROOTS.items()))
def test_untyped_department_is_ambiguous_exactly_when_a_municipality_shares_the_name(root, official):
    got = resolve_geography(root, 'UNKNOWN', POS)
    homonyms = MUNI_KEYS.get(root, set())
    if not homonyms:
        assert (got.decision, got.level_resolved, got.department) == ('RESOLVED', 'DEPARTMENT', official)
    elif _is_bogota(homonyms):
        assert (got.decision, got.reason, got.department, got.municipality_divipola) == (
            'RESOLVED', 'GEO_CAPITAL_DISTRICT', official, '11001')
    else:
        assert (got.decision, got.reason) == ('CLARIFY', 'GEO_LEVEL_AMBIGUOUS')
        assert f'DEPARTMENT:{official}' in got.candidates
        assert sum(c.startswith('MUNICIPALITY:') for c in got.candidates) == len(homonyms)
        assert got.department is None and got.municipality_divipola is None


def test_there_are_department_municipality_homonyms_to_exercise():
    names = {r for r in DEPT_ROOTS if MUNI_KEYS.get(r) and not _is_bogota(MUNI_KEYS[r])}
    assert {'cordoba', 'narino', 'boyaca', 'sucre', 'san andres'} <= names


@pytest.mark.parametrize('root', sorted(r for r in DEPT_ROOTS if r in MUNI_KEYS and not _is_bogota(MUNI_KEYS[r])))
def test_typed_level_decides_a_department_municipality_homonym(root):
    dept = resolve_geography(root, 'DEPARTMENT', POS)
    assert (dept.decision, dept.level_resolved, dept.department) == ('RESOLVED', 'DEPARTMENT', DEPT_ROOTS[root])
    muni = resolve_geography(root, 'MUNICIPALITY', POS)
    if len(MUNI_KEYS[root]) == 1:
        (code,) = MUNI_KEYS[root]
        assert (muni.decision, muni.municipality_divipola) == ('RESOLVED', code)
    else:
        assert (muni.decision, muni.reason) == ('CLARIFY', 'GEO_MUNICIPALITY_AMBIGUOUS')
    framed = resolve_geography(f'municipio de {root}', 'UNKNOWN', POS)
    assert framed.decision == muni.decision and framed.municipality_divipola == muni.municipality_divipola
    typed_dept = resolve_geography(f'departamento de {root}', 'UNKNOWN', POS)
    assert (typed_dept.decision, typed_dept.level_resolved) == ('RESOLVED', 'DEPARTMENT')


# --------------------------------------------------------------------------- #
# Municipalities: the 1103 DIVIPOLA units, every curated alias, the island
# --------------------------------------------------------------------------- #
ALL_CODES = sorted(DEPT_OF_CODE)


def _group(code: str) -> set[str]:
    name_key = key(OFFICIAL[code][0]) if code in OFFICIAL else key(variants.SUPPLEMENT_TERRITORIES[code][0])
    return MUNI_KEYS[name_key] | {code}


def test_catalog_denominator():
    assert len(OFFICIAL) == 1103 and set(variants.SUPPLEMENT_TERRITORIES) == {'88001'}


@pytest.mark.parametrize('code', ALL_CODES)
def test_every_municipality_official_name_and_hint(code):
    name = OFFICIAL[code][0] if code in OFFICIAL else variants.SUPPLEMENT_TERRITORIES[code][0].title()
    group = MUNI_KEYS[key(name)] | {code}
    typed = resolve_geography(name, 'MUNICIPALITY', POS)
    if key(name) in COUNTRY_KEYS_SET:
        # the country name needs the typed frame or its department: "Colombia" alone is the country
        assert typed.decision == 'CLARIFY' and typed.municipality_divipola is None
        typed = resolve_geography(f'municipio de {name}', 'UNKNOWN', POS)
    if len(group) == 1:
        assert (typed.decision, typed.level_resolved, typed.municipality_divipola) == (
            'RESOLVED', 'MUNICIPALITY', code), typed
        assert typed.department == DEPT_OF_CODE[code]
    else:
        assert (typed.decision, typed.reason) == ('CLARIFY', 'GEO_MUNICIPALITY_AMBIGUOUS')
        assert {c.split(':')[1] for c in typed.candidates} == group
    # With the department the municipality is always decided, unless the catalog
    # itself repeats the name inside one department.
    same_dept = {c for c in group if DEPT_OF_CODE[c] == DEPT_OF_CODE[code]}
    hinted = resolve_geography(name, 'MUNICIPALITY', POS, department_hint=DEPT_OF_CODE[code])
    if len(same_dept) == 1:
        assert (hinted.decision, hinted.municipality_divipola, hinted.department) == (
            'RESOLVED', code, DEPT_OF_CODE[code])
    # a wrong department never resolves to this municipality
    other = next(d for d in set(DEPT_OF_CODE.values()) if d not in {DEPT_OF_CODE[c] for c in group})
    wrong = resolve_geography(name, 'MUNICIPALITY', POS, department_hint=other)
    expected_reason = 'GEO_COUNTRY_VS_MUNICIPALITY' if key(name) in COUNTRY_KEYS_SET else 'GEO_DEPARTMENT_MISMATCH'
    assert (wrong.decision, wrong.reason) == ('CLARIFY', expected_reason)
    assert wrong.municipality_divipola is None


@pytest.mark.parametrize('code', [c for c in ALL_CODES if ',' not in (OFFICIAL.get(c) or ('',))[0]])
def test_qualifier_inside_the_quote_equals_the_hint(code):
    name = OFFICIAL[code][0] if code in OFFICIAL else 'San Andrés'
    if len(_group(code)) == 1 or code in OFFICIAL:
        a = resolve_geography(f'{name}, {DEPT_OF_CODE[code]}', 'MUNICIPALITY', POS)
        b = resolve_geography(name, 'MUNICIPALITY', POS, department_hint=DEPT_OF_CODE[code])
        assert (a.decision, a.municipality_divipola, a.reason) == (b.decision, b.municipality_divipola, b.reason)


def test_homonym_groups_cover_the_audited_cases():
    groups = [codes for codes in MUNI_KEYS.values() if len(codes) > 1]
    assert len(groups) >= 64
    for names in ('san luis', 'la union', 'armenia', 'san francisco', 'cordoba', 'rionegro', 'concepcion'):
        assert len(MUNI_KEYS[names]) >= 2


def test_every_homonym_group_is_ambiguous_until_the_department_decides():
    checked = 0
    for name_key, codes in MUNI_KEYS.items():
        if len(codes) < 2:
            continue
        got = resolve_geography(name_key, 'MUNICIPALITY', POS)
        assert (got.decision, got.reason) == ('CLARIFY', 'GEO_MUNICIPALITY_AMBIGUOUS'), name_key
        assert {c.split(':')[1] for c in got.candidates} == codes
        for dept in {DEPT_OF_CODE[c] for c in codes}:
            members = [c for c in codes if DEPT_OF_CODE[c] == dept]
            hinted = resolve_geography(name_key, 'MUNICIPALITY', POS, department_hint=dept)
            if len(members) == 1:
                assert (hinted.decision, hinted.municipality_divipola) == ('RESOLVED', members[0]), (name_key, dept)
            else:
                assert hinted.decision == 'CLARIFY'
            checked += 1
    assert checked >= 146


def test_untyped_municipality_names_resolve_by_catalog_cardinality():
    for name_key, codes in MUNI_KEYS.items():
        got = resolve_geography(name_key, 'UNKNOWN', POS)
        is_dept = name_key in DEPT_ROOTS or name_key in COUNTRY_KEYS_SET
        is_region = name_key in REGION_KEYS
        if name_key in COUNTRY_KEYS_SET:
            assert got.decision == 'BLOCK', name_key
        elif is_dept or is_region:
            assert got.decision in {'RESOLVED', 'CLARIFY'}
        elif len(codes) == 1:
            assert (got.decision, got.municipality_divipola) == ('RESOLVED', next(iter(codes))), name_key
        else:
            assert (got.decision, got.reason) == ('CLARIFY', 'GEO_MUNICIPALITY_AMBIGUOUS'), name_key
        assert got.level_resolved != 'DEPARTMENT' or is_dept


COUNTRY_KEYS_SET = set(variants.COUNTRY_KEYS)


def test_colloquial_aliases_resolve_to_exactly_their_declared_codes():
    for alias, codes in variants.USER_ALIASES.items():
        got = resolve_geography(alias, 'MUNICIPALITY', POS)
        expected = MUNI_KEYS[alias]
        assert expected >= set(codes)
        if len(expected) == 1:
            assert (got.decision, got.municipality_divipola) == ('RESOLVED', codes[0]), alias
        else:
            assert got.decision == 'CLARIFY' and {c.split(':')[1] for c in got.candidates} == expected


def test_alias_tables_are_internally_consistent():
    assert all(key(a) == a for a in variants.USER_ALIASES)
    assert all(c in DEPT_OF_CODE for codes in variants.USER_ALIASES.values() for c in codes)
    assert set(variants.SECOP_CITY_VALUES) <= set(DEPT_OF_CODE)
    # an alias never shadows a department-only name by accident
    assert not (set(variants.USER_ALIASES) & set(REGION_KEYS))


# --------------------------------------------------------------------------- #
# Regions
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize('alias,region', sorted(REGION_KEYS.items()))
def test_every_region_alias(alias, region):
    for level in ('REGION', 'UNKNOWN'):
        got = resolve_geography(alias, level, POS)
        if alias in variants.AMBIGUOUS_REGION_ALIASES:
            assert (got.decision, got.reason) == ('CLARIFY', 'GEO_REGION_AMBIGUOUS')
            assert set(got.candidates) == {f'REGION:{r}' for r in variants.AMBIGUOUS_REGION_ALIASES[alias]}
            continue
        assert (got.decision, got.level_resolved, got.region) == ('RESOLVED', 'REGION', region)
        assert got.departments_in_region == tuple(ZONES_BY_NAME[region])
        assert got.department is None and got.municipality_divipola is None
        assert any(e.startswith('region_alias:') for e in got.evidence)


def test_region_departments_are_official_secop_values_and_framing_is_accepted():
    official = set(DEPARTAMENTOS.values())
    for region, departments in ZONES_BY_NAME.items():
        assert departments and set(departments) <= official
        for frame in (f'región de {region}', f'la región {region}', f'zona {region}', f'región del {region}'):
            got = resolve_geography(frame, 'UNKNOWN', POS)
            assert (got.decision, got.region) == ('RESOLVED', region), frame
    assert resolve_geography('Amazonas', 'REGION', POS).reason == 'GEO_LEVEL_CONFLICT'
    assert resolve_geography('Amazonas', 'UNKNOWN', POS).level_resolved == 'DEPARTMENT'
    assert region_lookup('Amazonas') == () and region_lookup('') == ()


def test_no_municipality_or_department_name_is_a_region():
    assert not [k for k in set(MUNI_KEYS) | set(DEPT_KEYS) if region_lookup(k)]


@pytest.mark.parametrize('quote', ['Urabá', 'Magdalena Medio', 'Catatumbo', 'Montes de María'])
def test_sub_departmental_regions_are_not_invented(quote):
    got = resolve_geography(quote, 'REGION', POS)
    assert got.decision == 'CLARIFY' and got.departments_in_region == ()


# --------------------------------------------------------------------------- #
# Typed-level conflicts, country, polarity, malformed input
# --------------------------------------------------------------------------- #
def test_level_conflicts_never_resolve():
    for root in DEPT_ROOTS:
        if root not in MUNI_KEYS:
            got = resolve_geography(root, 'MUNICIPALITY', POS)
            assert (got.decision, got.reason) == ('CLARIFY', 'GEO_LEVEL_CONFLICT'), root
            assert got.municipality_divipola is None
    for name_key, codes in MUNI_KEYS.items():
        if name_key not in DEPT_ROOTS and name_key not in COUNTRY_KEYS_SET:
            got = resolve_geography(name_key, 'DEPARTMENT', POS)
            assert (got.decision, got.reason) == ('CLARIFY', 'GEO_LEVEL_CONFLICT'), name_key
            assert got.department is None
    assert resolve_geography('municipio de Antioquia', 'DEPARTMENT', POS).reason == 'GEO_LEVEL_CONFLICT'
    assert resolve_geography('departamento de Antioquia', 'MUNICIPALITY', POS).reason == 'GEO_LEVEL_CONFLICT'


@pytest.mark.parametrize('country', sorted(variants.COUNTRY_KEYS))
def test_country_is_never_a_department_or_a_municipality(country):
    for level in ('UNKNOWN', 'DEPARTMENT', 'REGION'):
        got = resolve_geography(country, level, POS)
        assert got.decision == 'BLOCK' and got.reason == 'GEO_COUNTRY_IS_NOT_A_FILTER'
        assert got.municipality_divipola is None and got.department is None
    got = resolve_geography(country, 'MUNICIPALITY', POS)
    assert got.decision == 'CLARIFY' and got.municipality_divipola is None
    got = resolve_geography(f'{country}, Colombia', 'UNKNOWN', POS)
    assert got.municipality_divipola is None


def test_colombia_the_huila_municipality_needs_an_explicit_reason():
    for ok in (resolve_geography('municipio de Colombia', 'UNKNOWN', POS),
               resolve_geography('Colombia', 'MUNICIPALITY', POS, department_hint='Huila')):
        assert (ok.decision, ok.municipality_divipola, ok.department) == ('RESOLVED', '41206', 'Huila')
    assert resolve_geography('Colombia', 'MUNICIPALITY', POS, department_hint='Antioquia').decision != 'RESOLVED'


@pytest.mark.parametrize('quote', ['Cali', 'Antioquia', 'Caribe', 'Córdoba', 'San Luis', 'Colombia', 'zzz'])
@pytest.mark.parametrize('level', ['DEPARTMENT', 'MUNICIPALITY', 'REGION', 'UNKNOWN'])
def test_negative_polarity_is_always_blocked(quote, level):
    got = resolve_geography(quote, level, 'NEGATIVE')
    assert got.decision == 'BLOCK' and got.reason == 'GEO_NEGATIVE_UNSUPPORTED'
    assert got.level_resolved is None and got.department is None and got.municipality_divipola is None
    assert got.region is None and got.departments_in_region == ()


def test_every_catalog_name_is_blocked_when_negative():
    names = list(DEPT_KEYS) + list(MUNI_KEYS) + list(REGION_KEYS)
    assert all(resolve_geography(n, 'UNKNOWN', 'NEGATIVE').decision == 'BLOCK' for n in names)


def test_malformed_inputs_fail_closed():
    assert resolve_geography('', 'UNKNOWN', POS).reason == 'GEO_EMPTY_QUOTE'
    assert resolve_geography('   ', 'UNKNOWN', POS).reason == 'GEO_EMPTY_QUOTE'
    assert resolve_geography(None, 'UNKNOWN', POS).reason == 'GEO_EMPTY_QUOTE'     # type: ignore[arg-type]
    assert resolve_geography('Cali', 'CITY', POS).reason == 'GEO_LEVEL_INVALID'
    assert resolve_geography('Cali', 'UNKNOWN', 'MAYBE').reason == 'GEO_POLARITY_INVALID'
    assert resolve_geography('Cali', 'UNKNOWN', None).reason == 'GEO_POLARITY_INVALID'   # type: ignore[arg-type]
    assert resolve_geography('Atlantida', 'UNKNOWN', POS).reason == 'GEO_NOT_FOUND'
    assert resolve_geography('Cali', 'UNKNOWN', POS, department_hint='Narnia').reason == 'GEO_QUALIFIER_UNRESOLVED'
    assert resolve_geography('Cali, Narnia', 'UNKNOWN', POS).reason == 'GEO_QUALIFIER_UNRESOLVED'
    assert resolve_geography('Cali', 'UNKNOWN', POS, department_hint='Antioquia').reason == 'GEO_DEPARTMENT_MISMATCH'
    assert resolve_geography('San Luis, Tolima', 'UNKNOWN', POS, department_hint='Huila').reason == \
        'GEO_DEPARTMENT_HINT_CONFLICT'


def test_enum_inputs_are_accepted():
    from enum import Enum

    class P(str, Enum):
        POSITIVE = 'POSITIVE'

    class L(str, Enum):
        MUNICIPALITY = 'MUNICIPALITY'

    assert resolve_geography('Cali', L.MUNICIPALITY, P.POSITIVE).municipality_divipola == '76001'


# --------------------------------------------------------------------------- #
# SECOP evidence: what `ciudad` really stores
# --------------------------------------------------------------------------- #
def _evidence():
    return json.loads(EVIDENCE.read_text(encoding='utf-8'))


def test_every_real_secop_city_department_pair_resolves_to_its_municipality():
    data = _evidence()
    assert len(data['pairs']) > 1000 and data['rows_with_defined_city'] > 10_000_000
    failures = []
    for pair in data['pairs']:
        got = resolve_geography(pair['ciudad'], 'MUNICIPALITY', POS, department_hint=pair['departamento'])
        spellings = {key(v) for v in got.secop_city_values}
        if got.decision != 'RESOLVED' or key(pair['ciudad']) not in spellings or got.department != pair['departamento']:
            failures.append((pair['ciudad'], pair['departamento'], got.reason))
    assert failures == []
    assert data['rows_matched_by_geo_authority'] == data['rows_with_defined_city']
    assert data['rows_matched_by_official_divipola'] < 0.7 * data['rows_with_defined_city']   # why the bridge exists


def test_pairs_the_official_registry_missed_are_the_curated_ones():
    missed = [p for p in _evidence()['pairs'] if not p['official_name_matches']]
    assert 25 <= len(missed) <= 40
    for pair in missed:
        got = resolve_geography(pair['ciudad'], 'MUNICIPALITY', POS, department_hint=pair['departamento'])
        assert got.decision == 'RESOLVED' and got.municipality_divipola in variants.SECOP_CITY_VALUES


def test_secop_city_values_default_to_the_official_name():
    for code in ALL_CODES:
        if code not in variants.SECOP_CITY_VALUES:
            assert ga.secop_city_values(code) == (ga._place(code).name,)
    assert ga.secop_city_values('99999') == ()


# --------------------------------------------------------------------------- #
# Typical Gold phrases
# --------------------------------------------------------------------------- #
GOLD = [
    ('Barranquilla', 'UNKNOWN', ('RESOLVED', 'MUNICIPALITY', '08001', 'Atlántico')),
    ('Soledad', 'MUNICIPALITY', ('RESOLVED', 'MUNICIPALITY', '08758', 'Atlántico')),
    ('Cali', 'UNKNOWN', ('RESOLVED', 'MUNICIPALITY', '76001', 'Valle del Cauca')),
    ('Cartagena', 'MUNICIPALITY', ('RESOLVED', 'MUNICIPALITY', '13001', 'Bolívar')),
    ('Tumaco', 'UNKNOWN', ('RESOLVED', 'MUNICIPALITY', '52835', 'Nariño')),
    ('Cúcuta', 'UNKNOWN', ('RESOLVED', 'MUNICIPALITY', '54001', 'Norte de Santander')),
    ('Bogotá', 'MUNICIPALITY', ('RESOLVED', 'MUNICIPALITY', '11001', 'Distrito Capital de Bogotá')),
    ('Bogotá', 'DEPARTMENT', ('RESOLVED', 'DEPARTMENT', None, 'Distrito Capital de Bogotá')),
    ('Bogotá D.C.', 'UNKNOWN', ('RESOLVED', 'DEPARTMENT', '11001', 'Distrito Capital de Bogotá')),
    ('San Luis, Tolima', 'UNKNOWN', ('RESOLVED', 'MUNICIPALITY', '73678', 'Tolima')),
    ('municipio de Córdoba', 'UNKNOWN', ('CLARIFY', None, None, None)),
    ('Córdoba', 'UNKNOWN', ('CLARIFY', None, None, None)),
    ('Córdoba', 'DEPARTMENT', ('RESOLVED', 'DEPARTMENT', None, 'Córdoba')),
    ('Medellín (Antioquia)', 'UNKNOWN', ('RESOLVED', 'MUNICIPALITY', '05001', 'Antioquia')),
    ('en la ciudad de Cali', 'UNKNOWN', ('RESOLVED', 'MUNICIPALITY', '76001', 'Valle del Cauca')),
    ('Colombia', 'UNKNOWN', ('BLOCK', None, None, None)),
]


@pytest.mark.parametrize('quote,level,expected', GOLD)
def test_gold_phrases(quote, level, expected):
    got = resolve_geography(quote, level, POS)
    assert (got.decision, got.level_resolved, got.municipality_divipola, got.department) == expected


@pytest.mark.parametrize('quote,region,count', [('Caribe', 'Caribe', 7), ('Costa Caribe', 'Caribe', 7),
                                                ('Eje Cafetero', 'Eje Cafetero', 3), ('los Llanos', 'Orinoquia', 4)])
def test_gold_regions(quote, region, count):
    got = resolve_geography(quote, 'UNKNOWN', POS)
    assert (got.decision, got.region, len(got.departments_in_region)) == ('RESOLVED', region, count)


def test_la_costa_is_not_silently_the_caribbean():
    got = resolve_geography('la costa', 'UNKNOWN', POS)
    assert got.decision == 'CLARIFY' and 'REGION:Caribe' in got.candidates and 'REGION:Pacifica' in got.candidates


# --------------------------------------------------------------------------- #
# Window (can only retire), destination value and reasons
# --------------------------------------------------------------------------- #
def toks(text):
    return tuple(key(text).split())


def test_window_can_only_retire_authority():
    # owner cue: "del municipio de Soledad" is the buyer, not a geography
    assert resolve_geography('Soledad', 'MUNICIPALITY', POS, left=toks('contratos del municipio de')).reason == \
        'GEO_CONTEXT_OWNER_CUE'
    assert resolve_geography('municipio de Soledad', 'UNKNOWN', POS, left=toks('contratos del')).reason == \
        'GEO_CONTEXT_OWNER_CUE'
    assert resolve_geography('Cali', 'UNKNOWN', POS, left=toks('contratos de la alcaldia de')).reason == \
        'GEO_CONTEXT_OWNER_CUE'
    # ...but "en el municipio de" is geography
    ok = resolve_geography('Soledad', 'MUNICIPALITY', POS, left=toks('contratos en el municipio de'))
    assert (ok.decision, ok.municipality_divipola) == ('RESOLVED', '08758')
    # typed frame in the window contradicts the resolved level
    assert resolve_geography('Antioquia', 'UNKNOWN', POS, left=toks('en el municipio de')).reason == 'GEO_LEVEL_CONFLICT'
    assert resolve_geography('Cali', 'UNKNOWN', POS, left=toks('en el departamento de')).reason == 'GEO_LEVEL_CONFLICT'
    # next words name another department
    assert resolve_geography('Cali', 'UNKNOWN', POS, right=toks('Antioquia y más')).reason == \
        'GEO_CONTEXT_DEPARTMENT_CONFLICT'
    assert resolve_geography('Cali', 'UNKNOWN', POS, right=toks('Valle del Cauca')).decision == 'RESOLVED'
    assert resolve_geography('Cali', 'UNKNOWN', POS, right=toks('departamento de Valle del Cauca')).decision == 'RESOLVED'
    # a window never GRANTS: ambiguity, unknowns and country stay what they are
    for quote, level, left, right in [
        ('Córdoba', 'UNKNOWN', toks('en el municipio de'), toks('Bolívar')),
        ('San Luis', 'MUNICIPALITY', (), toks('Tolima')),
        ('Colombia', 'UNKNOWN', toks('en el municipio de'), ()),
        ('Atlantida', 'UNKNOWN', toks('en el municipio de'), toks('Huila')),
    ]:
        base = resolve_geography(quote, level, POS)
        with_window = resolve_geography(quote, level, POS, left=left, right=right)
        assert base.decision != 'RESOLVED' and with_window == base, quote
    # the capital district is both levels: either frame is true
    assert resolve_geography('Bogotá', 'UNKNOWN', POS, left=toks('en el municipio de')).decision == 'RESOLVED'
    assert resolve_geography('Bogotá', 'UNKNOWN', POS, left=toks('en el departamento de')).decision == 'RESOLVED'
    # the window is clipped to four folded tokens
    long_left = toks('alcaldia de uno dos tres cuatro')
    assert resolve_geography('Cali', 'UNKNOWN', POS, left=long_left).decision == 'RESOLVED'


def test_window_never_changes_a_verdict_that_context_cannot_contradict():
    for code in ALL_CODES[::37]:
        name = OFFICIAL[code][0] if code in OFFICIAL else 'San Andrés'
        base = resolve_geography(name, 'MUNICIPALITY', POS, department_hint=DEPT_OF_CODE[code])
        quiet = resolve_geography(name, 'MUNICIPALITY', POS, department_hint=DEPT_OF_CODE[code],
                                  left=toks('contratos en'), right=toks('para obras de salud'))
        assert quiet == base


def test_geo_value_and_host_reasons_cover_every_verdict():
    emitted = set()
    for quote in list(DEPT_KEYS) + list(MUNI_KEYS) + list(REGION_KEYS) + ['Colombia', 'zzz', '', 'Cali, Narnia']:
        for level in ga.LEVELS:
            for pol in ('POSITIVE', 'NEGATIVE'):
                got = resolve_geography(quote, level, pol)
                emitted.add(got.reason)
                value = ga.to_geo_value(got)
                if got.decision == 'RESOLVED':
                    assert value is not None and ga.host_reason(got) is None
                    assert value.level == got.level_resolved
                    if value.level == 'MUNICIPALITY':
                        assert value.divipola == got.municipality_divipola and value.members == (got.department,)
                    elif value.level == 'REGION':
                        assert value.members == got.departments_in_region and value.divipola is None
                    else:
                        assert value.canonical == got.department and value.members == ()
                else:
                    assert value is None and ga.host_reason(got) in {
                        'AMBIGUOUS_HOST_DIMENSION:GEOGRAPHY', 'LEVEL_MISMATCH:GEOGRAPHY',
                        'UNRESOLVED_HOST_DIMENSION:GEOGRAPHY', 'NEGATION_UNSUPPORTED:GEOGRAPHY',
                        'GENERIC_IDENTITY:GEOGRAPHY'}
    assert emitted <= set(ga.HOST_REASON) | ga.RESOLVED_REASONS
    assert {'GEO_LEVEL_AMBIGUOUS', 'GEO_MUNICIPALITY_AMBIGUOUS', 'GEO_COUNTRY_IS_NOT_A_FILTER',
            'GEO_NEGATIVE_UNSUPPORTED', 'GEO_LEVEL_CONFLICT'} <= emitted
    assert ga.host_reason(resolve_geography('Colombia', 'UNKNOWN', POS)) == 'GENERIC_IDENTITY:GEOGRAPHY'
    assert ga.host_reason(resolve_geography('Cali', 'UNKNOWN', 'NEGATIVE')) == 'NEGATION_UNSUPPORTED:GEOGRAPHY'
    assert ga.host_reason(resolve_geography('Córdoba', 'UNKNOWN', POS)) == 'AMBIGUOUS_HOST_DIMENSION:GEOGRAPHY'


# --------------------------------------------------------------------------- #
# Properties (Hypothesis): arbitrary text never breaks the invariants
# --------------------------------------------------------------------------- #
from hypothesis import given, settings, strategies as st  # noqa: E402

_NAMES = sorted(set(DEPT_KEYS) | set(MUNI_KEYS) | set(REGION_KEYS) | COUNTRY_KEYS_SET)
_text = st.one_of(st.sampled_from(_NAMES), st.text(max_size=40),
                  st.builds(lambda a, b: f'{a}, {b}', st.sampled_from(_NAMES), st.sampled_from(_NAMES)),
                  st.builds(lambda f, n: f'{f} {n}', st.sampled_from(['municipio de', 'departamento de', 'región de', 'en', 'de']),
                            st.sampled_from(_NAMES)))
_level = st.sampled_from(ga.LEVELS)
_toks = st.lists(st.sampled_from(['en', 'de', 'del', 'municipio', 'alcaldia', 'antioquia', 'huila', 'x']), max_size=6).map(tuple)


@settings(max_examples=400, deadline=None)
@given(_text, _level, st.sampled_from(['POSITIVE', 'NEGATIVE']), st.one_of(st.none(), st.sampled_from(sorted(set(DEPARTAMENTOS.values())))),
       _toks, _toks)
def test_properties_hold_for_arbitrary_text(quote, level, polarity, hint, left, right):
    got = resolve_geography(quote, level, polarity, department_hint=hint, left=left, right=right)
    assert got.decision in {'RESOLVED', 'CLARIFY', 'BLOCK'} and got.authority == 'geo-authority@1'
    if polarity == 'NEGATIVE':
        assert got.decision == 'BLOCK'
    if got.decision == 'RESOLVED':
        assert got.level_resolved in {'DEPARTMENT', 'MUNICIPALITY', 'REGION'} and got.reason in ga.RESOLVED_REASONS
        if level in {'DEPARTMENT', 'MUNICIPALITY', 'REGION'}:
            assert got.level_resolved == level or got.reason == 'GEO_CAPITAL_DISTRICT'
        if got.level_resolved == 'MUNICIPALITY':
            assert got.municipality_divipola in DEPT_OF_CODE and got.department == DEPT_OF_CODE[got.municipality_divipola]
            if hint:
                assert got.department == hint
            assert got.municipality_divipola != '41206' or key(quote).endswith('colombia')
        if got.level_resolved == 'REGION':
            assert got.departments_in_region and got.department is None
        if got.level_resolved == 'DEPARTMENT':
            assert got.department in set(DEPARTAMENTOS.values())
        # the window can only retire: the same call without it is also RESOLVED to the same value
        bare = resolve_geography(quote, level, polarity, department_hint=hint)
        assert bare.decision == 'RESOLVED' and (bare.level_resolved, bare.department, bare.municipality_divipola,
                                                bare.region) == (got.level_resolved, got.department,
                                                                 got.municipality_divipola, got.region)
    else:
        assert got.level_resolved is None and got.municipality_divipola is None and got.region is None
        assert got.reason in ga.HOST_REASON


# --------------------------------------------------------------------------- #
# Mutation battery: wrong implementations must be caught
# --------------------------------------------------------------------------- #
def contract_violations(resolve) -> list[str]:
    """Compact oracle over catalog facts; empty on the real authority."""
    bad: list[str] = []

    def expect(label, got, **fields):
        for name, value in fields.items():
            if getattr(got, name) != value:
                bad.append(f'{label}:{name}={getattr(got, name)!r}')

    expect('neg', resolve('Cali', 'UNKNOWN', 'NEGATIVE'), decision='BLOCK')
    expect('country', resolve('Colombia', 'UNKNOWN', POS), decision='BLOCK', municipality_divipola=None)
    expect('country-muni', resolve('Colombia', 'MUNICIPALITY', POS), municipality_divipola=None)
    expect('homonym-level', resolve('Córdoba', 'UNKNOWN', POS), decision='CLARIFY', reason='GEO_LEVEL_AMBIGUOUS')
    expect('level-dept', resolve('Córdoba', 'DEPARTMENT', POS), decision='RESOLVED', department='Córdoba')
    expect('level-muni', resolve('Córdoba', 'MUNICIPALITY', POS), decision='CLARIFY', reason='GEO_MUNICIPALITY_AMBIGUOUS')
    expect('homonym-muni', resolve('San Luis', 'UNKNOWN', POS), decision='CLARIFY',
           reason='GEO_MUNICIPALITY_AMBIGUOUS', municipality_divipola=None)
    expect('hint', resolve('San Luis', 'UNKNOWN', POS, department_hint='Tolima'),
           decision='RESOLVED', municipality_divipola='73678')
    expect('hint-mismatch', resolve('Cali', 'MUNICIPALITY', POS, department_hint='Antioquia'), decision='CLARIFY')
    expect('qualifier', resolve('San Luis, Tolima', 'UNKNOWN', POS), municipality_divipola='73678')
    expect('colloquial', resolve('Cali', 'UNKNOWN', POS), municipality_divipola='76001', secop_city_values=('Cali',))
    expect('bogota', resolve('Bogotá', 'UNKNOWN', POS), level_resolved='DEPARTMENT', municipality_divipola='11001')
    expect('island', resolve('San Andrés', 'MUNICIPALITY', POS,
                             department_hint='San Andrés, Providencia y Santa Catalina'), municipality_divipola='88001')
    expect('region', resolve('Caribe', 'UNKNOWN', POS), region='Caribe')
    expect('region-set', resolve('Eje Cafetero', 'REGION', POS), departments_in_region=('Caldas', 'Quindío', 'Risaralda'))
    expect('costa', resolve('la costa', 'UNKNOWN', POS), decision='CLARIFY')
    expect('amazonas', resolve('Amazonas', 'UNKNOWN', POS), level_resolved='DEPARTMENT')
    expect('armenia', resolve('Armenia', 'UNKNOWN', POS), decision='CLARIFY')
    expect('conflict', resolve('Cali', 'DEPARTMENT', POS), decision='CLARIFY')
    expect('owner', resolve('Soledad', 'MUNICIPALITY', POS, left=toks('contratos del municipio de')), decision='CLARIFY')
    expect('frame', resolve('Antioquia', 'UNKNOWN', POS, left=toks('en el municipio de')), decision='CLARIFY')
    expect('after', resolve('Cali', 'UNKNOWN', POS, right=toks('Antioquia')), decision='CLARIFY')
    expect('geo-fine', resolve('Soledad', 'MUNICIPALITY', POS, left=toks('en el municipio de')), decision='RESOLVED')
    return bad


def test_battery_is_green_on_the_real_authority():
    assert contract_violations(resolve_geography) == []


def _wrapped(adapt):
    def mutant(quote, level, polarity, *, department_hint=None, left=(), right=()):
        q, lv, p, hint, lf, rg = adapt(quote, level, polarity, department_hint, left, right)
        return resolve_geography(q, lv, p, department_hint=hint, left=lf, right=rg)
    return mutant


def _patch(target, name, value):
    def apply(mp):
        mp.setattr(ga, name, value(getattr(ga, name)) if callable(value) and target == 'fn' else value)
        return resolve_geography
    return apply


MUTANTS = {
    'ignore_polarity': lambda mp: _wrapped(lambda q, lv, p, h, lf, rg: (q, lv, POS, h, lf, rg)),
    'ignore_hint': lambda mp: _wrapped(lambda q, lv, p, h, lf, rg: (q, lv, p, None, lf, rg)),
    'ignore_level': lambda mp: _wrapped(lambda q, lv, p, h, lf, rg: (q, 'UNKNOWN', p, h, lf, rg)),
    'ignore_window': lambda mp: _wrapped(lambda q, lv, p, h, lf, rg: (q, lv, p, h, (), ())),
    'country_is_a_name': _patch('v', 'COUNTRY_KEYS', frozenset()),
    'first_candidate_wins': _patch('fn', 'municipal_candidates', lambda f: (lambda name: f(name)[:1])),
    'no_colloquial_aliases': _patch('v', 'USER_ALIASES', {}),
    'no_island': _patch('v', 'SUPPLEMENT_TERRITORIES', {}),
    'no_secop_spellings': _patch('v', 'SECOP_CITY_VALUES', {}),
    'bogota_not_special': _patch('v', 'BOGOTA_DIVIPOLA', '00000'),
    'regions_blind': _patch('v', 'region_lookup', lambda text: ()),
    'ambiguous_region_first': _patch('fn', 'region_lookup', lambda f: (lambda text: f(text)[:1])),
    'departments_blind': _patch('v', 'department_of', lambda name: None),
    'qualifier_dropped': _patch('v', '_QUALIFIER_SPLITS', ()),
    'retire_disabled': _patch('v', '_retire', lambda result, left, right, quote_tokens: result),
}


@pytest.mark.parametrize('name', sorted(MUTANTS))
def test_every_mutant_is_killed(name, monkeypatch):
    resolve = MUTANTS[name](monkeypatch)
    assert contract_violations(resolve), f'mutant {name} survived the battery'
