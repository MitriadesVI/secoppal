"""Offline affiliation evidence for an already physically accredited BuyerUnit.

Never creates units, merges NITs, or changes runtime resolution. Directory
location is tested against the entire exact-source accredited M0 cohort.
"""
from collections import Counter, defaultdict
import re

from app.core.entity_resolver import EntityResolver
from app.core.entity_types import name_tokens
from app.core.municipal_territory import get_municipal_territories
from app.utils.money import normalize_text

LOCATION = re.compile(r'CO-[A-Z]{3}-(\d{5})')
ACCREDITED = 'ACCREDITED_MUNICIPAL_AFFILIATION'
AUTHORITY = 'SECOP_DIRECTORY_EXACT_DIVIPOLA'


def fiscal(value):
    """Audit comparison only: remove an explicitly separated DV, spaces/dots."""
    return re.sub(r'[ .]', '', str(value).split('-')[0])


def executive(name):
    return EntityResolver._official_matches_institutional_family(name, 'municipality')


def location_code(row):
    match = LOCATION.fullmatch(row.get('c_digo_ubicaci_n', ''))
    return match[1] if match else None


def validate_cohort(analysis, directory, sigep):
    registry = get_municipal_territories()
    accredited = defaultdict(list)
    for row in analysis['strata']:
        if row['status'] == ACCREDITED:
            accredited[row['account'], row['name']].append(row)
    recognized = {(u['account'], n) for u in analysis['recognized_units']
                  for names in u['source_names'].values() for n in names}
    rows = []
    for index, row in enumerate(directory):
        matched = accredited[str(row['codigo_entidad']), row.get('nombre_entidad', '')]
        if not matched:
            continue
        codes = sorted({r['municipality'] for r in matched})
        code = location_code(row)
        status = ('NO_MUNICIPAL_CODE' if code is None else 'AMBIGUOUS' if len(codes) != 1
                  else 'MATCH' if codes == [code] else 'CONTRADICTION')
        # Secondary witness ignores both directory code and M0's selected
        # SIGEP codes: use the published municipality NAME + department.
        witnesses = [s for s in sigep if fiscal(s['NIT']) == fiscal(row['nit_entidad'])
                     and (name_tokens(s['NOMBRE']) == name_tokens(row['nombre_entidad'])
                          or (executive(s['NOMBRE']) and executive(row['nombre_entidad'])
                              and set(name_tokens(s['MUNICIPIO'])) <= set(name_tokens(row['nombre_entidad']))))]
        independent = sorted({t.divipola for s in witnesses for t in registry.lookup(s['MUNICIPIO'])
                              if normalize_text(t.department) == normalize_text(s['DEPARTAMENTO'])})
        independent_status = ('NOT_TESTED' if not code else 'AMBIGUOUS' if len(independent) != 1
                              else 'MATCH' if independent == [code] else 'CONTRADICTION')
        rows.append(dict(directory_index=index, account=row['codigo_entidad'], name=row['nombre_entidad'],
                         location=row.get('c_digo_ubicaci_n'), adjudicated_codes=codes, classification=status,
                         recognized_unit=(row['codigo_entidad'], row['nombre_entidad']) in recognized,
                         executive=executive(row['nombre_entidad']),
                         independent_name_department_codes=independent,
                         independent_classification=independent_status))
    counts = {k: sum(r['classification'] == k for r in rows)
              for k in ('MATCH', 'CONTRADICTION', 'AMBIGUOUS', 'NO_MUNICIPAL_CODE')}
    secondary = dict(Counter(r['independent_classification'] for r in rows))
    # A deliberately scoped gate, not a statistical claim of global accuracy.
    accepted = (counts['MATCH'] >= 100 and not counts['CONTRADICTION'] and not counts['AMBIGUOUS']
                and secondary.get('MATCH', 0) >= 100 and not secondary.get('CONTRADICTION', 0))
    return dict(authority=AUTHORITY, ROWS_TESTED=len(rows),
                ROWS_WITH_MUNICIPAL_LOCATION_CODE=len(rows) - counts['NO_MUNICIPAL_CODE'], **counts,
                recognized_subset=dict(Counter(r['classification'] for r in rows if r['recognized_unit'])),
                executive_subset=dict(Counter(r['classification'] for r in rows if r['executive'])),
                independent_name_department_check=secondary,
                verdict='ACCEPT_SCOPED_AUTHORITY' if accepted else 'REJECT_AUTHORITY',
                limits='M0 selected most affiliations using location codes: primary agreement has incorporation bias. '
                'The secondary witness ignores those codes; ambiguous witnesses are reported, not counted as matches. '
                'Neither check certifies arbitrary directory rows or creates BuyerUnits.', rows=rows)


def directory_affiliation(record, strata, directory, validation):
    """Six requirements, applied uniformly; absence/conflict returns no codes."""
    fail = lambda reason: dict(codes=[], authority='REVIEW', reason=reason)
    if validation['verdict'] != 'ACCEPT_SCOPED_AUTHORITY':
        return fail('DIRECTORY_AUTHORITY_NOT_VALIDATED')
    owner = record['official_name']
    if not executive(owner):
        return fail('NOT_MUNICIPAL_EXECUTIVE')
    physical = [r for r in strata if r['name'] == owner and r['physical_rows'] > 0]
    if {r['dataset'] for r in physical} != {'p6dx-8zbt', 'jbjy-vk9h'}:
        return fail('BUYER_UNIT_NOT_ACCREDITED_IN_BOTH_PHYSICAL_DATASETS')
    accounts = {r['account'] for r in physical}
    if len(accounts) != 1:
        return fail('NON_UNIQUE_PHYSICAL_ACCOUNT')
    account = next(iter(accounts))
    registered = [r for r in directory if str(r['codigo_entidad']) == account]
    if not registered or any(r.get('nombre_entidad') != owner for r in registered):
        return fail('DIRECTORY_PHYSICAL_NAME_OR_ACCOUNT_CONFLICT')
    codes = {location_code(r) for r in registered}
    if len(codes) != 1 or None in codes:
        return fail('NO_UNIQUE_EXACT_MUNICIPAL_CODE')
    code = next(iter(codes))
    territory = get_municipal_territories().by_code.get(code)
    if not territory:
        return fail('CODE_NOT_IN_DIVIPOLA')
    departments = [record.get('departamento', ''), *(r.get('departamento', '') for r in registered)]
    if any(normalize_text(d) != normalize_text(territory.department) for d in departments):
        return fail('INCOMPATIBLE_DEPARTMENT')
    return dict(codes=[code], authority=AUTHORITY, account=account,
                sources={ds: [owner] for ds in ('p6dx-8zbt', 'jbjy-vk9h')},
                reason='Existing executive unit; exact physical name in both datasets and same unique directory '
                'account; exact municipal code in DIVIPOLA and compatible departments. NIT is not an identity key.')
