"""Independent SIGEP entity matching and indivisible municipality guards."""
from functools import lru_cache
import re

from app.core.compound_identity_guard import decide_identity, IdentityStatus
from app.core.entity_types import name_tokens
from app.core.municipal_territory import get_municipal_territories
from app.utils.money import normalize_text

FAMILIES = ('MUNICIPAL_EXECUTIVE', 'CONCEJO', 'PERSONERIA', 'CONTRALORIA', 'ESE_HEALTH',
            'INSTITUTE', 'PUBLIC_COMPANY', 'FUND', 'FIRE_DEPARTMENT', 'PUBLIC_SERVICES',
            'EDUCATION', 'OTHER_DECENTRALIZED', 'UNKNOWN')
LEGAL = {'alcaldia': 'MUNICIPAL_EXECUTIVE', 'concejo municipal': 'CONCEJO',
         'personeria municipal': 'PERSONERIA', 'contraloria municipal': 'CONTRALORIA',
         'empresas sociales del estado': 'ESE_HEALTH',
         'empresas de servicios publicos domiciliarios - oficiales': 'PUBLIC_COMPANY',
         'sociedad de economia mixta': 'PUBLIC_COMPANY',
         'empresas industriales y comerciales del estado': 'PUBLIC_COMPANY'}
HEAD = re.compile(r'^(concejo|personeria|contraloria|alcaldia|municipio)\b')


def provisional_family(name):
    n = normalize_text(name)
    for marker, family in [('concejo', 'CONCEJO'), ('personeria', 'PERSONERIA'),
                           ('contraloria', 'CONTRALORIA'), ('bomberos', 'FIRE_DEPARTMENT'),
                           ('institucion educativa', 'EDUCATION'), ('colegio', 'EDUCATION'),
                           ('universidad', 'EDUCATION'), ('escuela', 'EDUCATION'),
                           ('empresa social del estado', 'ESE_HEALTH'), ('e.s.e', 'ESE_HEALTH'),
                           ('hospital', 'ESE_HEALTH'), ('centro de salud', 'ESE_HEALTH'),
                           ('fondo', 'FUND'), ('f.s.e', 'FUND'), ('instituto', 'INSTITUTE'), ('unidad central', 'INSTITUTE'),
                           ('servicios publicos', 'PUBLIC_SERVICES'), ('empresa', 'PUBLIC_COMPANY'),
                           ('alcaldia', 'MUNICIPAL_EXECUTIVE'), ('municipio de', 'MUNICIPAL_EXECUTIVE')]:
        if marker in n:
            return family
    return 'UNKNOWN'


@lru_cache(maxsize=20000)
def guarded_town(name, code):
    """Longest complete spans are fed into the actual M1A.2 guard, never prefixes."""
    registry = get_municipal_territories()
    town = registry.by_code.get(code)
    if not town:
        return False
    spans = registry.name_spans(name)
    for start, end in spans:
        decision = decide_identity(f'municipio de {name[start:end]}, {town.department}', registry)
        if (decision.status == IdentityStatus.UNIQUE_MUNICIPAL_IDENTITY
                and not decision.department_mismatch
                and [t.divipola for t in decision.candidates] == [code]):
            return True
    return False


def institutional_signature(name, town):
    """Strict role + full town template; removes only declared department labels.

    No edit distance, token subset matching, acronyms or arbitrary suffix removal.
    """
    tokens = list(name_tokens(name))
    dep = list(name_tokens(town['department']))
    if tokens[:len(dep)] == dep:
        tokens = tokens[len(dep):]
    if dep and tokens[-len(dep):] == dep:
        tokens = tokens[:-len(dep)]
    if not tokens or not HEAD.match(tokens[0]):
        return None
    role = 'executive' if tokens[0] in ('alcaldia', 'municipio') else tokens[0]
    tokens = tokens[1:]
    while tokens and tokens[0] in ('municipal', 'distrital', 'del', 'de', 'municipio'):
        tokens.pop(0)
    return role, tuple(tokens)


def entity_match(name, sigep, towns):
    code = sigep['divipola']
    exact = name_tokens(name) == name_tokens(sigep['NOMBRE'])
    if exact:
        # If municipality tokens participate, require the complete guarded span.
        town_tokens = name_tokens(sigep['MUNICIPIO'])
        tokens = name_tokens(name)
        participates = any(tokens[i:i + len(town_tokens)] == town_tokens for i in range(len(tokens)))
        if participates and code in towns and not guarded_town(name, code):
            return None
        return 'EXACT_INSTITUTIONAL_TOKENS'
    if code not in towns or not guarded_town(name, code) or not guarded_town(sigep['NOMBRE'], code):
        return None
    a, b = institutional_signature(name, towns[code]), institutional_signature(sigep['NOMBRE'], towns[code])
    if a and a == b and a[1] == tuple(name_tokens(towns[code]['municipality'])):
        return 'STRICT_ROLE_AND_COMPLETE_GUARDED_TOWN'
    return None


def classify_family(name, matched):
    provisional = provisional_family(name)
    natures = {normalize_text(s['NATURALEZA JURÍDICA']) for s in matched}
    mapped = {LEGAL[n] for n in natures if n in LEGAL}
    if len(mapped) == 1:
        value = next(iter(mapped))
        if provisional in ('CONCEJO', 'PERSONERIA') and value == 'MUNICIPAL_EXECUTIVE':
            return 'UNKNOWN', 'CONFLICTING_ENTITY_NATURE', 'REVIEW'
        return value, 'SIGEP_ENTITY_MATCH_LEGAL_NATURE', 'VERIFIED'
    if len(mapped) > 1:
        return 'UNKNOWN', 'CONFLICTING_ENTITY_NATURE', 'REVIEW'
    if natures == {'establecimiento publico'}:
        value = provisional if provisional in ('EDUCATION', 'INSTITUTE', 'FUND') else 'OTHER_DECENTRALIZED'
        return value, 'SIGEP_ENTITY_MATCH_PUBLIC_ESTABLISHMENT_AND_OFFICIAL_NAME', 'VERIFIED'
    return provisional, 'NAME_ONLY', 'PROVISIONAL'
