"""Gates for the final municipal M1A promotion, anchored on the CURRENT baseline.

Authority notes that separate this module from the Retry #1 verifier:

* Equality is never asserted against ``municipal_m1a_retry/promoted_snapshot``.
  That snapshot was regenerated from a baseline that still carried the Turbaco
  duplicate canonical, so it is not a representative candidate any more.
* Every count (cross-interaction, gazetteer, alias delta) is derived from the
  files under audit. Nothing is compared to a frozen expectation.
* The four affirmative affiliations adjudicated by M1A.2A are read from frozen
  evidence; ``adjudicate()`` is never re-run because it rewrites that evidence.
"""
from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.municipal_territory import get_municipal_territories
from app.core.query_plan import compute_query_plan
from app.core.soql_builder import SoQLBuilder
from app.utils.money import normalize_text
from scripts.audits.gate_divipola_buyer_matrix import buyer_oracle, discovery, evaluate
from scripts.audits.promote_municipal_m1a import DATASETS, read
from scripts.audits.verify_municipal_m1a_retry import buyer_check

M0 = ROOT / 'artifacts/audits/municipal_m0_v1'
M1A2A = ROOT / 'artifacts/audits/municipal_m1a_2a'
PRODUCTION = ('app/data/aliases_db.json', 'app/data/buyer_source_names.json')


def adjudicated_affiliations():
    """Frozen M1A.2A verdicts; Turbaco is a consolidation, not an affiliation."""
    cases = read(M1A2A / 'adjudication.json')['cases']
    return {c['canonical']: c['affiliation'] for c in cases
            if c['decision'] == 'VERIFIED_MUNICIPAL_AFFILIATION'}


def final_oracle(records, sidecar, candidate):
    """M1A.2 oracle plus the affirmative evidence already adjudicated by M1A.2A.

    The injection can only turn an unaccredited owner into an accredited one for
    a canonical that already exists in the catalogue; it never invents owners and
    never overwrites codes that M0 already produced.
    """
    oracle = buyer_oracle(records, sidecar, candidate)
    for canonical, affiliation in adjudicated_affiliations().items():
        if canonical not in oracle:
            raise ValueError('STOP: adjudicated canonical absent from catalogue: ' + canonical)
        if oracle[canonical]['codes']:
            raise ValueError('STOP: adjudicated canonical already accredited: ' + canonical)
        oracle[canonical] = affiliation
    return oracle


def divipola_matrix(routers, catalogs):
    """Full 1.103-municipality matrix over both catalogues; REVIEW must be zero."""
    reports = {}
    for label, router in routers.items():
        records, sidecar = catalogs[label]
        reports[label] = evaluate(router, sidecar, final_oracle(records, sidecar, label != 'baseline'))
    counts = {k: sum(r['counts'][k] for r in reports.values()) for k in next(iter(reports.values()))['counts']}
    total = sum(r['total_queries'] for r in reports.values())
    return dict(status='PASS' if all(r['status'] == 'PASS' for r in reports.values()) else 'BLOCKED',
                total_queries=total, totals=counts,
                per_catalog={k: {n: v for n, v in r.items() if n != 'results'} for k, r in reports.items()},
                reports=reports)


def compound_identity(matrix_report):
    """Derived compound universe; the DIVIPOLA matrix must cover all of it."""
    universe = discovery()
    covered = {r['code'] for r in matrix_report['reports']['candidate']['results']}
    registry = get_municipal_territories()
    compound_codes = {t.divipola for name in universe['compound_names'] for t in registry.by_alias[name]}
    homonym_codes = {t['divipola'] for ts in universe['homonym_groups'].values() for t in ts}
    collision_codes = {t.divipola for c in universe['prefix_collisions']
                       for name in (c['full'], c['prefix']) for t in registry.by_alias[name]}
    relevant = compound_codes | homonym_codes | collision_codes
    return dict(counts=universe['counts'], relevant_codes=len(relevant),
                uncovered=sorted(relevant - covered), compound_names=len(universe['compound_names']),
                articulated_names=len(universe['articulated_names']),
                prefix_collisions=len(universe['prefix_collisions']),
                homonym_groups=len(universe['homonym_groups']),
                status='PASS' if not relevant - covered else 'BLOCKED')


def source_projection(router, proposals, sidecar):
    """503 bindings x 2 datasets against the real projector; no patched registry."""
    builder = SoQLBuilder()
    bindings = {b['canonical_name']: b for b in sidecar['bindings']}
    results, checks = [], 0
    for p in proposals:
        binding = bindings.get(p['canonical'])
        rows = [buyer_check(router, builder, p, p['aliases'][0], ds) for ds in DATASETS]
        checks += len(rows)
        results.append(dict(code=p['code'], canonical=p['canonical'], checks=rows,
                            passed=bool(binding and binding['sources'] == p['source_names']
                                        and binding['identity_key'] == {'type': 'BUYER_UNIT',
                                                                        'value': 'municipal_m0:' + p['code']}
                                        and all(r['passed'] for r in rows))))
    failures = [r for r in results if not r['passed']]
    return dict(status='PASS' if not failures else 'BLOCKED', bindings=len(results),
                dataset_checks=checks, failures=failures, results=results)


def _resolution(router, query):
    params = router.parse(query).params
    plan = compute_query_plan(params, query)
    territory = params.get('municipal_territory') or {}
    return dict(query=query, owner=params.get('entidad_resolved'),
                divipola=territory.get('divipola'), territory_status=territory.get('status'),
                should_execute=plan.should_execute, clarification=bool(plan.clarification_question),
                objeto=params.get('objeto'))


def identity_canaries(router):
    """Out-of-lot compound owners that the Retry #1 promotion resolved wrongly."""
    rows = {name: _resolution(router, query) for name, query in (
        ('sotavento_contracts', 'contratos del municipio de San Andrés de Sotavento'),
        ('sotavento_bare', 'municipio de San Andrés de Sotavento'),
        ('san_andres_santander', 'alcaldía de San Andrés, departamento Santander'),
        ('susacon_bare', 'municipio de Susacón'),
        ('susacon_contracts', 'contratos del municipio de Susacón'),
        ('susa_bare', 'municipio de Susa'))}
    sotavento = [rows['sotavento_contracts'], rows['sotavento_bare']]
    susacon = [rows['susacon_bare'], rows['susacon_contracts']]
    failures = []
    for row in sotavento:
        # 23670 is San Andrés de Sotavento, Córdoba. The Santander executive
        # (68669) must never own it, and no object may be manufactured.
        if row['owner'] or row['objeto'] or row['divipola'] not in (None, '23670') or not row['clarification']:
            failures.append(row)
    susa_owner = rows['susa_bare']['owner']
    for row in susacon:
        # Susacón (15764) may only ever be owned by a Susacón unit. Sharing an
        # owner with Susa (25772), or resolving to any other town, is a leak.
        if row['owner'] and 'susacon' not in normalize_text(row['owner']):
            failures.append(row)
        if susa_owner and row['owner'] == susa_owner:
            failures.append(row)
        if row['divipola'] not in (None, '15764'):
            failures.append(row)
    return dict(status='PASS' if not failures else 'BLOCKED', failures=failures, results=rows)


def baranoa_acceptance(router, proposals):
    """Buyer forms resolve to the unit; bare geography stays a territory."""
    proposal = next(p for p in proposals if p['code'] == '08078')
    buyer_rows = [_resolution(router, q) for q in
                  ('contratos de la alcaldia de baranoa', 'procesos del municipio de baranoa')]
    geo = _resolution(router, 'contratos en baranoa')
    owners = {r['owner'] for r in buyer_rows}
    ok = (owners == {proposal['canonical']} and all(r['should_execute'] and not r['objeto']
                                                    and not r['divipola'] for r in buyer_rows)
          and not geo['owner'] and geo['divipola'] == '08078' and geo['should_execute'])
    return dict(status='PASS' if ok else 'BLOCKED', canonical=proposal['canonical'],
                buyer=buyer_rows, geography=geo,
                snapshot_volumes={ds: proposal['volumes'][ds]['unique_ids'] for ds in DATASETS})
