"""Target high-volume M2B identity debt without recomputing or applying M2B."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from pathlib import Path
import socket
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.audits.m2b1 import abbreviations, accounts, acronyms, directory, gates, health, metrics, report, rules
from scripts.audits.m2b1.io import OUT, byte_identity, load, protected_hashes, read, save, validate_output


def deny_network(*args, **kwargs):
    raise RuntimeError('M2B1_OFFLINE_NETWORK_PROHIBITED')


@contextmanager
def offline():
    with patch.object(socket.socket, 'connect', deny_network), patch.object(socket.socket, 'connect_ex', deny_network), patch.object(socket, 'create_connection', deny_network):
        yield


def build(out=OUT, before=None):
    out = validate_output(out)
    with offline():
        before = before if before is not None else protected_hashes()
        ctx = load()
        rule_result = rules.evaluate(ctx)
        abbreviation = abbreviations.build(ctx, rule_result)
        acronym = acronyms.build(ctx, rule_result)
        directory_audit = directory.build(ctx, rule_result)
        account_audit = accounts.build(ctx, rule_result)
        conflict_audit = rules.official_conflicts(ctx)
        health_audit = health.build(ctx, rule_result)
        top = metrics.top_volume(ctx, rule_result)
        impact = metrics.impact(ctx, rule_result)
        proof = byte_identity(before)
        gate = gates.evaluate(ctx, rule_result, top, impact, proof, acronym,
                              directory_audit, account_audit, conflict_audit)
        artifacts = dict(abbreviation_taxonomy=abbreviation, acronym_registry_audit=acronym,
            directory_exact_audit=directory_audit, multi_account_audit=account_audit,
            official_conflicts=conflict_audit, health_patterns=health_audit, top_volume=top,
            new_identity_rules=dict(rules=rule_result['rules'], decisions=rule_result['decisions'],
                target_new_by_class={k: sorted(p['profile_id'] for p in v) for k, v in ctx['target_new'].items()}),
            adjudicated_delta=dict(M2B_ACCEPTED_CANDIDATES=55, m2b_reference='artifacts/audits/municipal_m2b/adjudicated_candidates.json',
                candidates=rule_result['accepted'], production_apply=False),
            impact=impact, gates=gate, byte_identity=proof)
        for name, data in artifacts.items():
            save(out / f'{name}.json', data)
        report.render(out, artifacts)
        if gate['audit_integrity'] != 'PASS':
            raise ValueError(f'M2B.1 gates failed: {gate["failed"]}')
        return gate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUT)
    parser.add_argument('--baseline-hashes', type=Path)
    parser.add_argument('--validate', action='store_true')
    args = parser.parse_args()
    result = build(args.output, read(args.baseline_hashes) if args.baseline_hashes else None)
    if args.validate:
        from scripts.audits.m2b1.validation import validate
        validate(args.output)
    print(result['MUNICIPAL_M2B_1'], 'resolved=', result['PROFILES_RESOLVED'],
          'new_units=', result['NEW_LOGICAL_UNITS'], 'next=', result['NEXT'])


if __name__ == '__main__':
    main()
