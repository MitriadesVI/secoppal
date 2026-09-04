"""Deterministic offline municipal identity adjudication; no production apply."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from pathlib import Path
import socket
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.audits.m2b import batches, gates, health, identity_vias, metrics, report, taxonomy
from scripts.audits.m2b.io import OUT, byte_identity, load, protected_hashes, read, save, summary, validate_output


def deny_network(*args, **kwargs):
    raise RuntimeError('M2B_OFFLINE_NETWORK_PROHIBITED')


@contextmanager
def offline():
    with patch.object(socket.socket, 'connect', deny_network), patch.object(socket.socket, 'connect_ex', deny_network), patch.object(socket, 'create_connection', deny_network):
        yield


def build(out=OUT, before=None):
    out = validate_output(out)
    with offline():
        before = before if before is not None else protected_hashes()
        ctx = load()  # Reconcile before creating any artifact.
        via = identity_vias.evaluate(ctx)
        tax = taxonomy.classify(ctx, via)
        work = batches.build(ctx, via)
        logical = batches.already_logical(ctx)
        delta = metrics.territory_delta(ctx, via)
        proof = byte_identity(before)
        gate = gates.evaluate(ctx, via, tax, work, logical, delta, proof)
        artifacts = dict(
            pending_identity_profiles=dict(summary=summary(ctx['pending']), profiles=ctx['pending'],
                authority='artifacts/audits/municipal_m2a/physical_profiles.json',
                predicate='affiliation == VERIFIED and logical_unit_id is null'),
            identity_failure_taxonomy=tax,
            identity_vias=dict(vias=via['vias'], profiles=via['decisions'],
                methods='Global independent SIGEP name matching; same territory is a constraint, not an identity authority.',
                scope=dict(observed_profiles=len(ctx['profiles']), sigep_rows=len(ctx['sigep']), directory_rows=len(ctx['directory'])),
                live_calls=0),
            shared_nit_cases=taxonomy.shared_nit(ctx, via),
            health_identity_audit=health.audit(ctx, via),
            adjudicated_candidates=dict(candidates=via['accepted'],
                rejected_profile_ids=sorted(d['profile_id'] for d in via['decisions'] if d['status'] == 'REVIEW'),
                production_apply=False, projection='CANDIDATE_KNOWLEDGE_ONLY'),
            batches=work, verified_logical_unrecognized=logical,
            impact=metrics.impact(ctx, via), territory_delta=delta, gates=gate, byte_identity=proof)
        for name, data in artifacts.items():
            save(out / f'{name}.json', data)
        report.render(out, artifacts)
        if gate['audit_integrity'] != 'PASS':
            raise ValueError(f'M2B gates failed: {gate["failed"]}')
        return gate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUT)
    parser.add_argument('--baseline-hashes', type=Path)
    parser.add_argument('--validate', action='store_true')
    args = parser.parse_args()
    result = build(args.output, read(args.baseline_hashes) if args.baseline_hashes else None)
    if args.validate:
        from scripts.audits.m2b.validation import validate
        validate(args.output)
    print(result['MUNICIPAL_M2B'], 'resolved=', result['profiles_resolved'], 'remaining=', result['remaining_identity_pending'])


if __name__ == '__main__':
    main()
