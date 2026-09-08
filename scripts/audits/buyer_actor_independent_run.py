"""Run bounded audit commands with persistence redirected before imports."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'docs/audits/buyer_actor_role_arbitration_v1_independent'
PYTHON = '/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('label')
    parser.add_argument('--frame', default='false')
    parser.add_argument('--local', action='store_true')
    parser.add_argument('--manifest')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command and command[0] == '--':
        command = command[1:]
    if args.manifest:
        command = [PYTHON, '-m', 'pytest', '-p', 'tests.buyer_actor_independent_guard',
                   *Path(args.manifest).read_text().splitlines(), '-q', '--tb=short', '-ra']
    with tempfile.TemporaryDirectory(prefix='buyer-independent-') as temporary:
        env = dict(os.environ)
        env.update(SECOP_FEEDBACK_PATH=temporary + '/feedback.jsonl',
                   BURR_TELEMETRY_ENABLED='false',
                   SECOP_FEEDBACK_DB_PATH=temporary + '/feedback.sqlite',
                   CONVERSATION_STORE_DIR=temporary + '/conversations',
                   SECOP_ACTOR_ALIAS_INDEX_PATH=temporary + '/absent.sqlite',
                   SECOPPAL_ACTOR_PROFILE_ENABLED='true',
                   SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED=args.frame,
                   BUYER_INDEPENDENT_EVIDENCE=str(OUT / args.label))
        env.pop('BUYER_ACTOR_EVIDENCE', None)
        env.pop('BUYER_ACTOR_LOCAL_INDEX', None)
        if args.local:
            env['BUYER_ACTOR_LOCAL_INDEX'] = '/Users/rodrigoortiz/Documents/secoppal/data/actor_alias_index.sqlite'
        result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True)
        (OUT / (args.label + '.stdout.txt')).write_bytes(result.stdout)
        (OUT / (args.label + '.stderr.txt')).write_bytes(result.stderr)
        (OUT / (args.label + '.command.json')).write_text(json.dumps({
            'command': command, 'cwd': str(ROOT), 'frame': args.frame,
            'actor': True, 'local': args.local, 'exit': result.returncode,
            'persistence': temporary,
        }, indent=2) + '\n')
        print(result.stdout.decode(errors='replace')[-6000:])
        print(result.stderr.decode(errors='replace')[-1500:])
        raise SystemExit(result.returncode)


if __name__ == '__main__':
    main()
