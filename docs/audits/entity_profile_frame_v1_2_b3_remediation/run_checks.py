"""Run frozen audit commands into B.3 evidence only, with the offline plugin."""
import importlib.util
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
HISTORICAL = ROOT / 'docs/audits/entity_profile_frame_v1_2_b2_independent'
sys.path.insert(0, str(HISTORICAL))
spec = importlib.util.spec_from_file_location('frozen_runner', HISTORICAL / 'audit_runner.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
runner.OUT = OUT
runner.ENV['PYTHONPATH'] += ':' + str(HISTORICAL) + ':' + str(OUT)

if __name__ == '__main__':
    runner.main()
