"""Run promotion-sensitive compatibility tests in an isolated candidate tree."""
from pathlib import Path
import shutil, tempfile, subprocess, gzip
root=Path(__file__).resolve().parents[2]
with tempfile.TemporaryDirectory(prefix='secoppal-m1a2-candidate-') as tmp:
    dest=Path(tmp)
    for name in ('app','tests','scripts'):
        shutil.copytree(root/name,dest/name,ignore=shutil.ignore_patterns('__pycache__','.DS_Store'))
    for name in ('pytest.ini','ruff.toml'):
        shutil.copy2(root/name,dest/name)
    # Frozen artifacts are read-only test inputs. Adjudication output is mocked.
    shutil.copytree(root/'artifacts',dest/'artifacts')
    (dest/'data').mkdir()
    for name in ('aliases_db.json','buyer_source_names.json'):
        (dest/'app/data'/name).write_bytes(gzip.decompress((root/'artifacts/audits/municipal_m1a_retry/promoted_snapshot'/f'{name}.gz').read_bytes()))
    tests=['tests/test_compound_buyer_capture_r2a1.py','tests/test_municipal_territory.py',
           'tests/test_control_national_r2e.py','tests/test_institutional_family_compatibility.py',
           'tests/test_entity_trailing_token_robustness.py','tests/test_control_language.py',
           'tests/test_declared_buyer_groups.py']
    result=subprocess.run([str(root/'.venv/bin/python'),'-m','pytest',*tests,'-q'],cwd=dest)
    raise SystemExit(result.returncode)
