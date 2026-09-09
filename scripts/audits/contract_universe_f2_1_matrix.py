"""Before/after matrix for F2.1, measured through the request adapter."""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TAIL = 'de la Alcaldía de Cartagena'
PAIRS = [('celebrados', 'cancelados'), ('celebrados', 'borradores'),
         ('firmados', 'cancelados'), ('suscritos', 'cancelados'),
         ('suspendidos', 'cancelados'), ('aprobados', 'cancelados'),
         ('cancelados', 'borradores')]
SINGLE = ['registros cancelados', 'registros en borrador',
          'registros enviados al proveedor', 'contratos celebrados',
          'contratos suspendidos', 'todos los contratos',
          'todos los registros del dataset']
F1 = ['contratos de elaboración de borradores de documentos',
      'contratos de custodia de borradores cancelados de archivo']

PROBE = r'''
import json, sys
sys.path.insert(0, '.')
from app.core.contract_universe_intent import request
out = {}
for text in json.loads(sys.argv[1]):
    intent = request(text)
    out[text] = None if intent is None else dict(
        universe=intent.universe.value, state=intent.state,
        conflict=intent.conflict, control=text[slice(*intent.span)])
print(json.dumps(out, ensure_ascii=False))
'''


def probe(tree: Path, texts: list[str]) -> dict:
    completed = subprocess.run(
        ['/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python', '-c', PROBE,
         json.dumps(texts, ensure_ascii=False)],
        cwd=tree, capture_output=True, text=True, check=True)
    return json.loads(completed.stdout)


def main() -> None:
    before_tree = Path(sys.argv[1])
    texts = ([f'registros {a} y {b} {TAIL}' for a, b in PAIRS]
             + [f'registros {b} y {a} {TAIL}' for a, b in PAIRS]
             + [f'{text} {TAIL}' for text in SINGLE + F1])
    before, after = probe(before_tree, texts), probe(ROOT, texts)
    rows = [dict(query=text, before=before[text], after=after[text]) for text in texts]
    (ROOT / 'docs/audits/contract_universe_v1_1_f2_1/matrix.json').write_text(
        json.dumps(dict(before_sha='2e564d06a4f18a8ef4d3b5a1fa462000e21a00da', rows=rows),
                   ensure_ascii=False, indent=2) + '\n')
    for row in rows:
        mark = 'CHANGED' if row['before'] != row['after'] else '   same'
        print(f"{mark}  {row['query'][:56]:58s} before={_short(row['before'])} after={_short(row['after'])}")


def _short(value):
    if value is None:
        return 'None'
    return f"{'CLARIFY' if value['conflict'] else value['universe']}/{value['state']}"


if __name__ == '__main__':
    main()
