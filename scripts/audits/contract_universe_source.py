"""Bounded, public source adjudication. No credentials, retries or national scan."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

BUYERS = (
    'DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA',
    'ALCALDÍA DEL DISTRITO TURÍSTICO Y CULTURAL DE CARTAGENA DE INDIAS',
)


def main():
    destination = Path('docs/audits/contract_universe_v1/source.json')
    args = argparse.ArgumentParser()
    args.add_argument('--predicate-probe', action='store_true')
    probe = args.parse_args().predicate_probe
    evidence = json.loads(destination.read_text()) if destination.exists() else []
    if len(evidence) + (1 if probe else 4) > 12:
        raise SystemExit('Public source request budget exhausted')
    for buyer in (BUYERS[:1] if probe else BUYERS):
        queries = (
            "SELECT estado_contrato, count(*) AS n, count(fecha_de_firma) AS dated "
            f"WHERE nombre_entidad = '{buyer}' GROUP BY estado_contrato LIMIT 20",
            "SELECT id_contrato,nombre_entidad,estado_contrato,fecha_de_firma,urlproceso "
            f"WHERE nombre_entidad = '{buyer}' AND estado_contrato = 'Aprobado' "
            "ORDER BY id_contrato ASC LIMIT 5",
        )
        if probe:
            queries = ("SELECT id_contrato,estado_contrato WHERE id_contrato = 'CO1.PCCNTR.1356949' "
                       "AND upper(unaccent(trim(estado_contrato))) IN ('APROBADO') LIMIT 1",)
        for query in queries:
            url = 'https://www.datos.gov.co/resource/jbjy-vk9h.json?' + urlencode({'$query': query})
            item = dict(url=url, query=query, observed_at=datetime.now(timezone.utc).isoformat(),
                        timeout_seconds=15, selection='exact reported buyer; grouped states or first 5 approved IDs')
            try:
                with urlopen(url, timeout=15) as response:
                    item['status'] = response.status
                    item['rows'] = json.load(response)
            except (OSError, ValueError) as exc:
                item['error'] = str(exc)
            evidence.append(item)
            destination.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(evidence, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
