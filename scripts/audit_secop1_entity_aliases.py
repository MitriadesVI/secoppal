"""C1A — Extraer muestra de entidades SECOP I 2026 para crosswalk.

Genera:
- top 50 entidades por volumen (f789-7hwg cargue 2026)
- top 25 entidades CONVOCADO 2026
- top 25 municipios pequeños/periferia 2026
- caso Ataco Tolima
"""
import json
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sodapy import Socrata
from app.config import get_settings

s = get_settings()
OUT = Path(__file__).resolve().parent.parent / "app" / "data" / "entity_source_aliases"
OUT.mkdir(parents=True, exist_ok=True)


def query(dataset, max_retries=3, timeout=30, **kwargs):
    for attempt in range(max_retries):
        try:
            c = Socrata(s.datos_gov_domain, s.secop_app_token, timeout=timeout)
            r = c.get(dataset, **kwargs)
            c.close()
            return r
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(4)
            else:
                return [{"_error": str(e)}]


# ═══ 1. Top 50 entidades f789-7hwg 2026 por volumen ═══
print("Querying top 50 entidades f789-7hwg 2026...")
r = query("f789-7hwg",
    select="nombre_entidad, COUNT(nombre_entidad) as cnt, SUM(cuantia_contrato) as total_contrato, SUM(cuantia_proceso) as total_proceso",
    where="anno_cargue_secop = 2026",
    group="nombre_entidad",
    order="COUNT(nombre_entidad) DESC",
    limit=50)
print(f"  Got {len(r)} rows")

# ═══ 2. Top 25 CONVOCADO 2026 ═══
print("Querying top 25 CONVOCADO 2026...")
r2 = query("f789-7hwg",
    select="nombre_entidad, COUNT(nombre_entidad) as cnt, SUM(cuantia_proceso) as total_proceso",
    where="anno_cargue_secop = 2026 AND estado_del_proceso = 'CONVOCADO'",
    group="nombre_entidad",
    order="COUNT(nombre_entidad) DESC",
    limit=25)
print(f"  Got {len(r2)} rows")

# ═══ 3. Top 25 municipios pequeños/periferia 2026 ═══
# Usamos municipios con < 500 procesos totales en 2026 pero > 0
print("Querying top 25 small/periphery municipios 2026...")
r3 = query("f789-7hwg",
    select="municipio_entidad, departamento_entidad, COUNT(municipio_entidad) as cnt",
    where="anno_cargue_secop = 2026",
    group="municipio_entidad, departamento_entidad",
    order="COUNT(municipio_entidad) ASC",
    limit=50)
# Filter to small municipios with at least some activity  
small_municipios = [row for row in r3 if int(row.get("cnt", 0)) > 0 and int(row.get("cnt", 0)) < 50]
print(f"  Got {len(small_municipios)} small municipios with activity")

# ═══ 4. rpmr-utcd top 50 entidades SECOPI 2026 ═══
print("Querying top 50 entidades rpmr-utcd SECOPI 2026...")
r4 = query("rpmr-utcd",
    select="nombre_de_la_entidad, COUNT(nombre_de_la_entidad) as cnt, SUM(valor_contrato) as total_valor",
    where="origen = 'SECOPI' AND date_extract_y(fecha_de_firma_del_contrato) = 2026",
    group="nombre_de_la_entidad",
    order="COUNT(nombre_de_la_entidad) DESC",
    limit=50)
print(f"  Got {len(r4)} rows")

# ═══ 5. Caso Ataco Tolima ═══
ataco_procesos = query("f789-7hwg",
    where="numero_de_proceso LIKE 'AMA-IMC%'",
    limit=20)
print(f"  Got {len(ataco_procesos)} AMA-IMC procesos")

# ═══ Save all ═══
result = {
    "top50_f789_2026": r,
    "top25_convocado_2026": r2,
    "small_municipios_2026": small_municipios[:25],
    "top50_rpmr_secopi_2026": r4,
    "ataco_tolima_sample": [
        {
            "numero_de_proceso": row.get("numero_de_proceso"),
            "estado_del_proceso": row.get("estado_del_proceso"),
            "nombre_entidad": row.get("nombre_entidad"),
            "modalidad_de_contratacion": row.get("modalidad_de_contratacion"),
            "cuantia_proceso": row.get("cuantia_proceso"),
            "fecha_de_cargue_en_el_secop": row.get("fecha_de_cargue_en_el_secop"),
        }
        for row in ataco_procesos
    ],
    "_meta": {
        "generated_by": "scripts/audit_secop1_entity_aliases.py",
        "date": "2026-05-29",
        "task": "C1A",
    }
}

out_path = OUT / "secop1_entities_sample.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2, default=str)
print(f"\nSaved: {out_path}")
print("DONE")
