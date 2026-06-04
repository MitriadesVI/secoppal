"""Quick inspection of rpmr-utcd (SECOP Integrado) — lightweight version."""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sodapy import Socrata
from app.config import get_settings

s = get_settings()
c = Socrata(s.datos_gov_domain, s.secop_app_token, timeout=15)

# 1. Count
r = c.get("rpmr-utcd", select="COUNT(*)", limit=1)
total = int(r[0]["COUNT"]) if r else 0
print(f"TOTAL: {total:,}")

# 2. Origen
r = c.get("rpmr-utcd", select="origen, COUNT(origen)", group="origen", limit=10)
print("ORIGEN:", json.dumps(r, ensure_ascii=False))

# 3. Years
r = c.get("rpmr-utcd",
          select="date_extract_y(fecha_de_firma_del_contrato) as year, COUNT(*)",
          group="year", order="year DESC", limit=25)
print("YEARS:", json.dumps(r, ensure_ascii=False))

# 4. Estados
r = c.get("rpmr-utcd",
          select="estado_del_proceso, COUNT(estado_del_proceso)",
          group="estado_del_proceso", limit=20)
print("ESTADOS:", json.dumps(r, ensure_ascii=False))

# 5. Sample
r = c.get("rpmr-utcd", limit=20, order="fecha_de_firma_del_contrato DESC")
p = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "secop_integrado_sample.json"
p.parent.mkdir(parents=True, exist_ok=True)
with open(p, "w", encoding="utf-8") as f:
    json.dump(r, f, ensure_ascii=False, indent=2, default=str)
print(f"FIXTURE: {p} ({len(r)} rows)")

# 6. Sample SECOP I
r = c.get("rpmr-utcd", where="origen = 'SECOP I'", limit=20, order="fecha_de_firma_del_contrato DESC")
p = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "secop_integrado_secop1_sample.json"
with open(p, "w", encoding="utf-8") as f:
    json.dump(r, f, ensure_ascii=False, indent=2, default=str)
print(f"FIXTURE SECOP I: {p} ({len(r)} rows)")

# 7. Sample SECOP II
r = c.get("rpmr-utcd", where="origen = 'SECOP II'", limit=20, order="fecha_de_firma_del_contrato DESC")
p = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "secop_integrado_secop2_sample.json"
with open(p, "w", encoding="utf-8") as f:
    json.dump(r, f, ensure_ascii=False, indent=2, default=str)
print(f"FIXTURE SECOP II: {p} ({len(r)} rows)")

c.close()
print("DONE")
