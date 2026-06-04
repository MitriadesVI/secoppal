"""Verificación C0.2 — Actividad SECOP I 2026 en f789-7hwg y rpmr-utcd.

Queries A–H. Solo agregados/counts/top lists. Read-only.
"""
import json
import time
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sodapy import Socrata
from app.config import get_settings

s = get_settings()
TIMEOUT = 60 if "rpmr-utcd" in " ".join(sys.argv) else 30


def query(dataset, max_retries=3, **kwargs):
    for attempt in range(max_retries):
        try:
            c = Socrata(s.datos_gov_domain, s.secop_app_token, timeout=TIMEOUT)
            r = c.get(dataset, **kwargs)
            c.close()
            return r
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(4)
            else:
                return [{"_error": f"{type(e).__name__}: {e}"}]


def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ═══════════════════════════════════════════════════════════════
# A. f789-7hwg — actividad 2026 por cargue, agrupado por estado
# ═══════════════════════════════════════════════════════════════
print_section("A. f789-7hwg — Actividad 2026 por estado (cargue)")

r = query("f789-7hwg",
    select="estado_del_proceso, COUNT(estado_del_proceso)",
    where="anno_cargue_secop = 2026",
    group="estado_del_proceso",
    order="COUNT(estado_del_proceso) DESC",
    limit=20)
total_a = sum(int(row.get(f"COUNT_estado_del_proceso", 0)) for row in r)
print(f"Total procesos cargados en 2026: {total_a:,}")
for row in r:
    cnt = row.get(f"COUNT_estado_del_proceso", "?")
    estado = row.get("estado_del_proceso", "?")
    print(f"  {estado[:45]:47s} {cnt:>8s}")

# ═══════════════════════════════════════════════════════════════
# B. f789-7hwg — procesos 2026 sin firma
# ═══════════════════════════════════════════════════════════════
print_section("B. f789-7hwg — Procesos 2026 sin contrato, por estado")

r = query("f789-7hwg",
    select="estado_del_proceso, COUNT(estado_del_proceso)",
    where="anno_cargue_secop = 2026 AND (fecha_de_firma_del_contrato IS NULL OR anno_firma_contrato = 'Sin Firma')",
    group="estado_del_proceso",
    order="COUNT(estado_del_proceso) DESC",
    limit=20)
total_b = sum(int(row.get(f"COUNT_estado_del_proceso", 0)) for row in r)
pct_b = (total_b / total_a * 100) if total_a else 0
print(f"Sin contrato: {total_b:,} de {total_a:,} ({pct_b:.1f}%)")
for row in r:
    cnt = row.get(f"COUNT_estado_del_proceso", "?")
    estado = row.get("estado_del_proceso", "?")
    print(f"  {estado[:45]:47s} {cnt:>8s}")

# ═══════════════════════════════════════════════════════════════
# C. f789-7hwg — modalidades 2026
# ═══════════════════════════════════════════════════════════════
print_section("C. f789-7hwg — Top modalidades 2026")

r = query("f789-7hwg",
    select="modalidad_de_contratacion, COUNT(modalidad_de_contratacion)",
    where="anno_cargue_secop = 2026",
    group="modalidad_de_contratacion",
    order="COUNT(modalidad_de_contratacion) DESC",
    limit=15)
for row in r:
    cnt = row.get(f"COUNT_modalidad_de_contratacion", "?")
    mod = row.get("modalidad_de_contratacion", "?")
    print(f"  {mod[:55]:57s} {cnt:>8s}")

# ═══════════════════════════════════════════════════════════════
# D. f789-7hwg — top entidades/municipios/departamentos 2026
# ═══════════════════════════════════════════════════════════════
print_section("D. f789-7hwg — Top departamentos 2026")
r = query("f789-7hwg",
    select="departamento_entidad, COUNT(departamento_entidad)",
    where="anno_cargue_secop = 2026",
    group="departamento_entidad",
    order="COUNT(departamento_entidad) DESC",
    limit=50)
for row in r:
    cnt = row.get(f"COUNT_departamento_entidad", "?")
    dep = row.get("departamento_entidad", "?")
    print(f"  {dep[:35]:37s} {cnt:>8s}")

print_section("D2. f789-7hwg — Top municipios 2026")
r = query("f789-7hwg",
    select="municipio_entidad, COUNT(municipio_entidad)",
    where="anno_cargue_secop = 2026",
    group="municipio_entidad",
    order="COUNT(municipio_entidad) DESC",
    limit=50)
for row in r:
    cnt = row.get(f"COUNT_municipio_entidad", "?")
    mun = row.get("municipio_entidad", "?")
    print(f"  {mun[:35]:37s} {cnt:>8s}")

print_section("D3. f789-7hwg — Top entidades 2026")
r = query("f789-7hwg",
    select="nombre_entidad, COUNT(nombre_entidad)",
    where="anno_cargue_secop = 2026",
    group="nombre_entidad",
    order="COUNT(nombre_entidad) DESC",
    limit=50)
for row in r:
    cnt = row.get(f"COUNT_nombre_entidad", "?")
    ent = row.get("nombre_entidad", "?")
    print(f"  {ent[:55]:57s} {cnt:>8s}")

# ═══════════════════════════════════════════════════════════════
# E. f789-7hwg — caso concreto AMA-IMC-027-2026
# ═══════════════════════════════════════════════════════════════
print_section("E. f789-7hwg — Caso concreto AMA-IMC-027-2026")

r = query("f789-7hwg",
    where="numero_de_proceso = 'AMA-IMC-027-2026' OR numero_de_constancia = 'AMA-IMC-027-2026'",
    limit=5)
if r and "_error" not in r[0]:
    for row in r:
        print(json.dumps({
            "numero_de_proceso": row.get("numero_de_proceso"),
            "numero_de_constancia": row.get("numero_de_constancia"),
            "estado_del_proceso": row.get("estado_del_proceso"),
            "nombre_entidad": row.get("nombre_entidad"),
            "modalidad_de_contratacion": row.get("modalidad_de_contratacion"),
            "cuantia_proceso": row.get("cuantia_proceso"),
            "cuantia_contrato": row.get("cuantia_contrato"),
            "fecha_de_cargue_en_el_secop": row.get("fecha_de_cargue_en_el_secop"),
            "detalle_del_objeto_a_contratar": (row.get("detalle_del_objeto_a_contratar") or "")[:200],
            "ruta_proceso_en_secop_i": row.get("ruta_proceso_en_secop_i"),
        }, ensure_ascii=False, indent=2))
else:
    print("  No encontrado por numero_de_proceso. Buscando con LIKE...")
    r = query("f789-7hwg",
        where="numero_de_proceso LIKE '%AMA-IMC%'",
        limit=5)
    if r and "_error" not in r[0]:
        for row in r:
            print(f"  {row.get('numero_de_proceso')} | {row.get('estado_del_proceso')} | {row.get('nombre_entidad')}")

# ═══════════════════════════════════════════════════════════════
# F. rpmr-utcd — contratos 2026 por origen y estado
# ═══════════════════════════════════════════════════════════════
print_section("F. rpmr-utcd — Contratos 2026 por origen")

r = query("rpmr-utcd",
    select="origen, COUNT(origen)",
    where="date_extract_y(fecha_de_firma_del_contrato) = 2026",
    group="origen",
    limit=10)
for row in r:
    cnt = row.get("COUNT_origen", "?")
    orig = row.get("origen", "?")
    print(f"  {orig:15s} {cnt:>10s}")

print_section("F2. rpmr-utcd — Contratos 2026 por estado")
r = query("rpmr-utcd",
    select="estado_del_proceso, COUNT(estado_del_proceso)",
    where="date_extract_y(fecha_de_firma_del_contrato) = 2026",
    group="estado_del_proceso",
    order="COUNT(estado_del_proceso) DESC",
    limit=20)
for row in r:
    cnt = row.get(f"COUNT_estado_del_proceso", "?")
    est = row.get("estado_del_proceso", "?")
    print(f"  {est[:45]:47s} {cnt:>10s}")

# ═══════════════════════════════════════════════════════════════
# G. rpmr-utcd — contratos SECOPI 2026 detallado
# ═══════════════════════════════════════════════════════════════
print_section("G. rpmr-utcd — Contratos SECOPI 2026 por estado")

r = query("rpmr-utcd",
    select="estado_del_proceso, COUNT(estado_del_proceso)",
    where="origen = 'SECOPI' AND date_extract_y(fecha_de_firma_del_contrato) = 2026",
    group="estado_del_proceso",
    order="COUNT(estado_del_proceso) DESC",
    limit=15)
total_g = sum(int(row.get(f"COUNT_estado_del_proceso", 0)) for row in r)
print(f"Total contratos SECOP I 2026: {total_g:,}")
for row in r:
    cnt = row.get(f"COUNT_estado_del_proceso", "?")
    est = row.get("estado_del_proceso", "?")
    print(f"  {est[:45]:47s} {cnt:>10s}")

print_section("G2. rpmr-utcd — Contratos SECOPI 2026 por modalidad")
r = query("rpmr-utcd",
    select="modalidad_de_contrataci_n, COUNT(modalidad_de_contrataci_n)",
    where="origen = 'SECOPI' AND date_extract_y(fecha_de_firma_del_contrato) = 2026",
    group="modalidad_de_contrataci_n",
    order="COUNT(modalidad_de_contrataci_n) DESC",
    limit=15)
for row in r:
    cnt = row.get(f"COUNT_modalidad_de_contrataci_n", "?")
    mod = row.get("modalidad_de_contrataci_n", "?")
    print(f"  {mod[:55]:57s} {cnt:>10s}")

print_section("G3. rpmr-utcd — Contratos SECOPI 2026 por departamento")
r = query("rpmr-utcd",
    select="departamento_entidad, COUNT(departamento_entidad)",
    where="origen = 'SECOPI' AND date_extract_y(fecha_de_firma_del_contrato) = 2026",
    group="departamento_entidad",
    order="COUNT(departamento_entidad) DESC",
    limit=50)
for row in r:
    cnt = row.get(f"COUNT_departamento_entidad", "?")
    dep = row.get("departamento_entidad", "?")
    print(f"  {dep[:35]:37s} {cnt:>10s}")

print_section("G4. rpmr-utcd — Contratos SECOPI 2026 por entidad")
r = query("rpmr-utcd",
    select="nombre_de_la_entidad, COUNT(nombre_de_la_entidad)",
    where="origen = 'SECOPI' AND date_extract_y(fecha_de_firma_del_contrato) = 2026",
    group="nombre_de_la_entidad",
    order="COUNT(nombre_de_la_entidad) DESC",
    limit=50)
for row in r:
    cnt = row.get(f"COUNT_nombre_de_la_entidad", "?")
    ent = row.get("nombre_de_la_entidad", "?")
    print(f"  {ent[:55]:57s} {cnt:>10s}")

# ═══════════════════════════════════════════════════════════════
# H. rpmr-utcd — valores SECOPI 2026
# ═══════════════════════════════════════════════════════════════
print_section("H. rpmr-utcd — Valores SECOPI 2026 (global)")

r = query("rpmr-utcd",
    select="COUNT(*) as cnt, SUM(valor_contrato) as total_valor, AVG(valor_contrato) as avg_valor",
    where="origen = 'SECOPI' AND date_extract_y(fecha_de_firma_del_contrato) = 2026",
    limit=1)
if r and "_error" not in r[0]:
    print(f"  Count:     {r[0].get('cnt', '?')}")
    print(f"  Sum:       {r[0].get('total_valor', '?')}")
    print(f"  Avg:       {r[0].get('avg_valor', '?')}")

print_section("H2. rpmr-utcd — Valores SECOPI 2026 por departamento")
r = query("rpmr-utcd",
    select="departamento_entidad, COUNT(*) as cnt, SUM(valor_contrato) as total_valor, AVG(valor_contrato) as avg_valor",
    where="origen = 'SECOPI' AND date_extract_y(fecha_de_firma_del_contrato) = 2026",
    group="departamento_entidad",
    order="SUM(valor_contrato) DESC",
    limit=20)
for row in r:
    dep = row.get("departamento_entidad", "?")
    cnt = row.get("cnt", "?")
    total = row.get("total_valor", "?")
    avg = row.get("avg_valor", "?")
    print(f"  {dep[:30]:32s} cnt={cnt:>6s}  sum={total:>15s}  avg={avg:>12s}")

print_section("H3. rpmr-utcd — Valores SECOPI 2026 por modalidad")
r = query("rpmr-utcd",
    select="modalidad_de_contrataci_n, COUNT(*) as cnt, SUM(valor_contrato) as total_valor, AVG(valor_contrato) as avg_valor",
    where="origen = 'SECOPI' AND date_extract_y(fecha_de_firma_del_contrato) = 2026",
    group="modalidad_de_contrataci_n",
    order="SUM(valor_contrato) DESC",
    limit=15)
for row in r:
    mod = row.get("modalidad_de_contrataci_n", "?")
    cnt = row.get("cnt", "?")
    total = row.get("total_valor", "?")
    avg = row.get("avg_valor", "?")
    print(f"  {mod[:50]:52s} cnt={cnt:>6s}  sum={total:>15s}  avg={avg:>12s}")

print("\n✅ VERIFICACIÓN C0.2 COMPLETA")
