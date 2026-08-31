"""Muestra congelada CO1.REQ.* desde datos.gov.co (Socrata). Determinista:
cada estrato se toma con $order=id_del_proceso y $limit fijo."""
import json, urllib.parse, urllib.request, sys, time

BASE = "https://www.datos.gov.co/resource/p6dx-8zbt.json"
COLS = ("id_del_proceso,referencia_del_proceso,id_del_portafolio,"
        "estado_del_procedimiento,fase,urlproceso,entidad,nit_entidad,"
        "modalidad_de_contratacion,fecha_de_publicacion_del")

# (nombre_estrato, where, n)
STRATA = [
    ("login_borrador",     "urlproceso.url LIKE '%Login%' AND estado_del_procedimiento='Borrador'", 4),
    ("login_en_aprobacion","urlproceso.url LIKE '%Login%' AND estado_del_procedimiento='En aprobación'", 3),
    ("login_aprobado",     "urlproceso.url LIKE '%Login%' AND estado_del_procedimiento='Aprobado'", 3),
    ("login_cancelado",    "urlproceso.url LIKE '%Login%' AND estado_del_procedimiento='Cancelado'", 3),
    ("login_publicado",    "urlproceso.url LIKE '%Login%' AND estado_del_procedimiento='Publicado'", 3),
    ("login_seleccionado", "urlproceso.url LIKE '%Login%' AND estado_del_procedimiento='Seleccionado'", 2),
    ("public_presentacion_oferta", "urlproceso.url LIKE '%OpportunityDetail%' AND fase='Presentación de oferta'", 4),
    ("public_publicado",   "urlproceso.url LIKE '%OpportunityDetail%' AND estado_del_procedimiento='Publicado'", 3),
    ("public_evaluacion",  "urlproceso.url LIKE '%OpportunityDetail%' AND estado_del_procedimiento='Evaluación'", 3),
    ("public_cancelado",   "urlproceso.url LIKE '%OpportunityDetail%' AND estado_del_procedimiento='Cancelado'", 3),
]

def get(params, tries=3):
    url = BASE + "?" + urllib.parse.urlencode(params)
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                return json.load(r)
        except Exception as exc:            # noqa: BLE001 - script de spike
            if i == tries - 1:
                raise
            print(f"  retry {i+1}: {exc}", file=sys.stderr)
            time.sleep(3)

out = []
for name, where, n in STRATA:
    rows = get({"$select": COLS, "$where": where,
                "$order": "id_del_proceso", "$limit": n})
    for r in rows:
        r["_stratum"] = name
        out.append(r)
    print(f"{name}: {len(rows)}", file=sys.stderr)

json.dump(out, open(sys.argv[1], "w"), ensure_ascii=False, indent=1)
print(f"TOTAL {len(out)}", file=sys.stderr)
