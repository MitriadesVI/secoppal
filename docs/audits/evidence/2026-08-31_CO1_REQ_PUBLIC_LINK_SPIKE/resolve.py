"""Intenta resolver cada fila de la muestra a una URL pública oficial,
por dos vías candidatas: referencia_del_proceso+nit_entidad, e id_del_portafolio.
Sin terceros: única fuente = datos.gov.co (p6dx-8zbt)."""
import json, urllib.parse, urllib.request, sys, time

BASE = "https://www.datos.gov.co/resource/p6dx-8zbt.json"

def get(params, tries=3):
    url = BASE + "?" + urllib.parse.urlencode(params)
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                return json.load(r)
        except Exception as exc:            # noqa: BLE001
            if i == tries - 1:
                return {"_error": str(exc)}
            time.sleep(3)

def q(s):
    return "'" + str(s).replace("'", "''") + "'"

def url_of(row):
    u = row.get("urlproceso")
    if isinstance(u, dict):
        u = u.get("url")
    return u or ""

def public(u):
    return "OpportunityDetail" in u and "noticeUID=CO1.NTC." in u

sample = json.load(open(sys.argv[1]))
out = []
for row in sample:
    rec = {k: row.get(k) for k in
           ("_stratum", "id_del_proceso", "referencia_del_proceso",
            "id_del_portafolio", "estado_del_procedimiento", "fase",
            "entidad", "nit_entidad")}
    rec["urlproceso_raw"] = url_of(row)
    rec["url_class"] = ("public" if public(rec["urlproceso_raw"])
                        else "login" if "Login" in rec["urlproceso_raw"]
                        else "other")

    ref, nit = row.get("referencia_del_proceso"), row.get("nit_entidad")
    if ref and nit:
        r = get({"$select": "id_del_proceso,urlproceso",
                 "$where": f"referencia_del_proceso={q(ref)} AND nit_entidad={q(nit)}",
                 "$limit": 50})
        if isinstance(r, dict):
            rec["by_reference"] = {"error": r["_error"]}
        else:
            urls = sorted({url_of(x) for x in r if public(url_of(x))})
            rec["by_reference"] = {"rows": len(r), "distinct_public_urls": len(urls),
                                   "urls": urls[:3]}
    else:
        rec["by_reference"] = {"rows": 0, "distinct_public_urls": 0, "urls": []}

    port = row.get("id_del_portafolio")
    if port:
        r = get({"$select": "id_del_proceso,urlproceso",
                 "$where": f"id_del_portafolio={q(port)}", "$limit": 50})
        if isinstance(r, dict):
            rec["by_portfolio"] = {"error": r["_error"]}
        else:
            urls = sorted({url_of(x) for x in r if public(url_of(x))})
            rec["by_portfolio"] = {"rows": len(r), "distinct_public_urls": len(urls),
                                   "urls": urls[:3]}
    else:
        rec["by_portfolio"] = {"rows": 0, "distinct_public_urls": 0, "urls": []}
    out.append(rec)
    print(f"{rec['id_del_proceso']:22} {rec['url_class']:7} "
          f"ref={rec['by_reference'].get('distinct_public_urls')} "
          f"port={rec['by_portfolio'].get('distinct_public_urls')}", file=sys.stderr)

json.dump(out, open(sys.argv[2], "w"), ensure_ascii=False, indent=1)
