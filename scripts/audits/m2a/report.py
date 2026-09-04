"""Render the national knowledge artifact without changing runtime."""
import json


def render(out, coverage, vias, gates, territories, delta):
    u, c = coverage['universe'], coverage['cohorts']
    lines = ['# Municipal M2A — universo institucional y afiliación', '',
        f"**MUNICIPAL_M2A: {gates['MUNICIPAL_M2A']}. PUSH: NO. Promociones: 0.**", '',
        f"SECOPPAL reconoce **{u['TOTAL_RECOGNIZED_BY_SECOPPAL']} de {u['TOTAL_LOGICAL_BUYER_UNITS_VERIFIED']} BuyerUnits municipales VERIFICADAS**, "
        f"con un pool no resuelto de **{u['TOTAL_UNRESOLVED']} perfiles** (afiliación o identidad).", '',
        'La cifra pertenece al denominador conservador M2A; no representa todos los compradores municipales del país.', '',
        '## Granos y denominadores', '', '| Grano | Total |', '|---|---:|',
        *[f'| {k} | {v} |' for k, v in coverage['grains'].items()], '',
        coverage['denominator_contract'], '',
        'Los perfiles se reconcilian exclusivamente por (cuenta, nombre físico byte-exacto), entre datasets. '
        'Las unidades lógicas usan autoridad canónica congelada o identidad SIGEP independiente con cuenta observada única. '
        'Una afiliación territorial verificada sin identidad institucional resuelta permanece fuera del denominador lógico. '
        'Los NIT compartidos sólo anotan vecinos fiscales; ninguna propuesta se aplica.', '',
        '| Cohorte | Perfiles | Procesos físicos | Procesos únicos | Contratos físicos | Contratos únicos derivados |',
        '|---|---:|---:|---:|---:|---:|']
    for name, row in c.items():
        v = row['volumes']
        lines.append(f"| {name} | {row['physical_profiles']} | {v['process_physical_rows']} | {v['unique_processes']} | {v['contract_physical_rows']} | {v['unique_contracts']} |")
    lines += ['', '**Contratos únicos: DERIVED_FROM_GLOBAL_IDENTITY.** El SoQL de contratos congelado mide count(*), '
        'no DISTINCT por perfil. La igualdad global 1.095.013 = 1.095.013 permite derivar esa dimensión sólo en '
        'la ventana [2025-09-03, 2026-09-03). Cada volumen derivado lleva su provenance.', '',
        f"Cobertura del volumen territorial verificado: procesos **{coverage['PROCESS_COVERAGE']:.2%}**, contratos **{coverage['CONTRACT_COVERAGE']:.2%}**.", '',
        '## Vías de afiliación y falsificación', '',
        'Vía A compone la evidencia fiscal del perfil con una afiliación territorial ya acreditada y verifica la '
        'cardinalidad antes de usarla. Sola produce PROBABLE. VERIFIED requiere una fila SIGEP propia emparejada '
        'por identidad institucional o el registro propio byte-exacto de directorio con código concordante; '
        'la segunda evidencia proviene del perfil observado.', '',
        'Vía B distingue el territorio fiscal de la identidad. Su pool se reproduce seleccionando las filas SIGEP '
        'Municipal/Distrital con DIVIPOLA único. Para aplicar la vía se exige además que todas las filas fiscales '
        'tengan alcance único, código municipal válido y ausencia de Bogotá. VERIFIED territorial requiere cruce con directorio propio de cuenta/nombre byte-exacto/NIT '
        'y código municipal único. Ese cruce no se convierte en identidad lógica: la acreditación '
        'de BuyerUnit requiere identidad independiente o autoridad congelada previa.', '',
        'El emparejamiento usa tokens institucionales idénticos o plantilla estricta de rol + municipio completo '
        'con calificadores departamentales. No usa distancia, similitud ni subconjuntos arbitrarios. Los topónimos '
        'compuestos pasan por `decide_identity` de M1A.2, sobre spans completos de mayor longitud. '
        'La naturaleza jurídica sólo se toma de la fila emparejada. La familia inferida por nombre es PROVISIONAL.', '',
        '| Vía | Pool perfiles | Pool strata | Territorios pool | Aplicados | VERIFIED | PROBABLE |', '|---|---:|---:|---:|---:|---:|---:|']
    for key in ('via_a', 'via_b'):
        v = vias[key]; h = v['hypothesis_pool']
        lines.append(f"| {key} | {h['physical_profiles']} | {h['physical_strata']} | {h['territories']} | {v['applied_profiles']} | {v['status_counts'].get('VERIFIED', 0)} | {v['status_counts'].get('PROBABLE', 0)} |")
    b = vias['via_b']
    lines += ['', f"Cruce propio con directorio en el pool B: **{b['directory_crosscheck_profiles']}/{b['hypothesis_pool']['physical_profiles']} ({b['directory_crosscheck_rate']:.2%})**; "
              f"con identidad propia SIGEP: **{b['entity_match_profiles']}**; VERIFIED tras el cruce, entre aplicaciones B: **{b['verified_after_directory_crosscheck']}**.", '',
        'Los pools A/B se solapan; sus cifras nunca se suman para construir el universo.', '',
        '**Contradicción reproducida:** el pool de 1.814 perfiles incluye Personería Municipal de Venecia Antioquia '
        'porque descarta la fila fiscal de Asamblea Departamental de Córdoba. El NIT 890980764 tiene ambas '
        'jurisdicciones SIGEP: la vía fiscal B se cierra para ese perfil. Una fila propia independiente se evalúa '
        'por separado; el conflicto fiscal queda visible. Los códigos fuera del DIVIPOLA municipal vigente '
        'también se excluyen de la aplicación, conservando el pool hipotético.', '',
        'Canario fiscal (presencia de filas propias no equivale a match):', '', '```json', json.dumps(vias['canary'], ensure_ascii=False, indent=2), '```', '',
        'Los contadores anteriores miden contradicciones observadas y controles de seguridad; no son una tasa '
        'de precisión poblacional: las capturas no contienen un oráculo completo de afiliaciones negativas. '
        'El matching conservador sacrifica recall y publica la deuda sin forzar PASS por entidad.', '',
        '## Bogotá y deudas heredadas', '',
        'M0 contiene 174 strata/92 perfiles con código 11001: 76 strata/39 perfiles por el registro ejecutivo '
        'declarado, y 98 strata/53 perfiles por SIGEP. La orden exige exclusivamente la primera autoridad. '
        'M2A conserva los 92 como censo histórico y sólo afirma los 39 del registro declarado. Otros perfiles '
        'con señales de Bogotá se muestran como contexto, sin membresía ni reinterpretación del modelo productivo. '
        'Esta restricción impide comparar directamente el nuevo denominador con los 622 reconocidos del baseline '
        'sin consultar el ledger de cobertura anterior. Los bytes del modelo, bindings y grupos no cambian.', '',
        'Ocaña mantiene la deuda de NIT SIGEP; Calarcá mantiene 63002 frente a 63130. Se reutilizan las '
        'adjudicaciones M1A.2A y se conservan las contradicciones; esta auditoría no las resuelve.', '',
        '## Reconciliación del residuo acreditado anterior', '',
        '**2.513 + 13.272 = 15.785 procesos únicos**, 138 perfiles/96 territorios. '
        '**1.711 + 10.570 = 12.281 filas de contratos**. '
        'El primer sumando corresponde a 39 perfiles/36 territorios residuales; el segundo a 99 perfiles '
        'en 60 territorios ya reconocidos. El ledger en coverage.json enumera cada perfil y explica el segundo '
        'sumando por familia, sin atribuir toda la actividad territorial a su ejecutivo.', '',
        '| Familia del residuo en territorios reconocidos | Perfiles | Procesos únicos | Contratos físicos |', '|---|---:|---:|---:|']
    for family, r in coverage['inside_recognized_residue_by_family'].items():
        lines.append(f"| {family} | {len(r['profiles'])} | {r['volumes']['unique_processes']} | {r['volumes']['contract_physical_rows']} |")
    lines += ['', '## Delta M1B y familias', '',
        'Se consumen por referencia los 288 perfiles y 262 señales sólo de registro de M1B; no se vuelve '
        'a ejecutar su descubrimiento. Las 39 filas residuales conservan deuda y disposición. El único '
        'cambio taxonómico permitido separa EDUCATION de OTHER_DECENTRALIZED, explicitado por perfil en '
        'family_mapping_m1b.json. Las asociaciones municipales permanecen OTHER_DECENTRALIZED.', '',
        '| Familia | Unidades lógicas verificadas | Perfiles con afiliación verificada | Pool no resuelto |', '|---|---:|---:|---:|']
    for family, r in coverage['by_family'].items():
        lines.append(f"| {family} | {r['verified_logical_units']} | {r['verified_affiliation_profiles']} | {r['unresolved_profiles']} |")
    lines += ['', '## Controles territoriales', '',
        '| DIVIPOLA | Municipio | Familias lógicas observadas | Reconocidas | Sin reconocer | Contexto pendiente |', '|---|---|---|---:|---:|---:|']
    controls = {'08078', '68001', '08001', '13836', '54498', '63130', '68081', '76892', '05036', '05142', '52573', '85279', '11001'}
    for t in territories:
        if t['divipola'] in controls:
            lines.append(f"| {t['divipola']} | {t['municipality']} | {', '.join(t['institutional_families'])} | {t['recognized_count']} | {t['unrecognized_count']} | {t['unresolved_count']} |")
    lines += ['', 'Baranoa es el TerritoryProfile 08078; su ejecutivo es un elemento de executive[]. '
        'Cualquier otra unidad acreditada observada tiene identidad propia. El territorio no se reemplaza por la Alcaldía.', '',
        'Se publican los 1.103 TerritoryProfiles. Sus perfiles pendientes son referencias de contexto y pueden '
        'aparecer en varios territorios candidatos; no son un denominador sumable ni membresía afirmada.', '',
        '## Acciones futuras y reproducibilidad', '',
        'Los rankings son deterministas: métrica descendente, DIVIPOLA ascendente, ID lógico estable. '
        'La complejidad usa número de familias y luego número de unidades. No se usa valor monetario. '
        'READY_FOR_REGISTRY indica preparación de una ficha de conocimiento; no autoriza promoción. '
        'READY_FOR_BUYER_PROMOTION requiere adjudicación/pipeline posterior y permanece vacío. '
        'Los candidatos M1B retienen su disposición por referencia.', '',
        'Los artefactos cubren perfiles, unidades, cadenas de afiliación, vías, familias, mapeo M1B, '
        'territorios, cobertura, pendientes, rankings, hashes y gates. No hay nuevas fuentes de red.', '',
        '```bash', '.venv/bin/python3 scripts/audits/audit_municipal_m2a.py',
        '.venv/bin/python3 scripts/audits/audit_municipal_m2a.py --output /tmp/municipal_m2a_replay', '```', '',
        '**LLM_EXECUTION_MODE: DETERMINISTIC_OFFLINE_NO_LLM.** El ejecutor bloquea conexiones de socket '
        'antes de leer insumos. Los archivos productivos y autoridades históricas llevan SHA-256 antes/después '
        'en byte_identity.json. La validación se adjunta en validation.json; si algún byte protegido cambia '
        'la suite completa es obligatoria.', '',
        '**ADR_REQUIRED: YES.** Las cadenas aceptadas constituyen una autoridad reusable para auditoría offline, '
        'con límites de identidad y Bogotá explícitos. El ADR se escribe después de contrastar el experimento '
        'y verificar el máximo global de los worktrees. Véase '
        '[ADR-075](../decisions/2026-09-04_ADR-075_municipal-territorial-affiliation-evidence-chains.md) '
        'y el barrido retenido en adr_scan.json. **NEXT: M2B_ADJUDICATION.**', '']
    bounds = coverage['municipal_volume_bounds']
    lines += ['## Incertidumbre de volumen', '',
        f"Procesos municipales: cota inferior {bounds['unique_processes']['lower']:,}; superior {bounds['unique_processes']['upper']:,}. "
        f"Contratos únicos derivados: {bounds['unique_contracts']['lower']:,} a {bounds['unique_contracts']['upper']:,}.", '',
        'La cota superior aumenta frente a M0 porque M2A deja de usar el orden registral como autoridad '
        'para excluir perfiles nacionales y aplica la restricción de Bogotá. No se atribuye todo ese pool '
        'al universo municipal. La cota inferior incorpora sólo afiliaciones verificadas, incluyendo las '
        'que todavía requieren acreditar identidad lógica.', '',
        'El principal trabajo de M2B será adjudicar identidad propia de salud y otros descentralizados: '
        'un código territorial concordante no resuelve nombres oficiales abreviados ni múltiples cuentas. '
        'No se relaja el matching para aumentar ese denominador.', '']
    (out / 'MUNICIPAL_M2A.md').write_text('\n'.join(lines))
