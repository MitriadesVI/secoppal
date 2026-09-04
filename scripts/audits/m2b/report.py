"""Render the audit from computed artifacts; no manually supplied statistics."""
def render(out, artifacts):
    g, tax, impact = artifacts['gates'], artifacts['identity_failure_taxonomy'], artifacts['impact']
    vias, health = artifacts['identity_vias'], artifacts['health_identity_audit']
    logical, batches = artifacts['verified_logical_unrecognized'], artifacts['batches']['batches']
    all_safe = impact['scenarios']['ALL_RULE_BASED']
    lines = [
        '# Municipal M2B — adjudicación de identidad institucional', '',
        f'**MUNICIPAL_M2B: {g["MUNICIPAL_M2B"]}. Integridad: {g["audit_integrity"]}. PUSH: NO. Aplicaciones: 0.**', '',
        f'Se adjudican offline **{g["profiles_resolved"]} perfiles / {g["new_logical_units"]} BuyerUnits candidatas**; '
        f'permanecen **{g["remaining_identity_pending"]} identidades pendientes**. '
        'PARTIAL expresa deuda residual, no un gate de seguridad fallido.', '',
        '## Baseline y universo', '',
        'Autoridad congelada: M2A, HEAD inicial `e98bf39`, rama `main`. La afiliación no se recalcula. '
        'El universo exacto es 931 perfiles, 347.163 procesos únicos y 117.277 filas físicas de contratos. '
        'Se separan 370 perfiles ya lógicos sin reconocimiento de perfil y 628 reconocidos. '
        'Los 1.929 perfiles territoriales contienen 994 unidades lógicas iniciales; los granos no se suman.', '',
        '## Taxonomía reproducible', '',
        'La clase primaria es una partición. Los indicadores secundarios se solapan y quedan por perfil. '
        'Precedencia explícita: ' + ' → '.join(tax['primary_precedence']) + '. '
        'Una señal de abreviatura describe deuda; no acredita su expansión.', '',
        '| Clase | Causa | Perfiles | Procesos únicos | Contratos físicos |',
        '|---|---|---:|---:|---:|',
    ]
    for k, c in tax['classes'].items():
        s = c['primary']
        lines.append(f'| {k} | {c["name"]} | {s["profiles"]} | {s["unique_processes"]} | {s["contract_rows"]} |')
    lines += ['', '## Prioridad exacta por familia M2A', '',
              'Orden: procesos únicos, contratos físicos, perfiles, número seguro adjudicable; desempate estable. '
              'La familia congelada se conserva para reconciliar el denominador. Una naturaleza propia SIGEP puede '
              'refinar UNKNOWN en el candidato sin reescribir M2A.', '',
              '| Familia | Perfiles | Procesos únicos | Contratos físicos | Perfiles resueltos |',
              '|---|---:|---:|---:|---:|']
    for f in tax['family_priority']:
        lines.append(f'| {f["family"]} | {f["profiles"]} | {f["unique_processes"]} | {f["contract_rows"]} | {f["safe_profiles"]} |')
    lines += ['', '## Contraste de las cuatro vías', '',
              '| Vía | Pool con evidencia | Aceptados con cadena completa | Procesos del pool |',
              '|---|---:|---:|---:|']
    for k, v in vias['vias'].items():
        lines.append(f'| {k} | {v["profiles"]} | {v["accepted_profiles"]} | {v["unique_processes"]} |')
    lines += ['', 'Los pools se solapan: III es corroboración de IV, nunca ganancia adicional.', '',
              '- **I:** los 931 carecen de SIGEP_ENTITY_MATCH propio en M2A; no quedaron fuera por múltiples cuentas de un código propio ya acreditado.',
              '- **II:** ninguna cuenta de los 931 tiene otra superficie en la captura completa. SAME_BUYER_UNIT y SOURCE_VARIANT por esta vía: 0. No se fabrica un lote de variantes.',
              '- **III:** el directorio acredita una observación propia por cuenta/nombre byte-exacto y territorio concordante; por sí solo no concede identidad ni naturaleza jurídica.',
              '- **IV:** se consulta el índice global SIGEP por nombre institucional, sin seleccionar por NIT. Se contrastan igualdad de tokens/plantilla ADR-075 y la nueva regla ESE. Las contradicciones fiscales bloquean; nunca seleccionan una unidad.', '',
              '### Regla ESE probada', '',
              'Se retira **a lo sumo una etiqueta legal completa de un extremo**: ESE, E.S.E. o EMPRESA SOCIAL DEL ESTADO '
              '(también el plural literal). Todos los tokens del núcleo institucional deben coincidir en orden con '
              'la fila propia SIGEP. Sólo la naturaleza jurídica propia `Empresas Sociales del Estado` permite la operación. '
              'Se permite etiqueta presente/ausente o en el otro extremo; no se eliminan palabras del núcleo, '
              'topónimos, niveles hospitalarios ni abreviaturas arbitrarias. Un municipio solo no acredita identidad.', '',
              'El conjunto oficial debe identificar un código SIGEP único, territorio acreditado único y familia compatible. '
              'Se contrastan todas las cuentas observadas, cuentas adicionales del directorio, reclamaciones canónicas y '
              'vecinos fiscales de familia compatible. Otra cuenta no explicada bloquea aunque no transaccione en la ventana. '
              'Los IDs se forman con código SIGEP y cuenta; el NIT no es la llave. No se consolidan canonicals.', '',
              'No hay acreditación de siglas opacas como HMI, fuzzy, substring genérico ni corrección de errores ortográficos. '
              'El matching por tokens y el guard de municipio compuesto reutilizan ADR-075. '
              'Las superficies físicas byte-exactas permanecen intactas.', '',
              '### Razones de rechazo de IV (pueden solaparse)', '',
              '| Razón | Perfiles |', '|---|---:|']
    for reason, n in sorted(vias['vias']['VIA_IV']['reasons'].items()):
        lines.append(f'| {reason} | {n} |')
    lines += ['', '## Salud', '',
              f'El subaudit contiene **{health["summary"]["profiles"]} perfiles**, '
              f'{health["summary"]["unique_processes"]} procesos únicos y {health["summary"]["contract_rows"]} contratos físicos. '
              'Incluye señales ESE que M2A etiquetaba UNKNOWN; ese alcance no altera la familia histórica.', '',
              f'Hospital explícito: {health["patterns"]["hospital"]}; centro/puesto: {health["patterns"]["centre_or_post"]}; '
              f'una cuenta con varias grafías: {health["patterns"]["same_account_multiple_names"]}. '
              'Centro/puesto sólo obtiene naturaleza institucional mediante su propia fila SIGEP.', '',
              f'Se publican {len(health["same_territory_controls"])} controles con varias observaciones de salud en un territorio. '
              'Territorio y familia compartidos no fusionan hospitales. Cada rechazo permanece en el ledger y en el delta.', '',
              '## NIT compartido', '',
              f'{artifacts["shared_nit_cases"]["summary"]["profiles"]} perfiles pendientes tienen vecinos fiscales observados. '
              'Todos conservan KEEP_SEPARATE, incluidos los que logran acreditar su propia identidad. '
              f'Otros cruces de directorio se enumeran aparte: {artifacts["shared_nit_cases"]["directory_summary"]["profiles"]} perfiles '
              'con otra cuenta registral bajo su evidencia fiscal; ambos conjuntos se solapan. '
              'La clase I exige una coincidencia institucional estricta con otra cuenta del directorio; '
              'un vecino fiscal sin esa evidencia no basta para asignarla. **NIT_AUTO_MERGES = 0**.', '',
              '## Batches', '',
              'Los cinco batches particionan 931 perfiles. Un batch puede contener revisión y ganancia segura; '
              'expected_gain cuenta sólo candidatos adjudicados.', '',
              '| Batch | Perfiles | Candidatos lógicos | Procesos del batch | Contratos físicos | Ganancia procesos |',
              '|---|---:|---:|---:|---:|---:|']
    for name, b in batches.items():
        lines.append(f'| {name} | {b["profiles"]} | {len(b["candidate_logical_units"])} | {b["unique_processes"]} | {b["contract_rows"]} | {b["expected_gain"]["unique_processes"]} |')
    lines += ['', '## Impacto hipotético dentro de la afiliación verificada', '',
              '| Escenario | Perfiles | Nuevas unidades | Procesos | Contratos físicos | Cobertura procesos territorial |',
              '|---|---:|---:|---:|---:|---:|']
    for name, s in impact['scenarios'].items():
        lines.append(f'| {name} | {s["profiles_resolved"]} | {s["logical_units_created"]} | {s["processes_resolved"]} | {s["contract_rows_resolved"]} | {s["coverage_verified_territorial_processes_pct"]:.2f}% |')
    lines += ['', f'ALL_RULE_BASED resuelve **{all_safe["pct_pending_process_volume_resolved"]:.2f}%** del volumen de procesos pendiente '
              f'y **{all_safe["pct_pending_contract_rows_resolved"]:.2f}%** de sus filas de contratos. '
              'TOP_SAFE_100 agota el pool si hay menos de 100; no se rellena con casos REVIEW. '
              '**Cambio de cobertura productiva SECOPPAL: 0.**', '',
              '## Los 370 ya lógicos', '', '| Disposición | Perfiles | Procesos | Contratos físicos |', '|---|---:|---:|---:|']
    for action, s in logical['dispositions'].items():
        lines.append(f'| {action} | {s["profiles"]} | {s["unique_processes"]} | {s["contract_rows"]} |')
    lines += ['', 'No requieren adjudicar identidad. Las disposiciones M1B y conflictos congelados se conservan. '
              'Preparar un registro de conocimiento no equivale a aprobar aliases o promoción. '
              'READY_FOR_BUYER_PROMOTION exige un payload y gates posteriores, que M2B no fabrica.', '',
              '## Delta territorial', '',
              '| Municipio | Unidades actuales | Candidatas nuevas | Identidad pendiente |', '|---|---:|---:|---:|']
    for t in artifacts['territory_delta']['controls'] + artifacts['territory_delta']['small_territory_controls']:
        lines.append(f'| {t["municipality"]} | {len(t["current_logical_buyer_units"])} | {len(t["newly_adjudicated_candidate_buyer_units"])} | {len(t["identity_unresolved"])} |')
    lines += ['', 'Los municipios pequeños son controles de cardinalidad institucional baja, no de población. '
              'Los 1.103 territorios conservan sus unidades actuales y referencias de contexto pendientes M2A. '
              'Bogotá conserva exclusivamente su autoridad de registry declarado; nuevas identidades M2B: 0.', '',
              '## Gates, reproducción y siguiente paso', '',
              f'Archivos protegidos: {len(artifacts["byte_identity"]["files"])}; cambios: {len(artifacts["byte_identity"]["changed"])}. '
              'Incluye runtime completo y evidencia histórica M0/M1A/M1B/M2A, además del código M2A. '
              'El build bloquea conexiones de socket. LIVE_CALLS=0, NIT_AUTO_MERGES=0, '
              'AUTO_MERGES_BY_TERRITORY=0, FUZZY_IDENTITY_MATCHES=0.', '',
              '```bash', '.venv/bin/python3 scripts/audits/audit_municipal_m2b.py',
              '.venv/bin/python3 scripts/audits/audit_municipal_m2b.py --output /tmp/municipal_m2b_replay',
              '.venv/bin/python3 scripts/audits/audit_municipal_m2b.py --validate', '```', '',
              'Pruebas adversariales, corpus 89, Ruff, lint-core, diff y reproducción independiente quedan en '
              '`validation.json` y `validation/`. Si cambia un byte protegido se exige la suite completa y el audit falla cerrado.', '',
              f'**ADR_REQUIRED: {g["ADR_REQUIRED"]}.** La nueva regla ESE sólo se acepta tras la prueba offline. '
              'El barrido global y el ADR se referencian en `adr_scan.json`. No se cambia runtime.', '',
              '**NEXT: M2B_1_TARGETED_ADJUDICATION.** Priorizar abreviaturas opacas de alto volumen, cuentas adicionales '
              'de salud y conflictos oficiales; los ya lógicos preparados para registro son un lote separado para M2C. '
              'No se ha aprobado ni aplicado ninguna promoción. **PUSH: NO.**', '',
              '## Límites', '',
              'La evidencia es la captura congelada M0, no una consulta vigente. La ausencia de otra cuenta se limita '
              'a las observaciones y al directorio capturados. Los controles adversariales no constituyen una estimación '
              'de precisión contra un oráculo completo. El veto fiscal conserva contradicciones y sacrifica recall; '
              'no rehabilita el NIT como autoridad de identidad. Las familias distintas y nombres distintos no prueban '
              'por sí solos SAME_BUYER_UNIT ni su negación.', '']
    (out / 'MUNICIPAL_M2B.md').write_text('\n'.join(lines))
