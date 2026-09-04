"""Render the computed M2B.1 adjudication and its practical stopping signal."""


def render(out, artifacts):
    g = artifacts['gates']
    abbrev = artifacts['abbreviation_taxonomy']
    acronym = artifacts['acronym_registry_audit']
    directory = artifacts['directory_exact_audit']
    accounts = artifacts['multi_account_audit']
    conflicts = artifacts['official_conflicts']
    rules = artifacts['new_identity_rules']['rules']
    top = artifacts['top_volume']['scenarios']
    impact = artifacts['impact']
    delta, cumulative = impact['m2b1_delta'], impact['cumulative_m2b']
    lines = ['# Municipal M2B.1 — adjudicación dirigida de identidad de alto volumen', '',
        f'**MUNICIPAL_M2B_1: {g["MUNICIPAL_M2B_1"]}. Integridad: {g["audit_integrity"]}. PUSH: NO. Aplicaciones: 0.**', '',
        f'M2B.1 adjudica **{delta["profiles"]} perfiles**: {delta["new_logical_units"]} unidades lógicas candidatas nuevas '
        f'y {delta["existing_canonical_profiles_linked"]} perfiles enlazados a canonicals ya declarados. '
        f'La ganancia es **{delta["unique_processes"]} procesos únicos** y **{delta["contract_rows"]} filas físicas de contratos**. '
        'Los 55 candidatos M2B permanecen congelados y no se readjudican.', '',
        '## Reconciliación del universo', '',
        '| Clase M2B | Perfiles | Procesos únicos | Contratos físicos | Perfiles nuevos auditables |',
        '|---|---:|---:|---:|---:|']
    for key in ('F', 'C', 'I', 'H'):
        row = g['INPUT'][key]
        lines.append(f'| {key} | {row["profiles"]} | {row["unique_processes"]} | {row["contract_rows"]} | '
                     f'{len(artifacts["new_identity_rules"]["target_new_by_class"][key])} |')
    lines += ['', 'Los conteos F/C/I/H reproducen byte por byte la taxonomía M2B. Suman 552 perfiles; '
        '55 pertenecen al lote ya aceptado, por lo que el delta dirigido contiene 497. '
        'Los 379 perfiles J completan los 876 pendientes y no se atacan como lote.', '',
        '## F — patrones de abreviación', '',
        '| Patrón | Perfiles | Procesos | Contratos físicos | Expansión oficial disponible | Delta aceptado |',
        '|---|---:|---:|---:|---:|---:|']
    for name, row in abbrev['patterns'].items():
        lines.append(f'| {name} | {row["profiles"]} | {row["unique_processes"]} | {row["contract_rows"]} | '
                     f'{row["official_expansion_available"]} | {row["m2b1_delta_accepted"]} |')
    lines += ['', 'Estas clases son descriptivas. ESE, ESP, una sigla o un nombre corto no acreditan identidad por sí mismos. '
        'Los errores, truncamientos y ruido permanecen visibles; no se corrigen por similitud.', '',
        '## Siglas opacas', '',
        f'Se examinan los {acronym["profiles_examined"]} perfiles todavía pendientes tras M2B. '
        f'El inventario contiene {acronym["acronym_occurrences"]} ocurrencias estructurales: ' +
        ', '.join(f'{k}={v}' for k, v in acronym['classifications'].items()) + '. ',
        'Una expansión sólo es elegible si la propia fuente oficial imprime literalmente la sigla y la cadena completa '
        'de territorio, familia, cuenta y competidores pasa. Derivar iniciales de un nombre está prohibido. '
        'HMI, INDER o IMDER no se expanden por conocimiento común ni por parecido.', '',
        '## C — directorio exacto', '',
        f'Los 29 perfiles se clasifican como ' + ', '.join(f'{k}={v}' for k, v in directory['categories'].items()) + '. '
        f'Disposiciones: ' + ', '.join(f'{k}={v}' for k, v in directory['dispositions'].items()) + '. ',
        'No se añadió un registro externo ni una llamada viva. DIRECTORY_ONLY concede cero identidades. '
        'El enlace a un canonical existente exige además identidad propia ADR-076 y ausencia de cuentas competidoras.', '',
        '## I — cuentas múltiples', '',
        'Clasificación: ' + ', '.join(f'{k}={v}' for k, v in accounts['classifications'].items()) + '. ',
        'REGISTRY_ONLY_ACCOUNT significa que la cuenta aparece en el directorio y no en la ventana transaccional congelada. '
        'No se la llama histórica: la captura agregada no prueba sucesión temporal. Fusiones productivas: 0.', '',
        '## H — conflictos oficiales', '',
        'Tipos observados, con solapamiento: ' + ', '.join(f'{k}={v}' for k, v in conflicts['conflict_counts'].items()) + '. '
        f'Resueltos: {conflicts["resolved"]}; KEEP_REVIEW: {conflicts["keep_review"]}. '
        'No apareció una autoridad independiente superior; ninguna contradicción se ignora o decide por mayoría.', '',
        '## Reglas contrastadas', '',
        '| Regla | Matched | Aceptados | Rechazados | Procesos ganados | Contratos físicos | Resultado |',
        '|---|---:|---:|---:|---:|---:|---|']
    for name, row in rules.items():
        outcome = ('COMPOSICIÓN ADR EXISTENTE' if name.startswith('M2B1_R0') and row['profiles_accepted'] else
                   'ACEPTADA' if row['profiles_accepted'] else 'SIN ADJUDICACIÓN')
        lines.append(f'| {name} | {row["profiles_matched"]} | {row["profiles_accepted"]} | '
                     f'{row["profiles_rejected"]} | {row["process_gain"]} | {row["contract_gain"]} | {outcome} |')
    lines += ['', 'Cada trial agrega como máximo una regla nueva después del manejo legal ESE ya aceptado por ADR-076:', '',
        '1. **R0** enlaza un perfil byte-exacto a su canonical declarado cuando la identidad propia ADR-076 y la cuenta del directorio están completas. No agrega normalización.',
        '2. **R1** trata el municipio completo acreditado sólo como calificador terminal. Tras retirarlo debe quedar un núcleo institucional distintivo con secuencia exacta de tokens; territorio y familia nunca son el núcleo.',
        '3. **R2** permite únicamente NIVEL I/II/III/1/2/3 al final del nombre sanitario oficial. No elimina niveles internos ni otros calificadores.',
        '4. **R3** permite únicamente ESP terminal cuando la propia naturaleza SIGEP es una empresa de servicios públicos domiciliarios. No equipara SA, SAS o LTDA.',
        '5. **R4** exige una sigla impresa literalmente y delimitada en el nombre oficial. No obtuvo ninguna adjudicación.', '',
        'Todas exigen un único código SIGEP, territorio congelado, familia propia compatible, cuenta/nombre byte-exacto '
        'en directorio, NIT concordante sólo como veto y explicación independiente de cuentas vecinas. '
        'NIT no escoge identidad ni ID lógico. Un segundo código, cuenta, canonical o conflicto conserva REVIEW.', '',
        '## Salud y canarios', '',
        f'El subaudit de salud cubre {artifacts["health_patterns"]["input"]["profiles"]} perfiles del delta dirigido. '
        'Hospital local, razón social larga, calificador territorial, nivel y sigla se publican como hipótesis separadas. '
        'No se combinaron dos relajaciones nuevas. Dos hospitales del mismo territorio o NIT conservan unidades distintas.', '',
        '## Concentración de volumen', '',
        '| Corte | Procesos | Share del target | Share de los 876 pendientes | Aceptados en el corte |',
        '|---|---:|---:|---:|---:|']
    for name, row in top.items():
        lines.append(f'| {name} | {row["unique_processes"]} | {row["target_process_volume_share_pct"]:.2f}% | '
                     f'{row["remaining_process_volume_share_pct"]:.2f}% | {row["accepted"]["profiles"]} |')
    stop = impact['stop_condition']
    lines += ['', '## Impacto y stop condition', '',
        f'El acumulado M2B+M2B.1 es **{cumulative["total_profiles_resolved"]} perfiles**, '
        f'{cumulative["total_logical_candidates"]} unidades candidatas nuevas más '
        f'{cumulative["existing_canonical_profiles_linked"]} enlaces a unidades existentes, y '
        f'{cumulative["remaining_identity_pending"]} pendientes. Resuelve {cumulative["process_gain"]} procesos '
        f'({cumulative["pct_process_volume_resolved"]:.2f}% del pool M2B original) y {cumulative["contract_gain"]} filas '
        f'({cumulative["pct_contract_volume_resolved"]:.2f}%). La cobertura productiva cambia 0.', '',
        f'**M2B_IDENTITY_DIMINISHING_RETURNS = {stop["M2B_IDENTITY_DIMINISHING_RETURNS"]}.** '
        f'El delta alcanza exactamente {stop["actual_profiles"]} perfiles y {stop["actual_additional_process_pct"]:.2f}% '
        'del volumen original. Cumple la rama de 50 perfiles del umbral OR, aunque no la rama de 10% de volumen.', '',
        '**Sugerencia para los edges:** no abrir M2B.2 como barrido general. Sólo justificarlo con una fuente oficial '
        'con cuentas fechadas o expansiones institucionales explícitas. En paralelo, los 314 registry-only ya lógicos '
        'son mejor insumo para TERRITORIAL_KNOWLEDGE_FOUNDATION que nuevas reglas nominales.', '',
        f'**NEXT: {g["NEXT"]}.** ADR_REQUIRED: {g["ADR_REQUIRED"]}. '
        'R1, R2 y R3 son semánticamente distintas y se documentan por separado después del barrido global.', '',
        '## Reproducción y límites', '', '```bash',
        '.venv/bin/python3 scripts/audits/audit_municipal_m2b1.py',
        '.venv/bin/python3 scripts/audits/audit_municipal_m2b1.py --output /tmp/municipal_m2b1_replay',
        '.venv/bin/python3 scripts/audits/audit_municipal_m2b1.py --validate', '```', '',
        'El ejecutor bloquea sockets y no invoca LLM. Los ceros de WRONG_IDENTITY, fuzzy y merge indican que ningún '
        'candidato aceptado viola los canarios ejecutables; no son una estimación poblacional contra un oráculo completo. '
        'La ausencia de actividad no fecha una cuenta. La captura congelada puede estar incompleta, por lo que la falta '
        'de expansión oficial queda como deuda, no como evidencia negativa.', '']
    (out / 'MUNICIPAL_M2B_1.md').write_text('\n'.join(line.rstrip() for line in lines))
