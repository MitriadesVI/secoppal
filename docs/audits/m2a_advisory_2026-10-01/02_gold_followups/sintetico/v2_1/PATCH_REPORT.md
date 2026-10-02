# PATCH_REPORT v2.1

Base: `../followup_synthetic_cases.jsonl` (162 casos) -> `followup_synthetic_cases.jsonl` (164 casos).

## 1. `prior.pending` ampliado con `span` y `candidates` (17 ids)
SF047, SF048, SF049, SF050, SF051, SF052, SF053, SF055, SF056, SF057, SF058, SF059, SF060, SF061, SF062, SF063, SF064

- SF047–SF053, SF055–SF057, SF063, SF064 (periodo sin año): `span = "el primer semestre"`, `candidates = []` (pregunta abierta, `options = 0`).
- SF058: `span = "San Pedro"`; 3 candidatos de BUYER (San Pedro del Valle del Cauca, San Pedro de Urabá, San Pedro de Sucre).
- SF059, SF060: `span = "Soledad"`; 2 candidatos: BUYER (Alcaldía de Soledad) y GEOGRAPHY (municipio de Soledad).
- SF061, SF062: `span = "Santa Rosa"`; 3 candidatos de BUYER (Santa Rosa de Cabal, de Osos, del Cauca).
- SF064 tiene `state = EXPIRED` (no PENDING) pero conserva `pending`; lo amplié igual por coherencia con SF063. Si no se quiere, revertir ese id.

## 2. Casos nuevos
- SF163 «con Soacha» y SF164 «con la Alcaldía de Soacha»: previo EXECUTED, `ACTOR = Ingeniería y Vías del Páramo S.A.S.` (ficticio),
  universo CONTRACTS, gap 45 s. El gemelo cambia un solo rasgo: «la Alcaldía de» deja a X sólo como comprador.

## 3. `FORM_CONVENTIONS.md` escrito sin cambiar convenciones.

## 4. Diff de campos (existentes vs v2.1)
Comparación por campo de los 162 ids originales: ningún `followup_text`, `followup_sha256`, `slice`, `source_kind`, `gold_id` ni campo de
`prior` distinto de `pending` difiere. En `pending` sólo se añaden `span` y `candidates`; `dimension`, `reason` y `options` quedan idénticos
(verificado por aserción). Diferencias totales fuera de `pending`: 0.

## 5. Comprobaciones
`similarity_check.json` sobre los 164 casos: 0 rechazados, máximo Jaccard 0,300 contra REAL y 0,444 contra público (los dos nuevos
quedan por debajo); sin patrones de cédula/NIT/correo/teléfono. Los 162 `followup_sha256` originales son idénticos.
