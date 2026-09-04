# ADR-079 — ESP terminal respaldado por naturaleza jurídica propia

**Fecha:** 2026-09-04
**Estado:** Accepted — autoridad candidata de conocimiento offline; runtime sin cambios.
**Evidencia:** [Municipal M2B.1](../gazetteer/MUNICIPAL_M2B_1.md),
[new_identity_rules.json](../../artifacts/audits/municipal_m2b_1/new_identity_rules.json),
[adjudicated_delta.json](../../artifacts/audits/municipal_m2b_1/adjudicated_delta.json).

## Problema y experimento

ADR-076 demuestra que una etiqueta legal puede ser opcional sólo cuando la
naturaleza jurídica de la propia entidad oficial explica exactamente esa
variación. M2B.1 prueba el caso distinto `ESP`: no se hereda la regla ESE ni se
generaliza a formas societarias.

`M2B1_R3_ESP_LEGAL_DESIGNATOR` elimina únicamente `ESP` o `E.S.P.` terminal y
exige que la fila propia SIGEP tenga naturaleza que comience por `Empresas de
Servicios Públicos Domiciliarios`. Empareja 22 perfiles, acepta 16 y rechaza 6;
gana 5.607 procesos únicos y 154 filas físicas de contratos. No se aplica a
producción.

## Decisión acotada

Se acepta la omisión o presencia de `ESP` terminal para formar un candidato de
identidad offline bajo este predicado completo:

1. Se retira a lo sumo una etiqueta terminal `ESP` o su tokenización literal
   `E S P`; debe aparecer en una de las dos superficies.
2. El núcleo restante conserva exactamente los mismos tokens y orden. No se
   retiran ni equiparan `SA`, `SAS`, `LTDA`, números, calificadores territoriales,
   departamentos, palabras de objeto social u otras formas jurídicas.
3. La propia fila SIGEP emparejada, no una fila con igual NIT, declara naturaleza
   de empresa de servicios públicos domiciliarios y acredita la familia.
4. Existe un único código SIGEP en el territorio `VERIFIED`, con alcance completo
   Municipal/Distrital, y la cuenta/nombre aparecen byte-exactos en el directorio.
5. Superficies no explicadas de la misma cuenta, cuentas o entidades competidoras,
   canonicals incompatibles y contradicciones de familia o fiscal conservan
   `REVIEW`.
6. El NIT sólo veta contradicciones. No propone el match, no fusiona cuentas,
   no construye el ID lógico ni acredita territorio. Bogotá conserva su registro.

## Exclusiones y consecuencias

La regla no afirma que `SA`, `SAS` y `LTDA` sean intercambiables y no combina
ESP con otra relajación nueva. No admite fuzzy, substring, corrección nominal,
LLM, identidad por municipio ni consolidación multi-account.

Los 16 perfiles aceptados son candidatos de conocimiento offline. Las cuentas,
grafías y referencias físicas permanecen separadas y no se modifican aliases,
bindings, BuyerGroups o filtros runtime. La unicidad se refiere a las fuentes
congeladas; una fuente oficial ausente mantiene deuda explícita.

## Numeración y validación

El barrido global posterior al experimento encontró ADR-076 como máximo en los
29 worktrees registrados. [adr_scan.json](../../artifacts/audits/municipal_m2b_1/adr_scan.json)
reserva ADR-077, ADR-078 y ADR-079. Los tests contrastan naturaleza propia,
formas SA/SAS incompatibles, conflicto fiscal, cuentas competidoras y ausencia
de selección por NIT.
