# ADR-078 — Nivel sanitario terminal como calificador exacto

**Fecha:** 2026-09-04
**Estado:** Accepted — autoridad candidata de conocimiento offline; runtime sin cambios.
**Evidencia:** [Municipal M2B.1](../gazetteer/MUNICIPAL_M2B_1.md),
[new_identity_rules.json](../../artifacts/audits/municipal_m2b_1/new_identity_rules.json),
[adjudicated_delta.json](../../artifacts/audits/municipal_m2b_1/adjudicated_delta.json).

## Problema y experimento

Algunas fuentes sanitarias incluyen el nivel de atención al final de la razón
institucional y otras publican el mismo núcleo sin ese calificador. ADR-076 no
permite suprimir `NIVEL II` ni otros términos: cualquier ampliación requiere una
hipótesis independiente.

`M2B1_R2_TRAILING_CARE_LEVEL` prueba sólo `NIVEL I/II/III/1/2/3` terminal después
del manejo ESE de ADR-076. En el delta dirigido empareja y acepta un perfil, con
2.643 procesos únicos y una fila física de contrato; no hay rechazo ni cambio
productivo. El resultado pequeño no autoriza una normalización sanitaria general.

## Decisión acotada

Se acepta el nivel sanitario terminal como calificador para un candidato de
identidad offline sólo si se cumple todo lo siguiente:

1. Una de las superficies termina exactamente en `NIVEL I`, `NIVEL II`,
   `NIVEL III`, `NIVEL 1`, `NIVEL 2` o `NIVEL 3`; la otra conserva el mismo
   núcleo institucional sin ese sufijo.
2. Se retira a lo sumo una ocurrencia terminal. Un nivel interno, una frase de
   complejidad, sede, unidad, clínica o cualquier otro descriptor no participa.
3. El núcleo restante tiene la misma secuencia exacta de tokens y satisface la
   identidad propia ESE de ADR-076. No se combina con el retiro de topónimo de
   ADR-077 ni con otra relajación nueva.
4. El territorio `VERIFIED`, la entidad SIGEP única, la naturaleza propia, la
   familia compatible, el directorio byte-exacto y la ausencia de cuentas,
   entidades y canonicals competidores satisfacen la cadena completa.
5. Una contradicción fiscal bloquea, pero el NIT nunca crea ni elige identidad,
   cuenta o ID lógico. Bogotá permanece reservada a su registro declarado.

## Exclusiones y consecuencias

La decisión no equipara niveles entre sí, no elimina tokens internos, no prueba
que dos sedes o niveles sean una BuyerUnit y no admite fuzzy, substring, LLM o
unión por NIT. La regla sólo produce candidatos offline y conserva las grafías,
cuentas y evidencia originales.

El único caso aceptado hace falsable el predicado, pero también limita su
generalidad empírica. Cualquier uso futuro debe conservar los mismos canarios y
no extrapolarlo a nuevos vocabularios sanitarios sin otro experimento.

## Numeración y validación

El barrido global posterior al experimento encontró ADR-076 como máximo en 29
worktrees y reservó consecutivamente ADR-077, ADR-078 y ADR-079. Evidencia:
[adr_scan.json](../../artifacts/audits/municipal_m2b_1/adr_scan.json). Los tests
recorren las seis grafías permitidas y rechazan nivel interno y texto libre de
complejidad.
