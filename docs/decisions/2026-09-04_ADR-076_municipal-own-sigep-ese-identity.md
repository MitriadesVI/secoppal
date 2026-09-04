# ADR-076 — Identidad propia ESE por núcleo institucional exacto y naturaleza SIGEP

**Fecha:** 2026-09-04
**Estado:** Accepted — auditoría offline. Sin autoridad nueva de runtime.
**Evidencia:** [Municipal M2B](../gazetteer/MUNICIPAL_M2B.md),
[identity_vias.json](../../artifacts/audits/municipal_m2b/identity_vias.json),
[adjudicated_candidates.json](../../artifacts/audits/municipal_m2b/adjudicated_candidates.json).

## Problema y prueba

M2A verifica territorio para 1.929 perfiles, pero deja 931 sin identidad lógica:
347.163 procesos únicos y 117.277 filas físicas de contratos. Ninguno de esos
931 contiene un SIGEP_ENTITY_MATCH propio congelado ni otra grafía observada
en su misma cuenta. Reaplicar la autoridad de cuenta única no resuelve esa deuda.

El experimento M2B contrasta globalmente los nombres oficiales SIGEP, sin usar
el NIT para seleccionar candidatos. Encuentra nombres de instituciones cuya
naturaleza oficial es Empresas Sociales del Estado y que SECOP publica con
una etiqueta legal adicional, abreviada o en el otro extremo. La institución
no cambia porque una fuente omita esa etiqueta en NOMBRE y la declare en
NATURALEZA JURÍDICA. Se necesita probar esa equivalencia en la propia fila,
no prestar la naturaleza de cualquier fila con igual NIT.

Los conteos finales, sus rechazos y referencias exactas se calculan en M2B.
La evidencia incluye cuentas adicionales del directorio sin actividad en la
ventana: la unicidad de cuenta transaccional sola es insuficiente. También
hay matches nominales globales con contradicción fiscal; quedan REVIEW.

## Decisión acotada

Se acepta `OWN_SIGEP_ESE_LEGAL_LABEL_EXACT_CORE` como autoridad de identidad
**candidata de conocimiento offline**, bajo el predicado completo:

1. El perfil tiene afiliación VERIFIED congelada y un solo territorio acreditado.
2. Se retira a lo sumo una etiqueta completa de un extremo de cada nombre:
   `ESE`, `E.S.E.`, `EMPRESA SOCIAL DEL ESTADO` o el plural literal
   `EMPRESAS SOCIALES DEL ESTADO`. Debe existir esa etiqueta en al menos uno
   de los nombres. No se eliminan tokens internos ni se itera la eliminación.
3. El núcleo restante coincide en todos sus tokens y en su orden con el nombre
   propio SIGEP. Se reutiliza la igualdad de tokens ADR-075; no se agregan
   fuzzy, similitud, substring, supresión de topónimos o niveles hospitalarios.
4. La propia fila emparejada tiene naturaleza jurídica
   `Empresas Sociales del Estado`. Esa naturaleza permite tratar la etiqueta
   como representación legal opcional y acredita la familia sanitaria.
5. El núcleo no es el municipio solo. Si participa su topónimo completo, se
   aplica el guard compuesto existente; no se contrae un municipio a su prefijo.
6. Hay un código institucional SIGEP único concordante. Todas las filas de ese
   código tienen alcance Municipal/Distrital y el mismo territorio acreditado.
   UNKNOWN provisional puede refinarse con la naturaleza propia; una familia
   incompatible o contradicción histórica bloquea.
7. Existe directorio propio por cuenta y nombre físico byte-exacto, con territorio
   concordante y sin contradicción fiscal. Otra superficie en esa cuenta que no
   satisface la misma identidad estricta bloquea; el directorio solo nunca
   acredita naturaleza ni identidad lógica.
8. No hay cuenta observada competidora para la entidad ni cuenta adicional de
   directorio que reclame la misma identidad. Los vecinos fiscales de familia
   compatible y otra cuenta requieren una institución independiente distinta
   que los explique; un nombre diferente solo no basta. La deuda queda visible.
9. No hay reclamación canónica sobre la superficie que exija primero revisar
   binding o consolidación. No se crea un duplicado productivo para resolver
   una proyección de fuente pendiente.
10. Las contradicciones fiscales bloquean conservadoramente. El NIT nunca
    genera un candidato, su ID, una fusión ni un grupo. Bogotá queda reservada
    a su registry declarado y no obtiene identidades por esta regla.

El ID candidato depende del código SIGEP y de la cuenta, no del NIT ni de la
pareja territorio/familia. Cada perfil conserva sus nombres físicos originales,
volúmenes y referencias de evidencia. M2A y sus TerritoryProfiles no se mutan.

## Qué no se acepta

- `ESE + municipio`, hospital + municipio o centro/puesto por texto como prueba
  suficiente. Debe satisfacerse la cadena institucional independiente completa.
- Expandir HMI, INDER u otras siglas opacas por intuición, corregir errores
  ortográficos o suprimir palabras como REGIONAL, DEPARTAMENTAL o NIVEL II.
- Extender la equivalencia a ESP, SA u otras formas jurídicas sin otro experimento.
- Asumir que dos hospitales de la misma familia/territorio son una BuyerUnit.
- Fusionar dos canonicals o dos cuentas bajo este predicado. ADR-074 conserva
  intactas sus restricciones para consolidaciones productivas.
- Usar la regla para aliases, bindings, BuyerGroups o filtros runtime.

## Consecuencias y límites

La deuda disminuye sólo dentro de la cohorte territorial verificada; la cobertura
productiva no cambia. Las vías I/II sin casos no se fuerzan para poblar batches.
La vía III es corroboración de IV y sus ganancias no se suman. Los 370 perfiles
ya lógicos se auditan aparte y no cuentan como nuevas identidades M2B.

La ausencia de competidores es relativa a la captura congelada; no demuestra
unicidad universal o vigente. Los canarios adversariales no estiman precisión
sobre un oráculo completo. Hay falsos negativos deliberados ante directorios
con cuentas históricas, identidad fiscal contradictoria o grafías no cubiertas.

## Numeración y validación

Después del experimento se escanearon `docs/`, incluidos ocultos e ignorados,
en los **29 worktrees registrados**: máximo global **ADR-075**. Evidencia y
comando: [adr_scan.json](../../artifacts/audits/municipal_m2b/adr_scan.json).
Por tanto se reserva ADR-076. El barrido no modifica ningún worktree.

Los tests focales incluyen dos hospitales distintos en un mismo territorio y
NIT, préstamos de naturaleza fiscal, siglas opacas, guard compuesto, cuentas
competidoras sin transacciones, conflictos oficiales y bloqueo de red.
`validation.json` conserva corpus, lint, reproducción offline y hashes.
