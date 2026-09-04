# ADR-075 — Afiliación territorial por cadenas de evidencia independientes

**Fecha:** 2026-09-04. **Estado:** Accepted, limitado a conocimiento y auditoría offline.
**Runtime:** sin cambios. **Promociones:** ninguna.

## Problema y evidencia

El predicado municipal M0 mezclaba el reconocimiento de ejecutivos con el de
otras instituciones. Una personería o concejo puede compartir NIT con el
municipio; la fila SIGEP de Alcaldía no corresponde a esa unidad compradora.

[M2A](../gazetteer/MUNICIPAL_M2A.md), sobre capturas congeladas, reproduce:

- 15.494 strata físicos y 11.298 perfiles por cuenta/nombre exacto.
- Vía A: 232 perfiles/458 strata/176 territorios/4.823 procesos únicos. Los
  716 NIT con afiliaciones M0 acreditadas tienen un solo territorio por NIT.
- Pool B: 1.814 perfiles/826 territorios. De los 868 strata cuyo NIT tiene
  Alcaldía en SIGEP, 538 son concejo/personería, equivalentes a 273 perfiles.
  Para 444 strata existe una fila del propio rol bajo ese NIT. Su presencia
  no demuestra todavía que sea la fila de la institución observada.
- NIT 890980764: el pool B incluye la Personería de Venecia si ignora la fila
  departamental de Asamblea de Córdoba. El alcance fiscal completo es
  contradictorio y la vía fiscal se cierra; una coincidencia institucional
  independiente sigue siendo evidencia distinta, conservando la contradicción.
- El cruce del pool B con el directorio propio tiene 1.111 éxitos/1.814
  perfiles (61,25%). Sólo 1.109 corresponden a aplicaciones de B; San Gil
  conserva su autoridad previa y Venecia usa identidad propia SIGEP y directorio,
  sin aplicar la vía fiscal contradictoria. Las aplicaciones de B con verificación
  aportada únicamente por A se enumeran aparte de esa tasa de cruce.

La identidad lógica y el territorio se separan en el resultado: 1.929 perfiles
con afiliación verificada, 931 de ellos pendientes de identidad lógica. Hay
994 unidades lógicas acreditadas, 625 reconocidas, y un pool no resuelto de
10.168 perfiles. Es un denominador condicionado a las autoridades descritas,
no un censo exhaustivo de todas las instituciones municipales.

## Decisión

Se aceptan cadenas explícitas, reproducibles y referenciadas para auditoría.
Cada resultado conserva los eslabones, los índices de fuente y las evidencias
contradictorias. No hay autoridad nueva de runtime, aliases, bindings o grupos.

### A. Puente fiscal con territorio acreditado único

1. Evidencia fiscal publicada por el perfil observado.
2. Coincidencia fiscal acotada con perfiles cuya afiliación ya está acreditada.
3. Cardinalidad del conjunto de territorios acreditados igual a uno.

La cadena sola produce `PROBABLE`. No acredita identidad institucional, familia,
merge ni membresía. Un NIT con varios territorios cierra la vía sin elegir una
mayoría.

Para `VERIFIED` se exige además evidencia del **propio perfil**: una fila SIGEP
independientemente emparejada por nombre institucional y territorio, o registro
propio del directorio por cuenta/nombre byte-exacto/evidencia fiscal y código
municipal único concordante. Una contradicción territorial no se oculta.

### B. SIGEP fiscal y directorio propio

`SIGEP_FISCAL_TERRITORY_MATCH` requiere evidencia fiscal, suborden
Municipal/Distrital y DIVIPOLA municipal único. Se inspeccionan todas las filas
SIGEP del NIT antes de aplicar la vía; un alcance contradictorio la cierra.

El pool hipotético se publica separado de la cohorte aplicable. Los códigos
fuera del catálogo municipal vigente, Bogotá y autoridades previas no se
reinterpretan para reproducir una cota.

La cadena fiscal sola produce `PROBABLE`. El registro **propio** de directorio
por cuenta, nombre byte-exacto, evidencia fiscal y código único concordante
permite `VERIFIED` territorial. Esa corroboración no otorga identidad lógica ni
naturaleza jurídica de una fila SIGEP que sólo comparte NIT.

### Identidad y familia

`SIGEP_ENTITY_MATCH` es evidencia independiente por nombre: igualdad de tokens
institucionales o plantilla estricta de rol + municipio completo con
calificadores departamentales explícitos. Cuando participa un topónimo, se
aplica el guard compuesto M1A.2 con spans completos de mayor longitud; no se
contrae el municipio a un prefijo ni se usa similitud difusa.

Sólo esa fila propia puede aportar naturaleza jurídica. Clasificación por nombre
queda `PROVISIONAL`; una fila de Alcaldía bajo el NIT nunca clasifica un Concejo
ni una Personería como ejecutivo. El matching conservador deja deuda de nombres
abreviados, especialmente en salud; no se rellena con naturaleza fiscal prestada.

Las unidades lógicas requieren autoridad canónica previamente acreditada o
identidad SIGEP propia sin competencia de cuentas observadas, conservando
los casos M0 cuya acreditación institucional ya estaba explícita. Las variantes
se concilian sólo en el artefacto de conocimiento bajo esa evidencia. No se
fusionan canonicals productivos ni se aplica la consolidación propuesta en M1B.

### Bogotá y fuentes previas

Bogotá queda fuera de A/B. Sólo se afirma
`DECLARED_BOGOTA_EXECUTIVE_REGISTRY_V1`; se preservan los bytes de unidades,
bindings, territorio y grupos. El censo M0 contiene 92 perfiles/174 strata,
pero sólo 39 perfiles/76 strata usan esa autoridad. El resto se conserva como
contexto histórico, sin adjudicación nueva ni sustitución de su evidencia.

Ocaña y Calarcá conservan sus deudas SIGEP y su autoridad M1A.2A. M1B se consume
por referencia: sus 39 residuales retienen deuda y disposición. La separación
taxonómica de Educación se declara por perfil.

## Consecuencias y límites

- `PHYSICAL_SOURCE`, `BUYER_UNIT`, `TERRITORY` y `FISCAL_IDENTITY` permanecen
  dimensiones distintas; la clave fiscal nunca deduplica compradores.
- Se publican denominador lógico verificado y pool no resuelto, además de la
  afiliación verificada con identidad todavía pendiente. No se suman granos.
- `ordenentidad/orden` no acreditan afiliación ni exclusión municipal.
- Los contratos únicos por perfil son `DERIVED_FROM_GLOBAL_IDENTITY` sólo
  dentro de la ventana congelada, pues el agregado por perfil mide `count(*)`.
- No se exige que todos los municipios tengan todas las familias institucionales.
- Los controles adversariales y las contradicciones observadas no son una
  estimación de precisión sobre una población completamente adjudicada.
- Toda promoción, alias, binding o grupo requiere una decisión posterior. El
  paso siguiente es `M2B_ADJUDICATION`.

## Alternativas contrastadas

NIT como identidad única está falsificado por ADR-074 y el canario M2A.
Exigir siempre una fila propia SIGEP para verificar **territorio** desaprovecha
la corroboración independiente del directorio; exigirla para atribuir su
**naturaleza institucional** sí es obligatorio. Forzar un denominador lógico
para los perfiles sólo afiliados fabricaría identidades.

## Reserva del número y validación

El número se eligió después del experimento: barrido de `docs/`, incluyendo
archivos ocultos e ignorados, en los 29 worktrees registrados; máximo global
ADR-074. Evidencia: `artifacts/audits/municipal_m2a/adr_scan.json`.

Las pruebas focales falsifican atajos fiscales, prefijos compuestos, conflictos
de directorio, alcance SIGEP contradictorio, equivalencia automática de cuentas
y afiliación genérica de Bogotá. El corpus, lint, reproducción offline y
SHA-256 antes/después quedan en `validation.json` y `byte_identity.json`.
